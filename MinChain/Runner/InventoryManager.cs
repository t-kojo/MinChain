using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using static MinChain.InventoryMessageType;
using static MessagePack.MessagePackSerializer;

namespace MinChain
{
    // 今どんなブロック持ってるのかのマネージャー
    public class InventoryManager
    {
        public const int MaximumBlockSize = 1024 * 1024; // 1MB

        public Dictionary<ByteString, byte[]> Blocks { get; }
            = new Dictionary<ByteString, byte[]>(); // ブロックの一覧
        public Dictionary<ByteString, Transaction> MemoryPool { get; }
            = new Dictionary<ByteString, Transaction>(); // 未処理トランザクション一覧

        public ConnectionManager ConnectionManager { get; set; }
        public Executor Executor { get; set; }

        public Task HandleMessage(InventoryMessage message, int peerId)
        {
            switch (message.Type)
            {
                // このブロック・トランザクション持ってますか？   
                case Advertise: return HandleAdvertise(message, peerId);
                // くれ！   
                case Request: return HandleRequest(message, peerId);
                // あげる！
                case Body: return HandleBody(message, peerId);
                default: return Task.CompletedTask;
            }
        }

        async Task HandleAdvertise(InventoryMessage message, int peerId)
        {
            // Data should not contain anything. (To prevent DDoS)
            if (!message.Data.IsNull()) throw new ArgumentException();

            // 自分がそのオブジェクト持ってるか？
            var haveObject = message.IsBlock ?
                Blocks.ContainsKey(message.ObjectId) :
                MemoryPool.ContainsKey(message.ObjectId);
            if (haveObject) return;

            message.Type = Request;
            // 送ってちょうだい
            await ConnectionManager.SendAsync(message, peerId);
        }

        async Task HandleRequest(InventoryMessage message, int peerId)
        {
            // Data should not contain anything. (To prevent DDoS)
            if (!message.Data.IsNull()) throw new ArgumentException();

            byte[] data;
            if (message.IsBlock)
            {
                // ちゃんとしたブロック？
                // ハッシュテーブルのそのブロックあるか見てみる
                if (!Blocks.TryGetValue(message.ObjectId, out data)) return;
            }
            else
            {
                Transaction tx;
                // ちゃんとしたトランザクション
                if (!MemoryPool.TryGetValue(message.ObjectId, out tx)) return;
                data = tx.Original;
            }

            message.Type = Body;
            message.Data = data;
            // 送ってあげる
            await ConnectionManager.SendAsync(message, peerId);
        }

        async Task HandleBody(InventoryMessage message, int peerId)
        {
            // Data should not exceed the maximum size.
            var data = message.Data;
            if (data.Length > MaximumBlockSize) throw new ArgumentException();

            // ハッシュ値正しい？
            var id = message.IsBlock ?
                BlockchainUtil.ComputeBlockId(data) :
                Hash.ComputeDoubleSHA256(data);
            if (!ByteString.CopyFrom(id).Equals(message.ObjectId)) return;

            if (message.IsBlock)
            {
                // ミューテックス
                lock (Blocks)
                {
                    if (Blocks.ContainsKey(message.ObjectId)) return;
                    // ハッシュテーブルに追加
                    Blocks.Add(message.ObjectId, data);
                }

                // 前のブロックも知らなかったら前のももらう
                var prevId = Deserialize<Block>(data).PreviousHash;
                if (!Blocks.ContainsKey(prevId))
                {
                    await ConnectionManager.SendAsync(new InventoryMessage
                    {
                        Type = Request,
                        IsBlock = true,
                        ObjectId = prevId,
                    }, peerId);
                }
                Executor.ProcessBlock(data, prevId);
            }
            else
            {
                if (MemoryPool.ContainsKey(message.ObjectId)) return;

                var tx = BlockchainUtil.DeserializeTransaction(data);

                // Ignore the coinbase transactions.
                if (tx.InEntries.Count == 0) return;

                lock (MemoryPool)
                {
                    if (MemoryPool.ContainsKey(message.ObjectId)) return;
                    MemoryPool.Add(message.ObjectId, tx);
                }
            }

            message.Type = Advertise;
            message.Data = null;
            // 他の人に教えてあげる
            await ConnectionManager.BroadcastAsync(message, peerId);
        }
    }
}
