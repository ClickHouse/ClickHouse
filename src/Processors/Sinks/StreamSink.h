#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/IStorage.h>

namespace DB
{

// class StreamSink : public SinkToStorage
// {
// public:
//     StreamSink(const StorageMetadataPtr & metadata_snapshot_, SinkPtr sink_, StoragePtr storage_)
//         : SinkToStorage(metadata_snapshot_->getSampleBlock()), sink(sink_)
//         , storage(storage_)
//         // , storage_snapshot(storage_.getStorageSnapshot(metadata_snapshot_, context_))
//     {}
    
//     using SinkToStorage::SinkToStorage;

//     std::string getName() const override { return "StreamSink"; }

//     void consume(Chunk chunk) override
//     {
//         // sink->consume(chunk);
//         auto block = getHeader().cloneWithColumns(chunk.getColumns());
//         // storage_snapshot->metadata->check(block, true);

//         new_blocks.emplace_back(block);
//     }

//     void onFinish() override
//     {
//         // sink->onFinish();
//         for (auto it = storage->subscribers->begin(); it != storage->subscribers->end(); ++it) {
//             std::lock_guard lock((*it)->mutex);
//             (*it)->blocks = std::make_shared<Blocks>();
//             (*it)->blocks->insert((*it)->blocks->end(), new_blocks.begin(), new_blocks.end());
//             (*it)->condition.notify_all();
//         }
//     }
// private:
//     SinkPtr sink;
//     Blocks new_blocks;
//     StoragePtr storage;
//     // StorageSnapshotPtr storage_snapshot;
// };
}
