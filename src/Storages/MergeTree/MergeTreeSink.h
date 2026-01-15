#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/InsertBlockInfo.h>
#include <Common/ProfileEvents.h>
#include <Interpreters/InsertDeduplication.h>


namespace DB
{

class StorageMergeTree;
struct BlockWithPartition;

struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

class IMergeTreeDataPart;
using MutableDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;

struct MergeTreeTemporaryPart;
using TemporaryPartPtr = std::unique_ptr<MergeTreeTemporaryPart>;

struct MergeTreeDelayedChunk
{
    struct Partition
    {
        LoggerPtr log;
        BlockWithPartition block_with_partition;

        DeduplicationInfo::Ptr deduplication_info;
        TemporaryPartPtr temp_part;
        UInt64 elapsed_ns;
        ProfileEvents::Counters part_counters;
    };

    std::vector<Partition> partitions;
};

class MergeTreeSink : public SinkToStorage
{
public:
    MergeTreeSink(
        StorageMergeTree & storage_,
        StorageMetadataPtr metadata_snapshot_,
        size_t max_parts_per_block_,
        ContextPtr context_);

    ~MergeTreeSink() override;

    String getName() const override { return "MergeTreeSink"; }
    void consume(Chunk & chunk) override;
    void onStart() override;
    void onFinish() override;

protected:
    StorageMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    size_t max_parts_per_block;
    ContextPtr context;
    StorageSnapshotPtr storage_snapshot;
    UInt64 num_blocks_processed = 0;
    bool deduplicate = true;
    /// We can delay processing for previous chunk and start writing a new one.
    std::unique_ptr<MergeTreeDelayedChunk> delayed_chunk;

    std::vector<std::string> commitPart(MutableDataPartPtr & part, const std::vector<String> & block_ids);
    virtual void finishDelayedChunk();
    virtual TemporaryPartPtr writeNewTempPart(BlockWithPartition & block);
};

}
