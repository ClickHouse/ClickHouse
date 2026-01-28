#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/InsertBlockInfo.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
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

    struct BufferedPartitionKey
    {
        Row fields;

        bool operator==(const BufferedPartitionKey & other) const { return fields == other.fields; }

        struct Hash
        {
            size_t operator()(const BufferedPartitionKey & p) const;
        };
    };

    struct BufferedPartitionData
    {
        std::vector<Block> blocks = {};
        size_t rows = 0;
        size_t bytes = 0;

        size_t getMetric(const Settings & settings) const;
    };

    const std::optional<size_t> max_parts_buffer_rows;
    const std::optional<size_t> max_parts_buffer_bytes;
    const bool insert_parts_buffered;
    size_t parts_buffer_rows = 0;
    size_t parts_buffer_bytes = 0;
    std::unordered_map<BufferedPartitionKey, BufferedPartitionData, BufferedPartitionKey::Hash> parts_buffer;
    std::set<std::pair<size_t, decltype(parts_buffer)::pointer>> parts_heap;

    /// We can delay processing for previous chunk and start writing a new one.
    std::unique_ptr<MergeTreeDelayedChunk> delayed_chunk;

    std::vector<std::string> commitPart(MutableDataPartPtr & part, const std::vector<String> & block_ids);
    virtual void finishDelayedChunk();
    virtual TemporaryPartPtr writeNewTempPart(BlockWithPartition & block);
    void consumePartsSimple(BlocksWithPartition part_blocks, std::shared_ptr<DeduplicationInfo> deduplication_info);
    void consumePartsBuffered(BlocksWithPartition part_blocks, std::shared_ptr<DeduplicationInfo> deduplication_info);
    void flushPartsBuffer(bool just_one_bucket);
};
}
