#pragma once

#include <chrono>
#include <deque>
#include <exception>
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
    void setHasDependentMaterializedViews(bool has_dependent_views) override;

protected:
    StorageMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    size_t max_parts_per_block;
    ContextPtr context;
    StorageSnapshotPtr storage_snapshot;
    /// The result of the "too many parts" check, evaluated on the query thread at sink
    /// construction and thrown from onStart, when the sink starts executing.
    std::exception_ptr too_many_parts_exception;
    UInt64 num_blocks_processed = 0;
    bool deduplicate = true;
    bool synchronously_commit_part_for_dependent_views = false;
    /// We can delay processing for previous chunk and start writing a new one.
    std::unique_ptr<MergeTreeDelayedChunk> delayed_chunk;

    /// With `fsync_after_insert`, the parts are written and committed without fsync, and the files
    /// of all parts written by this sink are fsynced together in `onFinish`, before the insert is
    /// acknowledged. Meanwhile the OS writes the data back in the background, so the final fsync is
    /// cheap, and the insert does not wait for the disk after every part.
    /// Holding the parts prevents their removal, even if they are merged in the meantime.
    /// To not hold them for the whole duration of a long insert, the parts committed long enough ago
    /// are synced while the insert is still running.
    struct PartToSync
    {
        MutableDataPartPtr part;
        std::chrono::steady_clock::time_point commit_time;
    };
    bool defer_fsync = false;
    std::deque<PartToSync> parts_to_sync;
    void syncDeferredParts(bool only_old);

    std::vector<std::string> commitPart(MutableDataPartPtr & part, const std::vector<DeduplicationHash> & deduplication_hashes);
    virtual void finishDelayedChunk();
    virtual TemporaryPartPtr writeNewTempPart(BlockWithPartition & block);
};

}
