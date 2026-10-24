#pragma once

#include <exception>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/InsertBlockInfo.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Common/ProfileEvents.h>
#include <Interpreters/InsertDeduplication.h>
#include <Interpreters/MergeTreeTransactionHolder.h>


namespace DB
{

class StorageMergeTree;
struct UniqueKeyInsertOutcome;
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
        /// Commits this partition's part; its csn versions the part and every bitmap the commit
        /// writes. Empty when the table has no unique key.
        MergeTreeTransactionHolder uk_txn;
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
    /// Parts committed by this sink, to be fsynced in one batch by onFinish(). Only the parts that
    /// were not synced when written (MergeTreeTemporaryPart::needs_fsync_on_finish) are collected,
    /// so that a long-running INSERT without batched fsync does not accumulate them for nothing.
    /// With `fsync_after_insert`, also the existing parts that blocks of this INSERT were
    /// deduplicated against, in either mode (MergeTreeTemporaryPart::fsync_after_insert).
    std::vector<MergeTreePartInfo> committed_parts;

    /// fsync the parts committed so far and forget them. See MergeTreeData::fsyncPartsAfterInsert().
    void fsyncCommittedParts();

    /// Returns the block ids that are already in the deduplication log. If `conflict_parts` is given,
    /// the parts those block ids belong to are appended to it.
    std::vector<std::string> commitPart(
        MutableDataPartPtr & part,
        const std::vector<DeduplicationHash> & deduplication_hashes,
        std::vector<MergeTreePartInfo> * conflict_parts = nullptr);
    virtual void finishDelayedChunk();
    virtual TemporaryPartPtr writeNewTempPart(BlockWithPartition & block, const MergeTreeTransactionPtr & txn);

private:
    UniqueKeyInsertOutcome commitUniqueKeyPart(
        MergeTreeDelayedChunk::Partition & partition, const std::vector<DeduplicationHash> & deduplication_hashes);
};

}
