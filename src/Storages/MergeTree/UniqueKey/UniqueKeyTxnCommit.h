#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyInsertSink.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/MergeTreeCommittingBlock.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyTxn.h>

#include <base/scope_guard.h>

#include <string>
#include <functional>
#include <vector>

namespace DB
{

class StorageMergeTree;

/// The three writes that implement `IUniqueKeyCommit`, one entry point each. The protocol they
/// run -- stage, publish, commit, settle, all inside one hold of the partition guard -- is
/// described on `IUniqueKeyCommit`; what differs per write is only what it kills and what it
/// publishes.
class UniqueKeyTxnCommit
{
public:
    struct InsertRequest
    {
        IUniqueKeyInsertSink & sink;
        StorageMergeTree & storage;
        const StorageMetadataPtr & metadata_snapshot;
        ContextPtr context;
        /// Replaced in place when `ignore` filters the block and the part is rewritten.
        MergeTreeTemporaryPartPtr & temp_part;
        BlockWithPartition & block_with_partition;
        const std::vector<DeduplicationHash> & deduplication_hashes;
        MergeTreeTransactionHolder & transaction;
    };

    struct InsertOutcome
    {
        /// The dedup-log conflicts that ask the sink to retry.
        std::vector<std::string> conflicting_blocks;
        /// `unique_key_conflict_action = ignore` filtered every incoming row, so there is no part.
        bool part_discarded = false;
    };

    /// INSERT:
    /// 1. Write temp part
    /// 2. Probes the dense index for every key in the written part and resolves conflicts per `unique_key_conflict_action`.
    static InsertOutcome insert(InsertRequest request);

    struct MergeRequest
    {
        MergeTreeTransactionHolder & transaction;
        const MergeTreeData::DataPartsVector & source_parts;
        const MergeTreeMutableDataPartPtr & merged_part;
        const std::vector<ConstDeleteBitmapPtr> & snapshot_bitmaps;
    };

    /// MERGE:
    /// 1. Create the snapshot, and run regular merge
    /// 2. Commit: Reconciles the rows killed by concurrent operations into the merged part's self-bitmap
    static void merge(StorageMergeTree & storage, const StorageMetadataPtr & metadata_snapshot, MergeRequest request);

    struct DeleteRequest
    {
        MergeTreeTransactionHolder & transaction;
        String partition_id;
        const DeleteRowsByPart & rows_by_part;
        /// A closure and not a holder because only `executeUniqueKeyDelete` is a friend of
        /// `StorageMergeTree`: the caller supplies the means, the commit picks the moment.
        std::function<std::unique_ptr<PlainCommittingBlockHolder>()> allocate_marker_block;
    };

    /// DELETE: Stages a 0-row marker part to carry the commit's csn, and installs one
    /// cumulative kill bitmap per touched part.
    /// Returns the newly-dead rows committed.
    static size_t deleteRows(StorageMergeTree & storage, DeleteRequest request);

private:
    /// Nested rather than file-local so they inherit this class's access
    class InsertCommit;
    class MergeCommit;
    class DeleteCommit;
};

}
