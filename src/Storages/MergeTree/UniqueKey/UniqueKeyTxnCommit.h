#pragma once

#include <Interpreters/InsertDeduplication.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
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
class MergedPartOffsets;

struct UniqueKeyInsertOutcome
{
    /// Block ids already in the table's insert-deduplication log
    std::vector<std::string> conflicting_blocks;
    /// Every incoming row conflicted under `unique_key_conflict_action = 'ignore'`, so there is no part.
    bool part_discarded = false;
};

/// The three writes that implement `IUniqueKeyCommit`, one entry point each. The protocol they
/// run -- stage, publish, commit, all inside one hold of the partition guard -- is described on
/// `IUniqueKeyCommit`; what differs per write is only what it kills and what it publishes.
class UniqueKeyTxnCommit
{
public:
    struct InsertRequest
    {
        StorageMergeTree & storage;
        StorageMetadataPtr metadata_snapshot;
        ContextPtr context;
        const MergeTreeTemporaryPart & temp_part;
        /// The rows `temp_part` was written from, before the writer sorted them.
        std::shared_ptr<const Block> block;
        const std::vector<DeduplicationHash> & deduplication_hashes;
        MergeTreeTransactionHolder & transaction;
    };

    /// INSERT:
    /// 1. Write temp part
    /// 2. Probes the dense index for every key in the written part and resolves conflicts per `unique_key_conflict_action`.
    static UniqueKeyInsertOutcome insert(InsertRequest request);

    struct MergeRequest
    {
        MergeTreeTransactionHolder & transaction;
        /// `snapshot_bitmaps` and `merged_part_offsets` are indexed like this.
        const MergeTreeData::DataPartsVector & source_parts;
        MergeTreeMutableDataPartPtr merged_part;
        /// The bitmaps the merge's input filter dropped rows by.
        const std::vector<ConstDeleteBitmapPtr> & snapshot_bitmaps;
        /// Where each row the input filter let through landed in `merged_part`.
        const MergedPartOffsets & merged_part_offsets;
    };

    /// MERGE:
    /// 1. Create the snapshot, and run regular merge
    /// 2. Commit: Reconciles the rows killed by concurrent operations into the merged part's self-bitmap
    static void merge(StorageMergeTree & storage, MergeRequest request);

    struct DeleteRequest
    {
        MergeTreeTransactionHolder & transaction;
        String partition_id;
        const DeleteRowsByPart & rows_by_part;
        /// A closure and not a holder because only `executeUniqueKeyDelete` is a friend of
        /// `StorageMergeTree`: the caller supplies the means, the commit picks the moment.
        std::function<std::unique_ptr<PlainCommittingBlockHolder>()> allocate_marker_block;
    };

    /// DELETE: Stages a 0-row marker part to carry the commit's csn, and installs one delta
    /// per touched part -- the rows this DELETE kills, and none it inherited.
    /// Returns the newly-dead rows committed.
    static size_t deleteRows(StorageMergeTree & storage, DeleteRequest request);

private:
    /// Nested rather than file-local so they inherit this class's access
    class InsertCommit;
    class MergeCommit;
    class DeleteCommit;
};

}
