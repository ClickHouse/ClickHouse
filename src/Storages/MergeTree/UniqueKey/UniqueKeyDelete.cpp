#include <Storages/MergeTree/UniqueKey/UniqueKeyDelete.h>

#include <Interpreters/Context.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeCommittingBlock.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/Txn/PartitionTxnController.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyDeleteRowFinder.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyMarkerPart.h>
#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyManifest.h>
#include <Storages/StorageMergeTree.h>

#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/TransactionID.h>

#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>


namespace ProfileEvents
{
    extern const Event UniqueKeyDeleteRows;
    extern const Event UniqueKeyMutexHoldMicroseconds;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

/// Run one partition's DELETE commit through the per-partition
/// `PartitionTxnController`. Returns the number of touched targets actually
/// committed (skipped-outdated targets are accounted by the caller). Caller
/// holds the partition's writer guard (`controller.lockForWrite()`) for the
/// duration.
size_t commitDeleteForPartition(
    StorageMergeTree & storage,
    UniqueKeyTxn::PartitionTxnController & txn_controller,
    const String & partition_id,
    const std::vector<UniqueKeyTxn::UniqueKeyDeleteRowsForPart> & parts_in_partition,
    LoggerPtr log,
    size_t & out_skipped_outdated_parts,
    std::unique_ptr<PlainCommittingBlockHolder> marker_block_holder)
{
    /// Resolve part_name → DataPartPtr under the writer guard. NOTE: the writer
    /// mutex is DELETE-only — merges do NOT take it. So resolving here only
    /// freezes the part set against other DELETEs, not against merges; the
    /// exact-match skip below catches a merge that committed BEFORE this
    /// resolution, but a merge that outdates a target AFTER it (between this
    /// critical section and the bitmap install) is not caught here — see the
    /// install-side note on that race below.
    struct ResolvedTarget
    {
        MergeTreeData::DataPartPtr part;
        UniqueKeyTxn::DeleteBitmapPtr rows;
    };
    std::vector<ResolvedTarget> targets;
    targets.reserve(parts_in_partition.size());

    /// Capture one MergeTreePartition for the marker part (any surviving
    /// touched part from this partition will do — they all share the same
    /// partition value).
    MergeTreePartition marker_partition;
    bool marker_partition_set = false;

    {
        auto parts_lock = storage.readLockParts();
        for (const auto & entry : parts_in_partition)
        {
            auto part = storage.getActiveContainingPart(entry.part_name, parts_lock);
            if (!part)
            {
                ++out_skipped_outdated_parts;
                continue;
            }
            /// DELETE-vs-merge retargeting: `getActiveContainingPart` returns
            /// the COVERING part, so a merge committed between our SELECT and
            /// this critical section would leave `_part_offset` targeting wrong
            /// rows. Require an exact `part->name == entry.part_name` match;
            /// best-effort skip otherwise.
            ///
            /// TODO(unique-key): this catches only a pre-resolution merge. A
            /// post-resolution merge (merges don't take the writer mutex) is a
            /// silent under-delete until merge-side late-kill lands; re-running
            /// the DELETE clears the survivors.
            if (part->name != entry.part_name)
            {
                LOG_DEBUG(log,
                    "UNIQUE KEY DELETE: skipping part '{}' — merged into covering part '{}' "
                    "between SELECT and the writer guard; stale _part_offset values cannot be remapped",
                    entry.part_name, part->name);
                ++out_skipped_outdated_parts;
                continue;
            }

            if (!marker_partition_set)
            {
                marker_partition = part->partition;
                marker_partition_set = true;
            }

            ResolvedTarget rt;
            rt.part = std::move(part);
            rt.rows = entry.rows;
            targets.push_back(std::move(rt));
        }
    }

    if (targets.empty())
        return 0;

    const Int64 marker_block_number = marker_block_holder->block.number;

    /// Stage the marker part. `creation_csn = INVALID_CSN` is a placeholder —
    /// the publish callback rewrites it with the real assigned csn.
    UniqueKeyTxn::UniqueKeyManifest marker_meta;
    marker_meta.creation_csn = UniqueKeyTxn::INVALID_CSN;
    marker_meta.is_marker = true;
    marker_meta.bitmaps_created.reserve(targets.size());
    for (const auto & t : targets)
        marker_meta.bitmaps_created.emplace_back(t.part->name, UniqueKeyTxn::INVALID_CSN);

    auto marker_handle = UniqueKeyTxn::createMarkerPart(
        storage, partition_id, marker_block_number, marker_partition, marker_meta);

    /// `touched` is the per-target DELTA; the commit cumulates
    /// `prev_bitmap ∪ new_kills` internally and PUTs the cumulative bytes.
    UniqueKeyTxn::CommitRequest req;
    req.touched.reserve(targets.size());
    for (auto & t : targets)
    {
        UniqueKeyTxn::TouchedPartKills tk;
        tk.target = t.part->name;
        tk.new_kills = t.rows;
        req.touched.push_back(std::move(tk));
    }

    /// Two callbacks split around the bitmap PUTs so the Writer ordering holds
    /// under the publish lock: manifest finalize, bitmap PUTs, rename.
    auto marker_data_part_shared = marker_handle.data_part;
    MergeTreeData & data_ref = storage;

    /// Runs inside the publish lock BEFORE any bitmap PUT: rewrite
    /// `unique_key.txt` with the real csn (replacing the staged
    /// `INVALID_CSN` placeholders in `marker_meta`) so the recovery sweep can
    /// find every sidecar this commit writes, then mirror the lightweight
    /// projection into the in-memory cache so same-process consumers don't wait
    /// on the lazy ATTACH-time disk read.
    req.staged.finalize_manifest = [marker_data_part_shared, finalized = marker_meta]
        (UniqueKeyTxn::CSN assigned_csn) mutable
    {
        finalized.creation_csn = assigned_csn;
        finalized.is_marker = true;
        for (auto & [_, csn] : finalized.bitmaps_created)
            csn = assigned_csn;

        UniqueKeyTxn::UniqueKeyManifest::write(
            marker_data_part_shared->getDataPartStorage(), finalized);

        marker_data_part_shared->setUniqueKeyMeta(
            UniqueKeyTxn::UniqueKeyPartMeta{finalized.creation_csn, finalized.is_marker});
    };

    /// Runs inside the publish lock AFTER `finalize_manifest` and after every
    /// per-target bitmap PUT. Renames tmp → active via `renameTempPartAndAdd`
    /// (same primitive INSERT uses); the part goes Active on `transaction.commit`.
    req.staged.publish = [&data_ref, marker_data_part_shared]
        (UniqueKeyTxn::CSN /*assigned_csn*/)
    {
        MergeTreeData::MutableDataPartPtr mutable_part = marker_data_part_shared;
        MergeTreeData::Transaction transaction(data_ref, /*txn=*/nullptr);
        {
            auto parts_lock = data_ref.lockParts();
            data_ref.renameTempPartAndAdd(
                mutable_part, transaction, parts_lock,
                /*rename_in_transaction=*/false);
            transaction.commit(parts_lock);
        }
    };

    UniqueKeyTxn::CommitResult result;
    try
    {
        result = txn_controller.commit(std::move(req));
    }
    catch (...)
    {
        /// A rollback double-fault (commit's undo couldn't remove a bitmap it
        /// installed) leaves the named targets carrying an orphaned
        /// delete_bitmap_<csn>.rbm. Quarantine each via `removePartsFromWorkingSet`
        /// (-> Outdated + deferred refcount-gated removal — the DETACH/DROP PART
        /// path, which never renames a live part's files): once Outdated the part
        /// leaves the active set, so csn-seed can't surface the orphan. The retire
        /// is the safety boundary (not swallowed); the clone-to-detached only
        /// preserves rows for recovery (best-effort). Then rethrow.
        for (const auto & broken_name : txn_controller.takeOrphanedTargets())
            for (const auto & t : targets)
                if (t.part->name == broken_name)
                {
                    storage.removePartsFromWorkingSet(NO_TRANSACTION_RAW, {t.part}, /*clear_without_timeout=*/true);
                    try
                    {
                        t.part->makeCloneInDetached(
                            "broken-uk-bitmap", t.part->getMetadataSnapshot(), /*disk_transaction=*/{});
                    }
                    catch (...)
                    {
                        tryLogCurrentException(log,
                            "broken UNIQUE KEY part " + broken_name
                                + " retired from working set but clone-to-detached failed");
                    }
                }
        throw;
    }
    chassert(result.csn != UniqueKeyTxn::INVALID_CSN,
        "UNIQUE KEY DELETE: PartitionTxnController::commit returned INVALID_CSN");

    /// Report the per-target delta cardinality for the ProfileEvent (not the
    /// cumulative dead set; the row finder pre-filters already-dead rows).
    size_t committed_rows = 0;
    for (const auto & t : targets)
        if (t.rows)
            committed_rows += t.rows->cardinality();

    return committed_rows;
}

}

void executeUniqueKeyDelete(
    StorageMergeTree & storage,
    const ASTPtr & query_ptr,
    ContextPtr query_context)
{
    /// Synchronous UNIQUE KEY DELETE; see the header for the full contract.
    /// Resurrection: with `_block_number` ordering, a later INSERT of a
    /// now-dead key produces a strictly-greater `_block_number`, so the
    /// bitmap-dead row stays invisible and the new row supersedes it.
    ///
    /// NOT a transaction. The atomic unit is one partition's commit (marker
    /// part + per-target bitmap installs under that partition's writer guard);
    /// the loop below visits partitions independently with no cross-partition
    /// atomicity or rollback. A crash or mid-loop error leaves the
    /// already-committed partitions deleted and the rest untouched — re-running
    /// the same DELETE is the recovery (idempotent: already-dead rows are
    /// filtered out by the read path). The statement is also not isolated from
    /// concurrent merges (see the outdated-part skip in
    /// `commitDeleteForPartition`).

    /// MVCC: marker parts + bitmaps are published with `txn = nullptr`, so a
    /// ROLLBACK cannot undo them. Reject DELETE inside an open transaction.
    if (query_context->getCurrentTransaction())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "DELETE on UNIQUE KEY tables is not supported inside a transaction");

    const auto & storage_id = storage.getStorageID();
    auto log = getLogger(storage_id.getNameForLogs());

    /// ----- Step 1: row finder (no lock).
    auto rf = UniqueKeyTxn::UniqueKeyDeleteRowFinder::find(storage, query_ptr, query_context);

    /// ----- Step 2: per partition, route through PartitionTxnController::commit.
    size_t total_committed_rows = 0;
    size_t skipped_outdated_parts = 0;

    for (auto & [partition_id, parts_in_partition] : rf.by_partition)
    {
        /// Acquire the partition's writer guard and hold it across the whole
        /// commit (resolve → stage → commit). On Local this is a held
        /// per-partition mutex; the guard closes the read-prev → publish window
        /// against a concurrent DELETE in the same partition.
        auto & txn_controller = storage.getOrCreateTxnController(partition_id);
        auto partition_lock = txn_controller.lockForWrite();
        Stopwatch mutex_hold_watch;

        /// Allocated here rather than in the anon-namespace helper below,
        /// which cannot reach `allocateBlockNumber` (this function is the
        /// `friend` of `StorageMergeTree`).
        auto marker_block_holder = storage.allocateBlockNumber(CommittingBlock::Op::NewPart);

        total_committed_rows += commitDeleteForPartition(
            storage, txn_controller, partition_id, parts_in_partition, log, skipped_outdated_parts,
            std::move(marker_block_holder));

        ProfileEvents::increment(
            ProfileEvents::UniqueKeyMutexHoldMicroseconds, mutex_hold_watch.elapsedMicroseconds());
    }

    ProfileEvents::increment(ProfileEvents::UniqueKeyDeleteRows, total_committed_rows);

    LOG_DEBUG(log,
        "UNIQUE KEY DELETE: predicate matched {} rows across {} parts; committed {} newly-dead rows; "
        "{} parts skipped (outdated under writer guard)",
        rf.stats.total_matched_rows, rf.stats.parts_with_hits, total_committed_rows, skipped_outdated_parts);

    /// Loud signal for the DELETE-vs-merge skip (see commitDeleteForPartition):
    /// rows whose part merged away between the predicate scan and commit are
    /// NOT deleted by this statement. Surfaced at WARNING so the gap is not
    /// silent until merge-side reconciliation (forwarding + late-kill) lands.
    if (skipped_outdated_parts > 0)
        LOG_WARNING(log,
            "UNIQUE KEY DELETE on table {}: {} part(s) merged away between the predicate "
            "scan and commit; their matching rows were NOT deleted by this statement "
            "(concurrent-merge race) — re-run the DELETE to remove them",
            storage.getStorageID().getNameForLogs(), skipped_outdated_parts);
}

}
