#include <Storages/MergeTree/UniqueKey/Txn/PartitionTxnController.h>

#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyManifest.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <utility>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}
}

namespace DB::UniqueKeyTxn
{

PartitionTxnController::PartitionTxnController(
    std::unique_ptr<ICommitCoordinator> coordinator_,
    std::unique_ptr<IBitmapStore>       bitmap_store_,
    std::unique_ptr<IPinRegistry>       pin_registry_)
    : coordinator(std::move(coordinator_))
    , bitmap_store(std::move(bitmap_store_))
    , pin_registry(std::move(pin_registry_))
{
    chassert(coordinator && bitmap_store && pin_registry,
        "PartitionTxnController requires all three strategies to be non-null");
}

PartitionTxnController::~PartitionTxnController() = default;

std::vector<PartitionTxnController::CumulativeDelete>
PartitionTxnController::prepareCumulativePayloads(const CommitRequest & req, CSN snapshot_csn)
{
    std::vector<CumulativeDelete> prepared;
    prepared.reserve(req.touched.size());

    for (const auto & t : req.touched)
    {
        /// `readBitmap` returns `(empty non-null bitmap, 0)` on miss — `prev`
        /// is NEVER null. "No deletions yet" is `prev->empty()`, not `!prev`.
        auto [prev, prev_csn] = bitmap_store->readBitmap(t.target, snapshot_csn);
        const bool has_delta = t.new_kills && !t.new_kills->empty();

        /// No prior, no delta → nothing to publish.
        if (prev->empty() && !has_delta)
            continue;

        ConstRoaringBitmapPtr bitmap;
        if (!has_delta)
            /// Re-publish the prior cumulative (already const, read-only here).
            bitmap = std::move(prev);
        else if (prev->empty())
            bitmap = t.new_kills;
        else
        {
            auto merged = std::make_shared<DeleteBitmap>();
            merged->merge(*prev);
            merged->merge(*t.new_kills);
            bitmap = std::move(merged);
        }

        prepared.push_back({t.target, std::move(bitmap)});
    }
    return prepared;
}

void PartitionTxnController::rollbackInstalls(const std::vector<PartName> & committed_puts, CSN assigned_csn)
{
    /// Reverse-order undo of this commit's installs. `removeBitmap` is
    /// idempotent and invalidates the per-part cache so readers see the
    /// pre-DELETE state. `assigned_csn` is strictly above every committed
    /// version on each target (csn-seed floor + per-commit bump + install
    /// monotonicity assert), so this can only unlink THIS commit's own writes.
    /// A `removeBitmap` that itself throws leaves the orphan on disk and
    /// latches the partition fail-closed; recovery reconciles on reload.
    for (auto it = committed_puts.rbegin(); it != committed_puts.rend(); ++it)
    {
        try
        {
            bitmap_store->removeBitmap(*it, assigned_csn);
        }
        catch (...)
        {
            partition_state_torn.store(true, std::memory_order_release);
            tryLogCurrentException(
                getLogger("PartitionTxnController"),
                "rollback removeBitmap failed for target=" + *it
                    + "; partition latched fail-closed until reload");
        }
    }
}

void PartitionTxnController::publishUnderLock(
    CSN assigned_csn, std::vector<CumulativeDelete> & prepared, const CommitRequest & req)
{
    /// Writer order at the SAME `assigned_csn`, all durable on return:
    ///   (1) finalize manifest (fsync'd before any sidecar so recovery's
    ///       tmp-scan finds every bitmap this commit claims).
    ///   (2) install each cumulative sidecar.
    ///   (3) rename(tmp → active) + fsync — the atomic visibility point.
    /// On throw at any step, `attemptCommit` leaves `partition.csn` unchanged
    /// and we roll back this commit's installs (closes the steady-state window
    /// before recovery's startup-only tmp-scan).
    if (req.staged.finalize_manifest)
        req.staged.finalize_manifest(assigned_csn);

    std::vector<PartName> committed_puts;
    committed_puts.reserve(prepared.size());
    try
    {
        for (auto & p : prepared)
        {
            /// Record the target BEFORE the install so rollback also covers an
            /// install that throws after its durable write+rename.
            committed_puts.push_back(p.part);
            bitmap_store->installBitmap(p.part, assigned_csn, *p.bitmap);
        }

        if (req.staged.publish)
            req.staged.publish(assigned_csn);
    }
    catch (...)
    {
        rollbackInstalls(committed_puts, assigned_csn);
        throw;
    }
}

CommitResult PartitionTxnController::commit(CommitRequest req)
{
    if (partition_state_torn.load(std::memory_order_acquire))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "UNIQUE KEY partition is fail-closed: a prior DELETE rollback left an orphaned "
            "delete bitmap that could not be removed. Reload the table to run recovery "
            "before issuing further DELETEs.");

    /// Single-attempt commit: the Local coordinator is pessimistic
    /// (`commit_lock`) and always assigns a csn. An optimistic Shared
    /// coordinator would reintroduce a read→prepare→retry loop here — see the
    /// ICommitCoordinator class note.
    auto snap = coordinator->readSnapshot();

    /// Build cumulative payloads against the snapshot csn, outside the publish
    /// lock. The install inside the lock uses the coordinator's freshly-
    /// allocated `assigned_csn` (not a pre-computed `snap.csn + 1`) so the
    /// sidecar name always matches the csn the manifest is finalized at —
    /// the single source of truth is `attemptCommit`'s return.
    auto prepared = prepareCumulativePayloads(req, snap.csn);

    auto staging = [this, &prepared, &req](CSN assigned_csn)
    {
        publishUnderLock(assigned_csn, prepared, req);
    };

    const CSN csn = coordinator->attemptCommit(std::move(staging));

    CommitResult result;
    result.csn = csn;
    /// `new_part` is filled in by the caller (which holds the MergeTreeData
    /// hook); this layer returns only the csn.
    return result;
}

QuerySnapshotResult PartitionTxnController::takeQuerySnapshot()
{
    /// Fast-fail; the authoritative torn check is the post-capture re-check below.
    if (partition_state_torn.load(std::memory_order_acquire))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "UNIQUE KEY partition is fail-closed: a prior DELETE rollback left an orphaned "
            "delete bitmap. Reload the table to run recovery.");

    /// Materialize the snapshot csn and the pin in one serialization region
    /// (the coordinator's publish lock), so GC can't unlink a bitmap this
    /// reader needs between the csn read and the pin install.
    auto view = std::make_shared<PartitionView>();
    std::shared_ptr<IPinRegistry::PinHandle> pin;
    coordinator->withinSnapshotRegion([&](CSN current_csn)
    {
        view->csn = current_csn;
        pin = pin_registry->acquire(current_csn);
    });
    chassert(pin, "withinSnapshotRegion did not install a pin");

    /// Authoritative torn re-check, after the capture: a commit latches `partition_state_torn`
    /// under the same lock the snapshot region samples csn under, so any capture that could
    /// observe the orphan necessarily took the lock after the latch and sees it here.
    if (partition_state_torn.load(std::memory_order_acquire))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "UNIQUE KEY partition is fail-closed: a prior DELETE rollback left an orphaned "
            "delete bitmap. Reload the table to run recovery.");

    /// Bind the bitmap lookup into the freshly-built, sole-owned view. `store`
    /// is owned by `*this`, which outlives every snapshot it issues.
    IBitmapStore * const store = bitmap_store.get();
    const CSN snap_csn = view->csn;
    view->bitmap_at = [store, snap_csn](const PartName & part) -> ConstRoaringBitmapPtr
    {
        /// Never null: readBitmap returns an empty sentinel on miss; consumers read it read-only.
        auto [bitmap, chosen_csn] = store->readBitmap(part, snap_csn);
        return bitmap;
    };

    return QuerySnapshot(std::move(view), std::move(pin));
}

void PartitionTxnController::runGcRound()
{
    /// Reclaim disk space from superseded delete-bitmap versions that no live
    /// query snapshot can still read; without GC the bitmap sidecars grow
    /// unbounded. Mechanism: remove each part's committed bitmaps superseded
    /// below `pin_registry->clusterFloor()`.
    /// TODO(unique-key): not implemented yet.
}

void PartitionTxnController::recover(
    std::vector<MergeTreeDataPartPtr> /*active_parts*/,
    std::vector<MutableDataPartStoragePtr> tmp_storages)
{
    /// Recovery sweep:
    ///
    ///   for each tmp_<op>/:
    ///     if unique_key.txt missing/unparseable: throw — fail closed
    ///     for each (target, csn) in bitmaps_created:
    ///       target_path = (target == 'self') ? tmp_<op>/ : active[target]
    ///       unlink target_path/delete_bitmap_<csn>.rbm
    ///     rm -r tmp_<op>/
    ///
    /// `partition.csn` is seeded separately by the factory
    /// (`MakeLocalStrategies` → `LocalCommitCoordinator::seedCsn`); we do
    /// not bump it here.
    ///
    /// The manifest-precedes-bitmap durability ordering is what makes this
    /// sweep complete: every aborted-commit bitmap on disk has a
    /// tmp_<op>/unique_key.txt claiming it.
    auto logger = getLogger("PartitionTxnController");
    for (const auto & tmp_storage : tmp_storages)
    {
        if (!UniqueKeyManifest::exists(*tmp_storage))
        {
            /// Fail closed: a writer-path bug or partially-written tmp dir.
            /// Operator inspects the tmp dir for forensics.
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Recovery: tmp dir '{}' is missing unique_key.txt manifest. "
                "Cannot determine which sidecars to remove; aborting startup.",
                tmp_storage->getFullPath());
        }

        UniqueKeyManifest meta = UniqueKeyManifest::read(*tmp_storage);

        for (const auto & [target, cs] : meta.bitmaps_created)
        {
            /// `target == "self"` means the sidecar lives inside the
            /// aborted tmp dir itself. `rm -r tmp_<op>/` below would
            /// remove it anyway, but removeBitmap first so the per-part
            /// cache (if any) is invalidated symmetrically.
            const PartName resolved_target = (target == "self")
                ? tmp_storage->getPartDirectory()
                : target;
            try
            {
                bitmap_store->removeBitmap(resolved_target, cs);
            }
            catch (...)
            {
                /// Fail closed: this is the rollback of a partially-installed
                /// (aborted-DELETE) bitmap. If the removal fails the bitmap
                /// version survives on disk and csn-seed can surface it as
                /// committed. Propagate so the tmp dir is left for forensics and
                /// the caller (runUniqueKeyTxnRecovery) fails the table's startup.
                tryLogCurrentException(logger,
                    fmt::format("recover: removeBitmap({}, {}) failed for tmp dir '{}'",
                        resolved_target, cs, tmp_storage->getFullPath()));
                throw;
            }
        }

        try
        {
            tmp_storage->removeRecursive();
        }
        catch (...)
        {
            tryLogCurrentException(logger,
                fmt::format("recover: failed to remove tmp dir '{}'", tmp_storage->getFullPath()));
        }
    }
}

CSN PartitionTxnController::clusterFloor() const
{
    return pin_registry->clusterFloor();
}

}
