#pragma once

#include <Storages/MergeTree/UniqueKey/IBitmapStore.h>
#include <Storages/MergeTree/UniqueKey/Txn/ICommitCoordinator.h>
#include <Storages/MergeTree/UniqueKey/Txn/SnapshotPinning.h>

#include <functional>
#include <memory>
#include <vector>

namespace DB
{
    class IMergeTreeDataPart;
    using MergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
    class IDataPartStorage;
    using MutableDataPartStoragePtr = std::shared_ptr<IDataPartStorage>;
}

namespace DB::UniqueKeyTxn
{

/// Opaque handle to a staged tmp part directory, forwarded into the publish
/// step at its linearization point. Two callbacks split the engine-specific
/// work around the bitmap installs so the writer ordering holds under the
/// publish lock: `finalize_manifest(assigned_csn)` rewrites the manifest and
/// is fsync'd before any bitmap install (so recovery's tmp-scan finds every
/// sidecar this commit claims), bitmaps install at `assigned_csn`, then
/// `publish(assigned_csn)` renames tmp→active and fsyncs — the point at which
/// the new part and its bitmaps become visible atomically. Both callbacks run
/// at the same freshly-allocated `assigned_csn`.
struct TempPartHandle
{
    /// Optional callback that finalizes csn-dependent manifest state under
    /// the publish lock BEFORE any bitmap PUT. Runs at `assigned_csn`. May
    /// be null when there's no manifest to rewrite (e.g. tests).
    std::function<void(CSN assigned_csn)> finalize_manifest;
    /// Optional callback that performs the engine-specific rename
    /// (`rename(tmp → active) + fsync(active/)` on Local), at the same
    /// `assigned_csn` as `finalize_manifest`. Runs AFTER bitmap PUTs. If
    /// null, the coordinator's `PublishAction` lambda is responsible for any
    /// work.
    std::function<void(CSN assigned_csn)> publish;
};

/// One touched (old) part's contribution to this commit's bitmap diff.
/// `new_kills` is the DELTA only; `commit` cumulates against
/// `readBitmap(target, snap.csn)` under the linearization.
struct TouchedPartKills
{
    PartName          target;
    DeleteBitmapPtr  new_kills;
};

struct CommitRequest
{
    TempPartHandle                                       staged;
    std::vector<TouchedPartKills>                        touched;
};

struct CommitResult
{
    CSN csn = INVALID_CSN;
};

/// Reader view returned by `takeQuerySnapshot`.
using QuerySnapshotResult = QuerySnapshot;

/// Per-partition transaction surface composing the strategies (commit
/// coordinator, bitmap store, pin registry); the snapshot capture lives
/// inline here. This is what the MergeTree consumers (sink / merge / scan /
/// DROP) couple to.
///
/// Threading contract:
///   - `commit` is NOT thread-safe on its own — the caller must hold the
///     partition's writer guard (`controller.lockForWrite()`) across the whole
///     write statement. The coordinator's INTERNAL publish lock serializes only
///     the publish region against `takeQuerySnapshot`'s atomic capture.
///   - `takeQuerySnapshot` is safe from any thread, including
///     concurrently with `commit` — the coordinator's internal lock
///     makes `(parts, csn, pin)` capture atomic with the publish region.
///     Readers do NOT take the writer mutex.
///   - `runGcRound` runs on the background scheduler.
///
/// Broken-part quarantine: if a `commit` rollback cannot remove an
/// already-installed bitmap (a double failure — publish threw, then
/// `removeBitmap` threw), the orphaned `delete_bitmap_<csn>.rbm` is stranded on
/// that active target part, and csn-seed would surface it as committed on the
/// next reload. Rather than latch an in-memory poison bit (which neither
/// survives reload nor lets recovery's tmp-scan reach an active-part orphan),
/// `commit` records the affected targets and `takeOrphanedTargets` hands them to
/// the caller (which owns the `MergeTreeData`) to quarantine the safe runtime way
/// MergeTree uses for DETACH/DROP PART — `removePartsFromWorkingSet` (-> Outdated,
/// deferred refcount-gated physical removal; never renames a live part's files)
/// plus a clone into `detached/` for recovery. Once Outdated the part leaves the
/// active set, so the orphan can no longer be surfaced. (Durability tail: a crash
/// in the narrow window after the part goes Outdated but before its deferred
/// physical removal can reload it active again; fully closing that needs load-time
/// reconciliation of active-part sidecars against the committed floor — TODO.)
///
/// Lock nesting: no method holds a stock-MergeTree lock
/// (`MergeTreeData::data_parts_mutex`) while holding a strategy-internal
/// lock. Strategies acquire their own locks for at most one operation.
class PartitionTxnController
{
public:
    /// The three strategies are owned (all non-null).
    PartitionTxnController(
        std::unique_ptr<ICommitCoordinator> coordinator_,
        std::unique_ptr<IBitmapStore>       bitmap_store_,
        std::unique_ptr<IPinRegistry>       pin_registry_);

    ~PartitionTxnController();

    /// Writer + merge commit. Single attempt: read the snapshot → prepare the
    /// per-target cumulative bitmaps → linearize the publish via the
    /// coordinator (on Local the coordinator never reports a conflict, so no
    /// retry).
    ///
    /// Concurrency contract: the CALLER must hold the partition's writer guard
    /// (`controller.lockForWrite()`) across the entire write statement. `commit`
    /// reads `prev_bitmap` from `IBitmapStore::readBitmap` BEFORE entering the
    /// coordinator's publish lock, so two unserialized `commit` calls would each
    /// cumulate against a stale `prev_bitmap` and the later would overwrite the
    /// earlier's kills.
    CommitResult commit(CommitRequest req);

    /// Per-partition writer lock; forwards to the coordinator. The caller holds
    /// the returned guard across the whole write statement (resolve → stage →
    /// commit). Local returns a held per-partition mutex (pessimistic); a future
    /// Shared coordinator returns an empty guard (CAS retry carries serialization).
    [[nodiscard]] std::unique_lock<std::mutex> lockForWrite() { return coordinator->lockForWrite(); }

    /// Atomic snapshot capture + pin install. Returns a `Pinned<>` that holds
    /// the snapshot + pin until destroyed (RAII).
    QuerySnapshotResult takeQuerySnapshot();

    /// Reclaim disk space from superseded delete-bitmap versions that no
    /// live query snapshot can still read (superseded below
    /// `IPinRegistry::clusterFloor()`); without GC the bitmap sidecars grow
    /// unbounded. Mechanism: `IBitmapStore::removeBitmap` for each such
    /// version.
    void runGcRound();

    /// Recovery (Local): tmp-only scan; for each tmp dir's part storage, read
    /// its manifest, `removeBitmap` the listed (target, csn) pairs, then remove
    /// the tmp dir. `tmp_storages` is the set of tmp-dir part storages that
    /// existed at startup.
    void recover(std::vector<MutableDataPartStoragePtr> tmp_storages);

    /// Accessor used by the GC round driver (and tests). Returns the
    /// current cluster floor without taking the GC round lock.
    CSN clusterFloor() const;

    /// After `commit` throws on a rollback double-fault (its undo could not
    /// remove a bitmap it installed), returns + clears the affected target part
    /// names. The caller — which owns the `MergeTreeData` — must detach them as
    /// broken so the orphaned `delete_bitmap_<csn>.rbm` leaves the active set
    /// (csn-seed would otherwise surface it as committed on the next reload).
    /// Empty after a clean commit or a cleanly-rolled-back failure. Call under
    /// the same writer guard (`lockForWrite`) the commit was issued under.
    std::vector<PartName> takeOrphanedTargets();

private:
    /// One touched part's cumulative payload (`prev ∪ new_kills`) ready to
    /// install at the commit's assigned csn. Read-only once built.
    struct CumulativeDelete
    {
        PartName part;
        ConstDeleteBitmapPtr bitmap;
    };

    /// Pre-publish (lock-free): per-target `prev_bitmap(at snapshot_csn) ∪
    /// new_kills` (the cumulative-bitmap invariant). Targets with no committed
    /// prior AND no delta are dropped.
    std::vector<CumulativeDelete> prepareCumulativePayloads(const CommitRequest & req, CSN snapshot_csn);

    /// Writer step run INSIDE the publish lock at `assigned_csn`:
    /// (1) finalize manifest, (2) install each cumulative sidecar, (3) publish
    /// (rename tmp→active). On any throw, roll back this commit's installs and
    /// rethrow. `req` carries the finalize/publish callbacks.
    void publishUnderLock(CSN assigned_csn, std::vector<CumulativeDelete> & prepared, const CommitRequest & req);

    /// Reverse-order rollback of `committed_puts` at `assigned_csn`. A
    /// `removeBitmap` that itself throws leaves the orphan on that target's
    /// storage and records the target in `orphaned_targets` for the caller to
    /// detach as broken (see the class note); the remaining installs still undo.
    void rollbackInstalls(const std::vector<PartName> & committed_puts, CSN assigned_csn);

    std::unique_ptr<ICommitCoordinator> coordinator;
    std::unique_ptr<IBitmapStore>       bitmap_store;
    std::unique_ptr<IPinRegistry>       pin_registry;

    /// Targets whose orphaned install a `commit` rollback could not remove
    /// (double-fault); drained by `takeOrphanedTargets`. Guarded by the caller's
    /// writer guard (the same serialization as `commit`), not a dedicated mutex —
    /// `takeQuerySnapshot` never touches it.
    std::vector<PartName> orphaned_targets;
};

using PartitionTxnControllerPtr = std::unique_ptr<PartitionTxnController>;

}
