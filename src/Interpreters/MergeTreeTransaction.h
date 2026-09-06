#pragma once
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <base/scope_guard.h>
#include <boost/noncopyable.hpp>
#include <Common/Stopwatch.h>
#include <Common/TransactionID.h>
#include <Common/ZooKeeper/IKeeper.h>

#include <cstdint>
#include <list>
#include <shared_mutex>
#include <unordered_set>
#include <Common/SharedMutex.h>

namespace DB
{

class IMergeTreeDataPart;
struct TransactionInfoContext;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using DataPartsVector = std::vector<DataPartPtr>;

/// This object is responsible for tracking all changes that some transaction is making in MergeTree tables.
/// It collects all changes that queries of current transaction made in data part sets of all MergeTree tables
/// to either make them visible when transaction commits or undo when transaction rolls back.
class MergeTreeTransaction : public std::enable_shared_from_this<MergeTreeTransaction>, private boost::noncopyable
{
    friend class TransactionManager;
public:
    enum State
    {
        RUNNING,
        COMMITTING,
        COMMITTED,
        ROLLED_BACK,
    };

    CSN getSnapshot() const { return snapshot.load(std::memory_order_relaxed); }
    void setSnapshot(CSN new_snapshot);
    State getState() const;

    const TransactionID tid;

    MergeTreeTransaction(CSN snapshot_, LocalTID local_tid_, UUID host_id, Int64 session_version_, std::list<CSN>::iterator snapshot_it_);

    void addNewPart(const StoragePtr & storage, const DataPartPtr & new_part);
    void removeOldPart(
        const StoragePtr & storage, const DataPartPtr & part_to_remove, const TransactionInfoContext & context,
        LockKind kind, const LockFingerprint & held_lock_fingerprint = {});

    void addMutation(const StoragePtr & table, const String & mutation_id);

    /// Record that this transaction holds `part`'s `<part>/removal_lock` znode, so rollback
    /// can release every held lock in one place. `acquired` is the lock's fingerprint at
    /// acquire time, used on release to tell our lock apart from one recreated by someone else.
    void addLockedPart(
        const StoragePtr & storage,
        const DataPartPtr & part,
        LockKind kind,
        LockFingerprint acquired = {});

    struct LockedPart
    {
        StoragePtr storage;
        DataPartPtr part;
        LockKind kind;
        LockFingerprint acquired;

        bool operator==(const LockedPart &) const = default;
    };

    static void addNewPart(const StoragePtr & storage, const DataPartPtr & new_part, MergeTreeTransaction * txn);
    static void removeOldPart(const StoragePtr & storage, const DataPartPtr & part_to_remove, MergeTreeTransaction * txn, LockKind kind);
    static void addNewPartAndRemoveCovered(
        const StoragePtr & storage, const DataPartPtr & new_part, const DataPartsVector & covered_parts,
        MergeTreeTransaction * txn, LockKind kind);

    bool isReadOnly() const;

    void onException();

    String dumpDescription() const;

    Float64 elapsedSeconds() const { return elapsed.elapsedSeconds(); }

    /// Waits for transaction state to become not equal to the state corresponding to current_state_csn
    bool waitStateChange(CSN current_state_csn) const;

    CSN getCSN() const { return csn; }

    /// Allocate a fresh, non-zero `JobId` for a background operation of this transaction (a
    /// merge/mutation of its own uncommitted parts). The operation carries this transaction's `tid`
    /// plus this job id, giving the operation's locks and marker a unique identity while its part
    /// version metadata keeps the plain `tid`. See `DB::JobId`.
    JobId allocateJobId()
    {
        return static_cast<JobId>(job_id_counter.fetch_add(1, std::memory_order_relaxed) + 1);
    }

    /// Returns a shared lock on `commit_gate` if the transaction is still RUNNING, else an empty lock
    /// (`owns_lock() == false`). A background operation holds it so the transaction's own commit (which
    /// takes `commit_gate` exclusive) cannot create the CSN entry until the operation releases it.
    std::shared_lock<SharedMutex> isRunning()
    {
        std::shared_lock<SharedMutex> lock{commit_gate};
        if (getState() != RUNNING)
            return {};
        return lock;
    }

    void addRequestsOnCommit(const Coordination::Requests & requests);
    void addRequestOnCommit(Coordination::RequestPtr request);
    Coordination::Requests getRequestsOnCommit() const;

    void addRequestsOnRollback(const Coordination::Requests & requests);
    void addRequestOnRollback(Coordination::RequestPtr request);
    Coordination::Requests getRequestsOnRollback() const;

    /// One SMT table touched by this transaction. `zk_path` is embedded in the
    /// stamp znode for orphan cleanup. The part lists are audit/diagnostics.
    struct AffectedSMTTable
    {
        Int64 cross_replica_id;
        String zk_path;
        std::vector<String> added_part_names;
        std::vector<String> removed_part_names;
    };

    /// SMT tables this transaction touched (non-SMT storages skipped). Used by
    /// the bounded-snapshot mechanism and the per-CSN archive eligibility check.
    std::vector<AffectedSMTTable> getAffectedSMTTables() const;

private:
    scope_guard beforeCommit();
    void afterCommit(CSN assigned_csn) noexcept;

    /// Outcome of `rollback`. `NotNeeded`: committed/cancelled concurrently. `Ok`: rolled back
    /// cleanly. `Failed`: rolled back, but a best-effort Keeper write failed, so the caller bumps
    /// the session once for peer reclaim.
    enum class RollbackResult : uint8_t { NotNeeded, Ok, Failed };

    /// Restores the parts this transaction removed. A part keeps its removal lock unless the
    /// `removal_tid` stamp was cleared: stamped but unlocked looks free to the next remover,
    /// which then overwrites the stamp. Restart clears a kept stamp via `loadAndUpdateMetadata`.
    RollbackResult rollback() noexcept;
    void afterFinalize();

    void checkIsNotCancelled() const;

    mutable std::mutex mutex;
    Stopwatch elapsed;

    /// Usually it's equal to tid.start_csn, but can be changed by SET SNAPSHOT query (for introspection purposes and time-traveling)
    std::atomic<CSN> snapshot;
    const std::list<CSN>::iterator snapshot_in_use_it;

    bool finalized TSA_GUARDED_BY(mutex) = false;

    /// Indicates if transaction was read-only before `afterFinalize`
    bool is_read_only TSA_GUARDED_BY(mutex) = false;

    /// Lists of changes made by transaction
    std::unordered_set<StoragePtr> storages TSA_GUARDED_BY(mutex);
    DataPartsVector creating_parts TSA_GUARDED_BY(mutex);
    /// Parts this transaction is removing, with the `<part>/removal_lock` fingerprint each
    /// arrived with: captured by `lockRemovalTID` on the plain path in `removeOldPart`, or
    /// handed over by a caller that already holds the lock. `afterCommit` and `rollback` pass
    /// it back to `unlockRemovalTID`, which needs it to identify the znode instance we
    /// acquired — a peer's `tryTakeOver` in between then surfaces as `ABORTED` instead of a
    /// release of the peer's lock. `kind` is the caller's: `REMOVAL`, `DROP`, or the merge /
    /// mutation / optimize kind of a source that arrived holding its lock.
    std::vector<LockedPart> removing_parts TSA_GUARDED_BY(mutex);
    using RunningMutationsList = std::vector<std::pair<StoragePtr, String>>;
    RunningMutationsList mutations TSA_GUARDED_BY(mutex);

    /// `<part>/removal_lock` znodes a merge / mutation / optimize of this transaction holds on
    /// its source parts, so `kind` is never `REMOVAL` here.
    /// Populated by `addLockedPart`; consumed by the rollback path.
    std::vector<LockedPart> locked_parts TSA_GUARDED_BY(mutex);

    Coordination::Requests requests_on_commit TSA_GUARDED_BY(mutex);
    Coordination::Requests requests_on_rollback TSA_GUARDED_BY(mutex);

    std::atomic<CSN> csn;

    /// Counter for `allocateJobId`. Each background operation of this transaction (a merge/mutation
    /// of its own uncommitted parts) takes the next value, so their locks and marker get distinct job
    /// ids. Foreground work uses `tid` with `Tx::MainJobId` (0).
    std::atomic<JobId> job_id_counter{0};

    /// Serialises a background operation's commit `multi` (which stamps this transaction's main TID
    /// onto part metadata) against this transaction's own commit. `isRunning` takes it shared;
    /// commit and rollback (in `TransactionManager`, a friend) take it exclusive around the point the
    /// transaction becomes committed/rolled-back. See `isRunning`.
    SharedMutex commit_gate;
};

using MergeTreeTransactionPtr = std::shared_ptr<MergeTreeTransaction>;

}
