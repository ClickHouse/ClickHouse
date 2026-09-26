#pragma once
#include "config.h"
#include <mutex>
#include <optional>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/MergeTreeTransactionHolder.h>
#include <Interpreters/TransactionLog.h>
#include <Interpreters/TransactionPayloads.h>
#include <Interpreters/TransactionSession.h>
#include <base/types.h>
#include <boost/noncopyable.hpp>
#include <Common/ThreadPool_fwd.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{

/// We want to create a TransactionManager object lazily and avoid creation if it's not needed.
/// But we also want to call shutdown() in a specific place to avoid race conditions.
/// We cannot simply use return-static-variable pattern,
/// because a call to shutdown() may construct unnecessary object in this case.
template <typename Derived>
class SingletonHelper : private boost::noncopyable
{
public:
    static Derived & instance()
    {
        Derived * ptr = instance_raw_ptr.load();
        if (likely(ptr))
            return *ptr;

        return createInstanceOrThrow();
    }

    static void shutdownIfAny()
    {
        std::lock_guard lock{instance_mutex};
        if (instance_holder)
            instance_holder->shutdown();
    }

private:
    static Derived & createInstanceOrThrow();

    static inline std::atomic<Derived *> instance_raw_ptr;
    /// It was supposed to be std::optional, but gcc fails to compile it for some reason
    static inline std::shared_ptr<Derived> instance_holder;
    static inline std::mutex instance_mutex;
};

class TransactionsInfoLog;
using TransactionsInfoLogPtr = std::shared_ptr<TransactionsInfoLog>;
using ZooKeeperPtr = std::shared_ptr<zkutil::ZooKeeper>;

/// This class maintains transaction log in ZooKeeper and a list of currently running transactions in memory.
///
/// Each transaction has unique ID (TID, see details below).
/// TransactionID is allocated when transaction begins.
///
/// We use TransactionID to associate changes (created/removed data parts) with transaction that has made/is going to make these changes.
/// To commit a transaction we create sequential node "/path_to_log/log/csn-" in ZK and write TID into this node.
/// Allocated sequential number is a commit timestamp or Commit Sequence Number (CSN). It indicates a (logical) point in time
/// when transaction is committed and all its changes became visible. So we have total order of all changes.
///
/// Also CSNs are used as snapshots: all changes that were made by a transaction that was committed with a CSN less or equal than some_csn
/// are visible in some_csn snapshot.
///
/// TransactionID consists of four parts: (start_csn, local_tid, host_id, session_node_version)
///   - start_csn is the newest CSN that existed when the transaction was started. Together with `local_tid` and `host_id`
///     it gives the TID a globally unique identity.
///   - local_tid is local sequential number of the transaction, each server allocates local_tids independently without requests to ZK
///   - host_id is persistent UUID of host that has started the transaction, it's kind of tie-breaker that makes ID unique across all servers
///   - session_node_version is the Keeper version of `<host_id>_session` at begin time; lets peers detect ghost commits from a dead session
///
/// To check if some transaction is committed or not we fetch "csn-xxxxxx" nodes from ZK and construct TID -> CSN mapping,
/// so for committed transactions we know commit timestamps.
/// However, if we did not find a mapping for some TID, it means one of the following cases:
///    1. Transaction is not committed (yet)
///    2. Transaction is rolled back (quite similar to the first case, but it will never be committed)
///    3. Transactions was committed a long time ago and we removed its entry from the log
/// To distinguish the third case we store a "tail pointer" — see multi-replica notes below.
///
/// Multi-replica model (SharedMergeTree)
/// -------------------------------------
/// Per-replica state under `/replicas/<uuid>_*`:
///   * `_tail_ptr` — this replica's oldest live snapshot CSN. Log cleanup uses
///     `min(live tail_ptrs)` so no replica is stranded reading evicted entries.
///   * `_session`  — persistent znode whose Keeper version is the replica's session
///     identity. Bumped on (re)connect; embedded in every TID this replica produces.
///   * `_active`   — ephemeral, signals liveness. Drops on session expiry.
///
/// Dead-replica detection: if a peer's `_active` is missing for longer than
/// `dead_replica_threshold_ms`, the cleaner replica atomically bumps the peer's
/// `_session` to "DEAD" via Keeper multi. The peer's in-flight commits then fail their
/// `Check(_session, version)` op and are rejected.
///
/// Single-owner cleanup lease: a `cleanup_lock` ephemeral grants the holder exclusive
/// rights to run `markDeadReplicas` and `removeOldEntries`. Lease passes automatically
/// on session expiry and eliminates concurrent read-modify-write on `/log`.
///
/// On-Keeper payloads and their formats live in `TransactionPayloads.h`.
class TransactionManager final : public SingletonHelper<TransactionManager>
{
public:

    TransactionManager();

    ~TransactionManager();

    void shutdown();

    /// Returns the newest snapshot available for reading
    CSN getLatestSnapshot() const { return txn_log.getLatestSnapshot(); }
    /// Returns the oldest snapshot that is visible for some running transaction
    CSN getOldestSnapshot() const;

    /// Allocates TID, returns new transaction object
    MergeTreeTransactionPtr beginTransaction();

    /// Tries to commit transaction. Returns Commit Sequence Number.
    /// Throw if transaction was concurrently killed or if some precommit check failed.
    /// May throw if ZK connection is lost. Transaction status is unknown in this case.
    /// Returns CommittingCSN if throw_on_unknown_status is false and connection was lost.
    CSN commitTransaction(const MergeTreeTransactionPtr & txn, bool throw_on_unknown_status);

    /// Releases locks that were acquired by transaction, releases snapshot, removes transaction from the list of active transactions.
    /// Normally it should not throw, but if it does for some reason (global memory limit exceeded, disk failure, etc)
    /// then we should terminate server and reinitialize it to avoid corruption of data structures. That's why it's noexcept.
    void rollbackTransaction(const MergeTreeTransactionPtr & txn) noexcept;

    /// Returns CSN if transaction with specified ID was committed and UnknownCSN if it was not.
    /// Returns NonTransactionalCSN for any non-transactional TID (sentinel or host-stamped)
    /// without creating a TransactionManager instance.
    ///
    /// On miss in the local `tid_to_csn` map, reads the Keeper `/log` tail directly before
    /// reporting UnknownCSN, so the returned UnknownCSN is authoritative ("not committed")
    /// rather than "consumer is behind Keeper". Throws on persistent Keeper error.
    static CSN getCSN(const TransactionID & tid);

    /// Ensures that getCSN returned UnknownCSN because transaction is not committed and not because entry was removed from the log.
    static void assertTIDIsNotOutdated(const TransactionID & tid);


    /// Returns a pointer to transaction object if it's running or nullptr.
    MergeTreeTransactionPtr tryGetRunningTransaction(const TIDHash & tid);

    /// Snapshot of transactions that were started on this replica and are still RUNNING. Used by
    /// background maintenance that acts on a transaction's own uncommitted data (e.g. SharedMergeTree
    /// merging a transaction's uncommitted parts). Other replicas' transactions are excluded because
    /// their uncommitted data is not local here.
    std::vector<MergeTreeTransactionPtr> getLocalRunningTransactions() const;

    /// See `TransactionSession::isTIDInvalid`.
    bool isTIDInvalid(const TransactionID & tid, JobId job_id) const { return session.isTIDInvalid(tid, job_id); }

    /// Non-transactional TID stamped with this replica's `host_id` and current
    /// `session_node_version`. Use it when a non-transactional op (on a transactions-
    /// enabled table) takes a part-scoped lock: if we die while holding the lock, peers
    /// can detect that and reclaim it. `start_csn` stays `NonTransactionalCSN`, so
    /// `isNonTransactional()` still returns true even though `local_tid` is a unique
    /// per-operation value.
    TransactionID getMyNonTransactionalTID() const;

    /// Same as above, but avoids creating the `TransactionManager` singleton when transactions
    /// are disabled on the calling table — in that case we don't need `host_id` or
    /// `session_node_version`, so returning the sentinel `Tx::NonTransactionalTID` is
    /// sufficient. Use this on call paths that may run on tx-disabled tables.
    static TransactionID getMyNonTransactionalTID(bool transactions_enabled);

    /// This replica's current `_session` Keeper version. Embedded in every TID we
    /// produce, and bumped by `handleReconnection` if a peer declared us dead during a
    /// Keeper disconnection. Lock holders re-check it before commit so they abort if
    /// their own session was bumped while they were holding the lock.
    Int64 getCurrentSessionNodeVersion() const { return session.getVersion().toInt64(); }

    /// Server-wide transactions gate, mirrored from the `allow_experimental_transactions` server
    /// config. Readable without constructing the log, which requires Keeper. The gate is monotonic:
    /// it is never lowered, because transactional state may still require cleanup.
    static void allowTransactions();
    static bool areTransactionsAllowed();

    /// Mark a transaction, or one of its background operations, as invalid so every replica can
    /// reclaim its locks and parts, while leaving other running transactions untouched. `job_id`
    /// (`Tx::MainJobId` = the whole transaction) selects which. See `TransactionSession`.
    void invalidateTID(const TransactionID & tid, JobId job_id, const String & reason = {}) noexcept
    {
        session.invalidateTID(tid, job_id, reason);
    }

    Coordination::RequestPtr makeInvalidateTIDRequest(const TransactionID & tid, JobId job_id, const String & reason) const
    {
        return session.makeInvalidateTIDRequest(tid, job_id, reason);
    }
    String getInvalidTIDRecordPath(const TransactionID & tid, JobId job_id) const
    {
        return session.getInvalidTIDRecordPath(tid, job_id);
    }

    /// In-memory half of `invalidateTID`, for a caller that wrote the record itself. Needed because
    /// `getCSN` reads this set, and waiting for the worker's next `loadInvalidTids` is too late for a
    /// caller that resolves the TID in the same query (a `DROP PART` reloading parts after a takeover).
    void markTIDInvalidInMemory(const TransactionID & tid, JobId job_id) noexcept
    {
        session.markTIDInvalidInMemory(tid, job_id);
    }

    using TransactionsList = std::unordered_map<TIDHash, MergeTreeTransactionPtr>;
    /// Returns copy of list of running transactions.
    TransactionsList getTransactionsList() const;

    /// Waits for provided CSN (and all previous ones) to be loaded from the log.
    /// Returns false if waiting was interrupted (e.g. by shutdown)
    bool waitForCSNLoaded(CSN csn) const { return txn_log.waitForCSNLoaded(csn); }

    bool isShuttingDown() const { return stop_flag.load(); }

    void sync() const { txn_log.sync([this] { return getZooKeeper(); }); }

    static void increaseAsyncTablesLoadingJobNumber();
    static void decreaseAsyncTablesLoadingJobNumber();
    static Int64 asyncTablesLoadingJobNumber();

    String tableLastCommittedTidPath(Int64 cross_replica_id) const { return txn_log.tableLastCommittedTidPath(cross_replica_id); }
    String tableProcessedCSNPath(Int64 cross_replica_id) const { return txn_log.tableProcessedCSNPath(cross_replica_id); }

    /// Last-replica DROP cleanup: remove this table's stamp and processed_csn znodes and clear
    /// the matching in-memory maps.
    void forgetDroppedTable(Int64 cross_replica_id) { txn_log.forgetDroppedTable(getZooKeeper(), cross_replica_id); }

    /// Discovery hooks that advance this replica's per-table stamp (monotonic).
    void updateTableStamp(Int64 cross_replica_id, CSN stamp_csn) { txn_log.updateTableStamp(cross_replica_id, stamp_csn); }
    /// Resolves the stamp's TID via `getCSN` first. A no-op if the log consumer has not caught
    /// up yet — the next discovery cycle retries.
    void updateTableStampFromTID(Int64 cross_replica_id, const TransactionID & stamp_tid);

    void updateTableProcessedCSN(Int64 cross_replica_id, CSN processed_csn)
    {
        txn_log.updateTableProcessedCSN(cross_replica_id, processed_csn);
    }

    /// Advance our local stamp for every table the commit touched so a follow-up
    /// `BEGIN` on this replica sees the commit (read-your-own-writes).
    void advanceAffectedTablesStamps(const std::vector<MergeTreeTransaction::AffectedSMTTable> & affected_tables, CSN csn)
    {
        txn_log.advanceAffectedTablesStamps(affected_tables, csn);
    }

private:
    void loadLogFromZooKeeper() TSA_REQUIRES(mutex);
    void runUpdatingThread();

    /// Restore our `_tail_ptr` and claim our session.
    void initOwnReplicaState() TSA_REQUIRES(mutex);
    void initReplicaNodes() TSA_REQUIRES(mutex);
    /// Re-establish replica presence after a Keeper session loss. Returns the new
    /// `session_node_version` if a peer declared us dead; caller passes it to
    /// `advanceSessionVersionAndRollbackStaleTxns`. `nullopt` otherwise.
    std::optional<ZooKeeperNodeVersion> handleReconnection();
    void updateOwnTailPtr();
    /// Store new `_session` version and roll back txns stamped with a smaller
    /// `session_node_version`. Store and `running_list` snapshot happen under one
    /// `running_list_mutex` so post-bump txns can't slip into the rollback set
    /// (`beginTransaction` reads `session_node_version` under the same lock).
    void advanceSessionVersionAndRollbackStaleTxns(ZooKeeperNodeVersion new_session_v);
    /// Apply what the log just loaded. Takes `running_list_mutex` because `beginTransaction` reads
    /// the snapshot and the local-TID counter under it as one pair.
    void publishLoadedSnapshot(std::optional<CSN> new_snapshot);

    CSN finalizeCommittedTransaction(MergeTreeTransaction * txn, CSN allocated_csn, scope_guard & state_guard) noexcept;

    void tryFinalizeUnknownStateTransactions();

public:
    /// The default Keeper. Tx-enabled SMT tables are constrained to use it.
    ZooKeeperPtr getZooKeeper() const;

private:
    String cleanupLockPath() const;
    /// Best-effort claim of the cleanup lease ephemeral. No-op if another replica holds it.
    void tryAcquireCleanupLock() TSA_REQUIRES(mutex);

    /// Defined out of line: a definition in the header gives every shared object its own copy.
    static std::atomic<Int64> async_tables_loading_job_number;

    /// Out of line for the same reason. See `allowTransactions`.
    static std::atomic<bool> transactions_allowed;

    /// On miss, resolves the TID straight from Keeper via `TransactionLog::resolveGapCSNFromKeeper`
    /// instead of
    /// `sync()`. We must not wait for `runUpdatingThread` here: the caller may hold a data-parts
    /// lock that the updating thread needs, so waiting on it would deadlock. May throw on Keeper
    /// error. Returns the committed CSN, or `Tx::UnknownCSN` (not in the log up to the Keeper
    /// tip read here) / `Tx::RolledBackCSN`. Applies the host-self rolled-back rule: if this
    /// replica is the host that minted `tid` (same `host_id` AND same
    /// `session_node_version`) and there's no `/log` entry for it, then
    /// absence from `running_list` is conclusive — the txn was rolled back.
    CSN getCSNImpl(const TransactionID & tid);

    const ContextPtr global_context;
    LoggerPtr const log;

    /// Guards `zookeeper` and the two ephemeral-node holders.
    mutable std::mutex mutex;

    mutable std::mutex running_list_mutex;
    /// Transactions that are currently processed
    TransactionsList running_list TSA_GUARDED_BY(running_list_mutex);
    /// If we lost connection on attempt to create csn- node then we don't know transaction's state.
    /// In-memory only; lost on crash. Transactional storages must recover persistent state
    /// independently — see `StorageSharedMergeTree::processPartsUpdate`.
    using UnknownStateList = std::vector<std::pair<MergeTreeTransactionPtr, scope_guard>>;
    UnknownStateList unknown_state_list TSA_GUARDED_BY(running_list_mutex);
    UnknownStateList unknown_state_list_loaded TSA_GUARDED_BY(running_list_mutex);
    /// Ordered list of snapshots that are currently used by some transactions. Needed for background cleanup.
    std::list<CSN> snapshots_in_use TSA_GUARDED_BY(running_list_mutex);

    ZooKeeperPtr zookeeper TSA_GUARDED_BY(mutex);
    const String zookeeper_path;

    const String zookeeper_path_replicas;          /// `/clickhouse/txn/replicas/`
    const String zookeeper_path_invalid_tids;      /// `/clickhouse/txn/invalid_tids/`
    const UUID my_replica_id;                      /// = ServerUUID

    std::atomic_bool stop_flag = false;

    /// This replica's session identity and the per-TID invalidation list.
    TransactionSession session;
    /// The CSN log and this replica's view of it. Must be declared after `session` and
    /// `stop_flag`, which it holds by reference.
    TransactionLog txn_log;

    /// `local_tid` of transactional TIDs. Reset for each new snapshot by `publishLoadedSnapshot`.
    std::atomic<LocalTID> local_tid_counter;
    /// Per-operation counter for the `local_tid` of non-transactional TIDs (`getMyNonTransactionalTID`).
    /// Makes each non-transactional operation's TID unique so its locks, removal stamp, and marker
    /// share one identity distinct from other operations. Starts above `MaxReservedLocalTID` so it
    /// never collides with the reserved `NonTransactionalLocalTID` / `DummyLocalTID` sentinels.
    /// Combined with `(host_id, session_node_version)` it is unique across replicas and restarts.
    mutable std::atomic<LocalTID> non_transactional_local_tid_counter{Tx::MaxReservedLocalTID + 1};
    /// RAII holder for the cluster-wide cleanup lease. Non-null only on the cleaner replica.
    zkutil::EphemeralNodeHolderPtr cleanup_lock_holder TSA_GUARDED_BY(mutex);
    /// czxid of the cleanup-lease ephemeral, captured at acquire from the Multi
    /// response's transaction zxid. Used to detect peer re-creation.
    int64_t cleanup_lock_czxid TSA_GUARDED_BY(mutex){0};

    /// Latches once the first `_tail_ptr` update runs, so async table loading only defers it at
    /// startup.
    std::atomic<bool> updated_tail_ptr{false};
    std::unique_ptr<ThreadFromGlobalPool> updating_thread;

    const Float64 fault_probability_before_commit = 0;
    const Float64 fault_probability_after_commit = 0;
};

template <typename Derived>
Derived & SingletonHelper<Derived>::createInstanceOrThrow()
{
    std::lock_guard lock{instance_mutex};
    if (!instance_holder)
    {
        instance_holder = std::make_shared<Derived>();
        instance_raw_ptr = instance_holder.get();
    }
    return *instance_holder;
}

}
