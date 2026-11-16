#pragma once
#include <Interpreters/DDLWorker.h>
#include <Interpreters/QueryFlags.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/QualifiedTableName.h>
#include <Poco/Event.h>

#include <unordered_set>

namespace DB
{

class DatabaseReplicated;

/// It's similar to DDLWorker, but has the following differences:
/// 1. DDL queue in ZooKeeper is not shared between multiple clusters and databases,
///    each DatabaseReplicated has its own queue in ZooKeeper and DatabaseReplicatedDDLWorker object.
/// 2. Shards and replicas are identified by shard_name and replica_name arguments of database engine,
///    not by address:port pairs. Cluster (of multiple database replicas) is identified by its zookeeper_path.
/// 3. After creation of an entry in DDL queue initiator tries to execute the entry locally
///    and other hosts wait for query to finish on initiator host.
///    If query succeed on initiator, then all hosts must execute it, so they will retry until query succeed.
///    We assume that cluster is homogeneous, so if replicas are in consistent state and query succeed on one host,
///    then all hosts can execute it (maybe after several retries).
/// 4. Each database replica stores its log pointer in ZooKeeper. Cleanup thread removes old entry
///    if its number < max_log_ptr - logs_to_keep.
class DatabaseReplicatedDDLWorker : public DDLWorker
{
public:
    DatabaseReplicatedDDLWorker(DatabaseReplicated * db, ContextPtr context_);

    String enqueueQuery(DDLLogEntry & entry, const ZooKeeperRetriesInfo &) override;

    String tryEnqueueAndExecuteEntry(DDLLogEntry & entry, ContextPtr query_context, QueryFlags flags);

    void shutdown() override;

    /// Returns false on timeout or if the worker is stopped before or during the wait.
    bool waitForReplicaToProcessAllEntries(UInt64 timeout_ms);

    /// The worker was shut down. A stopped worker is never restarted, the database replaces it with a new one.
    bool isStopped() const { return stop_flag; }

    /// Makes `tryEnqueueAndExecuteEntry` reject new initial queries (as if the worker was stopped),
    /// while the worker itself keeps running. Irreversible: the database replaces such a worker afterwards.
    void stopInitialQueries();

    /// Waits until the initial queries that `tryEnqueueAndExecuteEntry` admitted for execution finish.
    /// They execute and commit on the submitting threads, so `shutdown` (which joins only the worker's
    /// own threads) does not wait for them. Requires `stopInitialQueries` or `shutdown` to be called before.
    void waitForForegroundTasks();

    static String enqueueQueryImpl(const ZooKeeperPtr & zookeeper, DDLLogEntry & entry,
                                   DatabaseReplicated * const database, bool committed = false, Coordination::Requests additional_checks = {}); /// NOLINT

    UInt32 getLogPointer() const;

    UInt64 getCurrentInitializationDurationMs() const;

    bool isUnsyncedAfterRecovery() const { return unsynced_after_recovery; }

    static constexpr const char * FORCE_AUTO_RECOVERY_DIGEST = "42";

private:
    bool initializeMainThread() override;
    void initializeReplication() override;

    void scheduleTasks(bool reinitialized) override;

    void createReplicaDirs(const ZooKeeperPtr &, const NameSet &) override { }
    void markReplicasActive(bool reinitialized) override;

    void initializeLogPointer(const String & processed_entry_name);

    DDLTaskPtr initAndCheckTask(const String & entry_name, String & out_reason, const ZooKeeperPtr & zookeeper, bool dry_run) override;
    bool canRemoveQueueEntry(const String & entry_name, const Coordination::Stat & stat) override;
    static bool isBeyondRetention(UInt32 entry_number, UInt32 max_log_ptr, UInt32 logs_to_keep);

    bool checkParentTableExists(const UUID & uuid) const;

    bool shouldSkipCreatingRMVTempTable(const ZooKeeperPtr & zookeeper, UUID parent_uuid, UUID create_uuid, int64_t ddl_log_ctime);
    bool shouldSkipRenamingRMVTempTable(const ZooKeeperPtr & zookeeper, UUID parent_uuid, const QualifiedTableName & rename_from_table);

    DatabaseReplicated * const database;
    mutable std::mutex mutex;
    std::condition_variable wait_current_task_change;

    String current_task;
    /// The number of initial queries being executed by `tryEnqueueAndExecuteEntry` on the submitting threads.
    /// Protected by `mutex`, see `waitForForegroundTasks`.
    size_t foreground_tasks = 0;
    std::condition_variable foreground_tasks_finished;
    /// Set by `stopInitialQueries`. Protected by `mutex`.
    bool initial_queries_stopped = false;
    /// Events of `waitForReplicaToProcessAllEntries` waiting for our `log_ptr` in Keeper to change.
    /// `shutdown` sets them, so that the waiters return without waiting for the timeout.
    std::unordered_set<std::shared_ptr<Poco::Event>> log_ptr_waiters;
    std::atomic<UInt32> logs_to_keep = std::numeric_limits<UInt32>::max();
    std::atomic_bool unsynced_after_recovery = false;

    /// EphemeralNodeHolder has reference to ZooKeeper, it may become dangling
    ZooKeeperPtr active_node_holder_zookeeper;
    /// It will remove "active" node when database is detached
    zkutil::EphemeralNodeHolderPtr active_node_holder;

    std::optional<Stopwatch> initialization_duration_timer;
    mutable std::mutex initialization_duration_timer_mutex;

    // When the log entry is dummy, it indicates that a replica is added or removed.
    // We need to update the cached cluster
    // However, we don't update it for every dummy query.
    // We only update after processing a batch of queries to avoid sending too many requests to Keeper.
    // Because each update calls `getClusterImpl`, which sends a request to Keeper.
    bool need_update_cached_cluster{false};
};

}
