#pragma once

#include <Interpreters/DDLWorker.h>

#include <limits>

namespace DB
{

class DDLReplicator;
struct DDLReplicateTask;
using DDLReplicateTaskPtr = std::unique_ptr<DDLReplicateTask>;

/// It's similar to DDLWorker, but has the following differences:
/// 1. Shards and replicas are identified by shard_name and replica_name arguments,
///    not by address:port pairs.
/// 2. After creation of an entry in DDL queue initiator tries to execute the entry locally
///    and other hosts wait for query to finish on initiator host.
///    If query succeed on initiator, then all hosts must execute it, so they will retry until query succeed.
///    We assume that cluster is homogeneous, so if replicas are in consistent state and query succeed on one host,
///    then all hosts can execute it (maybe after several retries).
/// 3. Each replica stores its log pointer in ZooKeeper. Cleanup thread removes old entry
///    if its number < max_log_ptr - logs_to_keep.
class DDLReplicateWorker : public DDLWorker
{
public:
    DDLReplicateWorker(
        DDLReplicator * replicator_,
        ContextPtr context_,
        const String & logger_name);

    static constexpr const char * FORCE_AUTO_RECOVERY_DIGEST = "42";

    String enqueueQuery(DDLLogEntry & entry, const ZooKeeperRetriesInfo &) override;
    virtual String tryEnqueueAndExecuteEntry(DDLLogEntry & entry, ContextPtr query_context, bool internal_query);

    void shutdown() override;

    bool waitForReplicaToProcessAllEntries(UInt64 timeout_ms);

    static String enqueueQueryImpl(
        const ZooKeeperPtr & zookeeper,
        DDLLogEntry & entry,
        DDLReplicator * const replicator, /// NOLINT
        bool committed = false,
        Coordination::Requests additional_checks = {});

    UInt32 getLogPointer() const;

    UInt64 getCurrentInitializationDurationMs() const;

    bool isUnsyncedAfterRecovery() const { return unsynced_after_recovery; }
protected:
    bool initializeMainThread() override;
    /// Called before `initializeReplication` in `initializeMainThread`.
    /// Return if continue to initialize replication.
    virtual bool beforeReplication() { return true; }
    void initializeReplication() override;
    DDLTaskPtr initAndCheckTask(const String & entry_name, String & out_reason, const ZooKeeperPtr & zookeeper, bool dry_run) override;
    DDLTaskPtr initAndCheckTaskImpl(
        const String & entry_name,
        String & out_reason,
        const ZooKeeperPtr & zookeeper,
        bool dry_run,
        Coordination::Stat * entry_stats = nullptr);

    void createReplicaDirs(const ZooKeeperPtr &, const NameSet &) override {}
    void markReplicasActive(bool reinitialized) override;

    void initializeLogPointer(const String & processed_entry_name);

    bool canRemoveQueueEntry(const String & entry_name, const Coordination::Stat & stat) override;

    virtual DDLReplicateTaskPtr createTask(const String & name, const String & path) const;
    virtual void checkBeforeProcessEntry(DDLLogEntry &) {}

    DDLReplicator * const replicator;

    mutable std::mutex mutex;
    std::condition_variable wait_current_task_change;

    String current_task;
    std::atomic<UInt32> logs_to_keep = std::numeric_limits<UInt32>::max();
    std::atomic_bool unsynced_after_recovery = false;

    /// EphemeralNodeHolder has reference to ZooKeeper, it may become dangling
    ZooKeeperPtr active_node_holder_zookeeper;
    zkutil::EphemeralNodeHolderPtr active_node_holder;

    std::optional<Stopwatch> initialization_duration_timer;
    mutable std::mutex initialization_duration_timer_mutex;
};

}
