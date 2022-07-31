#pragma once
#include <Interpreters/DDLWorker.h>

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

    String enqueueQuery(DDLLogEntry & entry) override;

    String tryEnqueueAndExecuteEntry(DDLLogEntry & entry, ContextPtr query_context);

    void shutdown() override;

    static String enqueueQueryImpl(const ZooKeeperPtr & zookeeper, DDLLogEntry & entry,
                                   DatabaseReplicated * const database, bool committed = false); /// NOLINT

private:
    bool initializeMainThread() override;
    void initializeReplication();

    DDLTaskPtr initAndCheckTask(const String & entry_name, String & out_reason, const ZooKeeperPtr & zookeeper) override;
    bool canRemoveQueueEntry(const String & entry_name, const Coordination::Stat & stat) override;

    DatabaseReplicated * const database;
    mutable std::mutex mutex;
    std::condition_variable wait_current_task_change;
    String current_task;
    std::atomic<UInt32> logs_to_keep = std::numeric_limits<UInt32>::max();
};

}
