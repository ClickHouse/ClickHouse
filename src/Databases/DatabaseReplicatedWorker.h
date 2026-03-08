#pragma once
#include <Interpreters/DDLReplicateWorker.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/QualifiedTableName.h>

namespace DB
{

class DatabaseReplicated;

/// It's similar to DDLReplicateWorker, but has the following differences:
/// 1. DDL queue in ZooKeeper is not shared between multiple clusters and databases,
///    each DatabaseReplicated has its own queue in ZooKeeper and DatabaseReplicatedDDLWorker object.
/// 2. Shards and replicas are identified by shard_name and replica_name arguments of database engine,
///    not by address:port pairs. Cluster (of multiple database replicas) is identified by its zookeeper_path.
class DatabaseReplicatedDDLWorker : public DDLReplicateWorker
{
public:
    DatabaseReplicatedDDLWorker(DatabaseReplicated * db, ContextPtr context_);

    String tryEnqueueAndExecuteEntry(DDLLogEntry & entry, ContextPtr query_context, bool internal_query) override;
private:
    bool beforeReplication() override;
    void scheduleTasks(bool reinitialized) override;
    DDLTaskPtr initAndCheckTask(const String & entry_name, String & out_reason, const ZooKeeperPtr & zookeeper, bool dry_run) override;

    bool checkParentTableExists(const UUID & uuid) const;

    bool shouldSkipCreatingRMVTempTable(const ZooKeeperPtr & zookeeper, UUID parent_uuid, UUID create_uuid, int64_t ddl_log_ctime);
    bool shouldSkipRenamingRMVTempTable(const ZooKeeperPtr & zookeeper, UUID parent_uuid, const QualifiedTableName & rename_from_table);

    DDLReplicateTaskPtr createTask(const String & name, const String & path) const override;
    void checkBeforeProcessEntry(DDLLogEntry & entry) override;

    DatabaseReplicated * const database;

    // When the log entry is dummy, it indicates that a replica is added or removed.
    // We need to update the cached cluster
    // However, we don't update it for every dummy query.
    // We only update after processing a batch of queries to avoid sending too many requests to Keeper.
    // Because each update calls `getClusterImpl`, which sends a request to Keeper.
    bool need_update_cached_cluster{false};
};

}
