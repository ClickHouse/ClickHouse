#pragma once

#include <Core/Types.h>
#include <Interpreters/Cluster.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/SettingsChanges.h>
#include <Common/ZooKeeper/Types.h>
#include <filesystem>

namespace Poco
{
class Logger;
}

namespace zkutil
{
class ZooKeeper;
}

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class ASTQueryWithOnCluster;
using ZooKeeperPtr = std::shared_ptr<zkutil::ZooKeeper>;
using ClusterPtr = std::shared_ptr<Cluster>;
class DatabaseReplicated;

class ZooKeeperMetadataTransaction;
using ZooKeeperMetadataTransactionPtr = std::shared_ptr<ZooKeeperMetadataTransaction>;

struct HostID
{
    String host_name;
    UInt16 port;

    HostID() = default;

    explicit HostID(const Cluster::Address & address)
        : host_name(address.host_name), port(address.port) {}

    HostID(const String & host_name_, UInt16 port_)
        : host_name(host_name_), port(port_) {}

    static HostID fromString(const String & host_port_str);

    String toString() const
    {
        return Cluster::Address::toString(host_name, port);
    }

    String readableString() const
    {
        return host_name + ":" + DB::toString(port);
    }

    bool isLocalAddress(UInt16 clickhouse_port) const;

    static String applyToString(const HostID & host_id)
    {
        return host_id.toString();
    }
};


struct DDLLogEntry
{
    static constexpr const UInt64 OLDEST_VERSION = 1;
    static constexpr const UInt64 SETTINGS_IN_ZK_VERSION = 2;
    static constexpr const UInt64 NORMALIZE_CREATE_ON_INITIATOR_VERSION = 3;
    static constexpr const UInt64 OPENTELEMETRY_ENABLED_VERSION = 4;
    static constexpr const UInt64 PRESERVE_INITIAL_QUERY_ID_VERSION = 5;
    static constexpr const UInt64 BACKUP_RESTORE_FLAG_IN_ZK_VERSION = 6;
    static constexpr const UInt64 PARENT_TABLE_UUID_VERSION = 7;
    /// Add new version here

    /// Remember to update the value below once new version is added
    static constexpr const UInt64 DDL_ENTRY_FORMAT_MAX_VERSION = 7;

    UInt64 version = 1;
    String query;
    std::vector<HostID> hosts;
    String initiator; // optional
    std::optional<SettingsChanges> settings;
    OpenTelemetry::TracingContext tracing_context;
    String initial_query_id;
    bool is_backup_restore = false;
    /// If present, this entry should be executed only if table with this uuid exists.
    /// Only for DatabaseReplicated.
    std::optional<UUID> parent_table_uuid;

    void setSettingsIfRequired(ContextPtr context);
    String toString() const;
    void parse(const String & data);
    void assertVersion() const;
};

struct DDLTaskBase
{
    const String entry_name;
    const String entry_path;

    DDLLogEntry entry;

    String host_id_str;
    ASTPtr query;

    String query_str;
    String query_for_logging;

    bool is_initial_query = false;
    bool is_circular_replicated = false;
    bool execute_on_leader = false;

    Coordination::Requests ops;
    ExecutionStatus execution_status;
    bool was_executed = false;

    std::atomic_bool completely_processed = false;

    DDLTaskBase(const String & name, const String & path) : entry_name(name), entry_path(path) {}
    DDLTaskBase(const DDLTaskBase &) = delete;
    virtual ~DDLTaskBase() = default;

    virtual void parseQueryFromEntry(ContextPtr context);
    void formatRewrittenQuery(ContextPtr context);

    virtual String getShardID() const = 0;

    virtual ContextMutablePtr makeQueryContext(ContextPtr from_context, const ZooKeeperPtr & zookeeper);
    virtual Coordination::RequestPtr getOpToUpdateLogPointer() { return nullptr; }

    virtual void createSyncedNodeIfNeed(const ZooKeeperPtr & /*zookeeper*/) {}

    String getActiveNodePath() const { return fs::path(entry_path) / "active" / host_id_str; }
    String getFinishedNodePath() const { return fs::path(entry_path) / "finished" / host_id_str; }
    String getShardNodePath() const { return fs::path(entry_path) / "shards" / getShardID(); }
    String getSyncedNodePath() const { return fs::path(entry_path) / "synced" / host_id_str; }

    static String getLogEntryName(UInt32 log_entry_number);
    static UInt32 getLogEntryNumber(const String & log_entry_name);
};

struct DDLTask : public DDLTaskBase
{
    DDLTask(const String & name, const String & path) : DDLTaskBase(name, path) {}

    bool findCurrentHostID(ContextPtr global_context, LoggerPtr log, const ZooKeeperPtr & zookeeper, const std::optional<std::string> & config_host_name);

    void setClusterInfo(ContextPtr context, LoggerPtr log);

    String getShardID() const override;

private:
    bool tryFindHostInCluster();
    bool tryFindHostInClusterViaResolving(ContextPtr context);

    HostID host_id;
    String cluster_name;
    ClusterPtr cluster;
    Cluster::Address address_in_cluster;
    size_t host_shard_num = 0;
    size_t host_replica_num = 0;
};

struct DatabaseReplicatedTask : public DDLTaskBase
{
    DatabaseReplicatedTask(const String & name, const String & path, DatabaseReplicated * database_);

    String getShardID() const override;
    void parseQueryFromEntry(ContextPtr context) override;
    ContextMutablePtr makeQueryContext(ContextPtr from_context, const ZooKeeperPtr & zookeeper) override;
    Coordination::RequestPtr getOpToUpdateLogPointer() override;
    void createSyncedNodeIfNeed(const ZooKeeperPtr & zookeeper) override;

    DatabaseReplicated * database;
};

/// The main purpose of ZooKeeperMetadataTransaction is to execute all zookeeper operation related to query
/// in a single transaction when we performed all required checks and ready to "commit" changes.
/// For example, create ALTER_METADATA entry in ReplicatedMergeTree log,
/// create path/to/entry/finished/host_id node in distributed DDL queue to mark query as executed and
/// update metadata in path/to/replicated_database/metadata/table_name
/// It's used for DatabaseReplicated.
/// TODO we can also use it for ordinary ON CLUSTER queries
class ZooKeeperMetadataTransaction
{
    enum State
    {
        CREATED,
        COMMITTED,
        FAILED
    };

    State state = CREATED;
    ZooKeeperPtr current_zookeeper;
    String zookeeper_path;
    bool is_initial_query;
    String task_path;
    Coordination::Requests ops;

    /// CREATE OR REPLACE is special query that consists of 3 separate DDL queries (CREATE, RENAME, DROP)
    /// and not all changes should be applied to metadata in ZooKeeper
    /// (otherwise we may get partially applied changes on connection loss).
    /// So we need this flag to avoid doing unnecessary operations with metadata.
    bool is_create_or_replace_query = false;

public:
    ZooKeeperMetadataTransaction(const ZooKeeperPtr & current_zookeeper_, const String & zookeeper_path_, bool is_initial_query_, const String & task_path_)
    : current_zookeeper(current_zookeeper_)
    , zookeeper_path(zookeeper_path_)
    , is_initial_query(is_initial_query_)
    , task_path(task_path_)
    {
    }

    bool isInitialQuery() const { return is_initial_query; }

    bool isExecuted() const { return state != CREATED; }

    String getDatabaseZooKeeperPath() const { return zookeeper_path; }

    String getTaskZooKeeperPath() const { return task_path; }

    ZooKeeperPtr getZooKeeper() const { return current_zookeeper; }

    void setIsCreateOrReplaceQuery() { is_create_or_replace_query = true; }

    bool isCreateOrReplaceQuery() const { return is_create_or_replace_query; }

    void addOp(Coordination::RequestPtr && op)
    {
        if (isExecuted())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add ZooKeeper operation because query is executed. It's a bug.");
        ops.emplace_back(op);
    }

    void moveOpsTo(Coordination::Requests & other_ops)
    {
        if (isExecuted())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add ZooKeeper operation because query is executed. It's a bug.");
        std::move(ops.begin(), ops.end(), std::back_inserter(other_ops));
        ops.clear();
        state = COMMITTED;
    }

    void commit();

    ~ZooKeeperMetadataTransaction() { assert(isExecuted() || std::uncaught_exceptions() || ops.empty()); }
};

ClusterPtr tryGetReplicatedDatabaseCluster(const String & cluster_name);

}
