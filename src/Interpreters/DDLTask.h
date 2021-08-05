#pragma once

#include <Core/Types.h>
#include <Interpreters/Cluster.h>
#include <Common/ZooKeeper/Types.h>

namespace Poco
{
class Logger;
}

namespace zkutil
{
class ZooKeeper;
}

namespace DB
{

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
    UInt64 version = 1;
    String query;
    std::vector<HostID> hosts;
    String initiator; // optional
    std::optional<SettingsChanges> settings;

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

    virtual String getShardID() const = 0;

    virtual ContextPtr makeQueryContext(ContextPtr from_context, const ZooKeeperPtr & zookeeper);

    inline String getActiveNodePath() const { return entry_path + "/active/" + host_id_str; }
    inline String getFinishedNodePath() const { return entry_path + "/finished/" + host_id_str; }
    inline String getShardNodePath() const { return entry_path + "/shards/" + getShardID(); }

    static String getLogEntryName(UInt32 log_entry_number);
    static UInt32 getLogEntryNumber(const String & log_entry_name);
};

struct DDLTask : public DDLTaskBase
{
    DDLTask(const String & name, const String & path) : DDLTaskBase(name, path) {}

    bool findCurrentHostID(ContextPtr global_context, Poco::Logger * log);

    void setClusterInfo(ContextPtr context, Poco::Logger * log);

    String getShardID() const override;

private:
    bool tryFindHostInCluster();
    bool tryFindHostInClusterViaResolving(ContextPtr context);

    HostID host_id;
    String cluster_name;
    ClusterPtr cluster;
    Cluster::Address address_in_cluster;
    size_t host_shard_num;
    size_t host_replica_num;
};

struct DatabaseReplicatedTask : public DDLTaskBase
{
    DatabaseReplicatedTask(const String & name, const String & path, DatabaseReplicated * database_);

    String getShardID() const override;
    void parseQueryFromEntry(ContextPtr context) override;
    ContextPtr makeQueryContext(ContextPtr from_context, const ZooKeeperPtr & zookeeper) override;

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
    Coordination::Requests ops;

public:
    ZooKeeperMetadataTransaction(const ZooKeeperPtr & current_zookeeper_, const String & zookeeper_path_, bool is_initial_query_)
    : current_zookeeper(current_zookeeper_)
    , zookeeper_path(zookeeper_path_)
    , is_initial_query(is_initial_query_)
    {
    }

    bool isInitialQuery() const { return is_initial_query; }

    bool isExecuted() const { return state != CREATED; }

    String getDatabaseZooKeeperPath() const { return zookeeper_path; }

    void addOp(Coordination::RequestPtr && op)
    {
        assert(!isExecuted());
        ops.emplace_back(op);
    }

    void moveOpsTo(Coordination::Requests & other_ops)
    {
        assert(!isExecuted());
        std::move(ops.begin(), ops.end(), std::back_inserter(other_ops));
        ops.clear();
        state = COMMITTED;
    }

    void commit();

    ~ZooKeeperMetadataTransaction() { assert(isExecuted() || std::uncaught_exceptions()); }
};

ClusterPtr tryGetReplicatedDatabaseCluster(const String & cluster_name);

}
