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
class DatabaseReplicated;

struct MetadataTransaction;
using MetadataTransactionPtr = std::shared_ptr<MetadataTransaction>;

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
    String query;
    std::vector<HostID> hosts;
    String initiator; // optional

    static constexpr int CURRENT_VERSION = 1;

    String toString() const;

    void parse(const String & data);
};

struct DDLTaskBase
{
    const String entry_name;
    const String entry_path;

    DDLLogEntry entry;

    String host_id_str;
    ASTPtr query;

    bool is_circular_replicated = false;
    bool execute_on_leader = false;

    //MetadataTransactionPtr txn;
    Coordination::Requests ops;
    ExecutionStatus execution_status;
    bool was_executed = false;

    DDLTaskBase(const String & name, const String & path) : entry_name(name), entry_path(path) {}
    DDLTaskBase(const DDLTaskBase &) = delete;
    DDLTaskBase(DDLTaskBase &&) = default;
    virtual ~DDLTaskBase() = default;

    void parseQueryFromEntry(const Context & context);

    virtual String getShardID() const = 0;

    virtual std::unique_ptr<Context> makeQueryContext(Context & from_context);

    inline String getActiveNodePath() const { return entry_path + "/active/" + host_id_str; }
    inline String getFinishedNodePath() const { return entry_path + "/finished/" + host_id_str; }
    inline String getShardNodePath() const { return entry_path + "/shards/" + getShardID(); }

};

struct DDLTask : public DDLTaskBase
{
    DDLTask(const String & name, const String & path) : DDLTaskBase(name, path) {}

    bool findCurrentHostID(const Context & global_context, Poco::Logger * log);

    void setClusterInfo(const Context & context, Poco::Logger * log);

    String getShardID() const override;

private:
    bool tryFindHostInCluster();
    bool tryFindHostInClusterViaResolving(const Context & context);

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
    std::unique_ptr<Context> makeQueryContext(Context & from_context) override;

    static String getLogEntryName(UInt32 log_entry_number);
    static UInt32 getLogEntryNumber(const String & log_entry_name);

    DatabaseReplicated * database;
    bool we_are_initiator = false;
};


struct MetadataTransaction
{
    enum State
    {
        CREATED,
        COMMITED,
        FAILED
    };

    State state = CREATED;
    ZooKeeperPtr current_zookeeper;
    String zookeeper_path;
    bool is_initial_query;
    Coordination::Requests ops;

    void addOps(Coordination::Requests & other_ops)
    {
        std::move(ops.begin(), ops.end(), std::back_inserter(other_ops));
    }

    void commit();

};

}
