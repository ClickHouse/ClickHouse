#pragma once
#include <Core/Types.h>
#include <Interpreters/Cluster.h>


namespace DB
{

class ASTQueryWithOnCluster;

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


struct DDLTask
{
    /// Stages of task lifetime correspond ordering of these data fields:

    /// Stage 1: parse entry
    String entry_name;
    String entry_path;
    DDLLogEntry entry;

    /// Stage 2: resolve host_id and check that
    HostID host_id;
    String host_id_str;

    /// Stage 3.1: parse query
    ASTPtr query;
    ASTQueryWithOnCluster * query_on_cluster = nullptr;

    /// Stage 3.2: check cluster and find the host in cluster
    String cluster_name;
    ClusterPtr cluster;
    Cluster::Address address_in_cluster;
    size_t host_shard_num;
    size_t host_replica_num;

    /// Stage 3.3: execute query
    ExecutionStatus execution_status;
    bool was_executed = false;

    /// Stage 4: commit results to ZooKeeper
};


}
