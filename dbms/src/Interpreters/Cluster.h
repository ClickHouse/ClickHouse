#pragma once

#include <map>
#include <Interpreters/Settings.h>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Poco/Net/SocketAddress.h>

namespace DB
{

/// Cluster contains connection pools to each node
/// With the local nodes, the connection is not established, but the request is executed directly.
/// Therefore we store only the number of local nodes
/// In the config, the cluster includes nodes <node> or <shard>
class Cluster
{
public:
    Cluster(Poco::Util::AbstractConfiguration & config, const Settings & settings, const String & cluster_name);

    /// Construct a cluster by the names of shards and replicas.
    /// Local are treated as well as remote ones if treat_local_as_remote is true.
    /// 'clickhouse_port' - port that this server instance listen for queries.
    /// This parameter is needed only to check that some address is local (points to ourself).
    Cluster(const Settings & settings, const std::vector<std::vector<String>> & names,
            const String & username, const String & password,
            UInt16 clickhouse_port, bool treat_local_as_remote);

    Cluster(const Cluster &) = delete;
    Cluster & operator=(const Cluster &) = delete;

    /// is used to set a limit on the size of the timeout
    static Poco::Timespan saturate(const Poco::Timespan & v, const Poco::Timespan & limit);

public:
    struct Address
    {
        /** In configuration file,
        * addresses are located either in <node> elements:
        * <node>
        *     <host>example01-01-1</host>
        *     <port>9000</port>
        *     <!-- <user>, <password>, <default_database> if needed -->
        * </node>
        * ...
        * or in <shard> and inside in <replica> elements:
        * <shard>
        *     <replica>
        *         <host>example01-01-1</host>
        *         <port>9000</port>
        *         <!-- <user>, <password>, <default_database>. <secure> if needed -->
        *    </replica>
        * </shard>
        */
        Poco::Net::SocketAddress resolved_address;
        String host_name;
        UInt16 port;
        String user;
        String password;
        String default_database;    /// this database is selected when no database is specified for Distributed table
        UInt32 replica_num;
        bool is_local;
        Protocol::Compression compression = Protocol::Compression::Enable;
        Protocol::Secure secure = Protocol::Secure::Disable;

        Address() = default;
        Address(Poco::Util::AbstractConfiguration & config, const String & config_prefix);
        Address(const String & host_port_, const String & user_, const String & password_, UInt16 clickhouse_port);

        /// Returns 'escaped_host_name:port'
        String toString() const;

        /// Returns 'host_name:port'
        String readableString() const;

        static String toString(const String & host_name, UInt16 port);

        static void fromString(const String & host_port_string, String & host_name, UInt16 & port);

        /// Retrurns escaped user:password@resolved_host_address:resolved_host_port#default_database
        String toStringFull() const;
    };

    using Addresses = std::vector<Address>;
    using AddressesWithFailover = std::vector<Addresses>;

    struct ShardInfo
    {
    public:
        bool isLocal() const { return !local_addresses.empty(); }
        bool hasRemoteConnections() const { return pool != nullptr; }
        size_t getLocalNodeCount() const { return local_addresses.size(); }
        bool hasInternalReplication() const { return has_internal_replication; }

    public:
        /// Name of directory for asynchronous write to StorageDistributed if has_internal_replication
        std::string dir_name_for_internal_replication;
        /// Number of the shard, the indexation begins with 1
        UInt32 shard_num;
        UInt32 weight;
        Addresses local_addresses;
        /// nullptr if there are no remote addresses
        ConnectionPoolWithFailoverPtr pool;
        /// Connection pool for each replica, contains nullptr for local replicas
        ConnectionPoolPtrs per_replica_pools;
        bool has_internal_replication;
    };

    using ShardsInfo = std::vector<ShardInfo>;

    String getHashOfAddresses() const { return hash_of_addresses; }
    const ShardsInfo & getShardsInfo() const { return shards_info; }
    const AddressesWithFailover & getShardsAddresses() const { return addresses_with_failover; }

    const ShardInfo & getAnyShardInfo() const
    {
        if (shards_info.empty())
            throw Exception("Cluster is empty", ErrorCodes::LOGICAL_ERROR);
        return shards_info.front();
    }

    /// The number of remote shards.
    size_t getRemoteShardCount() const { return remote_shard_count; }

    /// The number of clickhouse nodes located locally
    /// we access the local nodes directly.
    size_t getLocalShardCount() const { return local_shard_count; }

    /// The number of all shards.
    size_t getShardCount() const { return shards_info.size(); }

    /// Get a subcluster consisting of one shard - index by count (from 0) of the shard of this cluster.
    std::unique_ptr<Cluster> getClusterWithSingleShard(size_t index) const;

private:
    using SlotToShard = std::vector<UInt64>;
    SlotToShard slot_to_shard;

public:
    const SlotToShard & getSlotToShard() const { return slot_to_shard; }

private:
    void initMisc();

    /// For getClusterWithSingleShard implementation.
    Cluster(const Cluster & from, size_t index);

    String hash_of_addresses;
    /// Description of the cluster shards.
    ShardsInfo shards_info;
    /// Any remote shard.
    ShardInfo * any_remote_shard_info = nullptr;

    /// Non-empty is either addresses or addresses_with_failover.
    /// The size and order of the elements in the corresponding array corresponds to shards_info.

    /// An array of shards. For each shard, an array of replica addresses (servers that are considered identical).
    AddressesWithFailover addresses_with_failover;

    size_t remote_shard_count = 0;
    size_t local_shard_count = 0;
};

using ClusterPtr = std::shared_ptr<Cluster>;


class Clusters
{
public:
    Clusters(Poco::Util::AbstractConfiguration & config, const Settings & settings, const String & config_name = "remote_servers");

    Clusters(const Clusters &) = delete;
    Clusters & operator=(const Clusters &) = delete;

    ClusterPtr getCluster(const std::string & cluster_name) const;
    void setCluster(const String & cluster_name, const ClusterPtr & cluster);

    void updateClusters(Poco::Util::AbstractConfiguration & config, const Settings & settings, const String & config_name);

public:
    using Impl = std::map<String, ClusterPtr>;

    Impl getContainer() const;

protected:
    Impl impl;
    mutable std::mutex mutex;
};

using ClustersPtr = std::shared_ptr<Clusters>;

}
