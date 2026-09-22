#pragma once

#include <Client/ConnectionPool_fwd.h>
#include <Core/Protocol.h>
#include <Core/Types.h>
#include <Common/Macros.h>
#include <Common/Exception.h>
#include <Common/MultiVersion.h>
#include <Common/Priority.h>

#include <Poco/Net/SocketAddress.h>
#include <Poco/Timespan.h>

#include <map>
#include <optional>
#include <string>
#include <unordered_set>

namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }
}

namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct DatabaseReplicaInfo
{
    String hostname;
    String shard_name;
    String replica_name;
    std::optional<bool> is_local;
};

/// List of replica hostnames grouped per shard. Used to construct a Cluster for the remote() function.
using HostsByShard = std::vector<Strings>;

struct ClusterConnectionParameters
{
    const String & username;
    const String & password;
    UInt16 clickhouse_port;
    bool treat_local_as_remote;
    bool treat_local_port_as_remote;
    bool secure = false;
    const String & bind_host;
    Priority priority{1};
    String cluster_name;
    String cluster_secret;
};

/// Cluster contains connection pools to each node
/// With the local nodes, the connection is not established, but the request is executed directly.
/// Therefore we store only the number of local nodes
/// In the config, the cluster includes nodes <node> or <shard>
class Cluster
{
public:
    Cluster(const Poco::Util::AbstractConfiguration & config,
            const Settings & settings,
            const String & config_prefix_,
            const String & cluster_name);

    /// Construct a cluster by the names of shards and replicas.
    /// Local are treated as well as remote ones if treat_local_as_remote is true.
    /// Local are also treated as remote if treat_local_port_as_remote is set and the local address includes a port
    /// 'clickhouse_port' - port that this server instance listen for queries.
    /// This parameter is needed only to check that some address is local (points to ourself).
    ///
    /// Used for remote() function.
    ///
    /// `shard_keys` are the per-shard keys this constructor's caller grouped `names` by and then
    /// discarded - the shards are renumbered `1..N` here regardless. They are what makes a shard number
    /// of this cluster mean something, so they, and not `params.cluster_name`, form the shard-scope
    /// identity (see `getShardScopeIdentity`): the same name describes a different numbering as soon as
    /// the caller's visible membership differs. A caller that has no such keys passes none and gets no
    /// identity, which declines a shard scope rather than trusting the name.
    ///
    /// `shard_scope_key` plays the same role it plays for a `Replicated` database below: it says whose
    /// shard keys those are, for keys that are per-cluster numbers rather than the shards' membership.
    /// A discovered cluster's keys are each node's own `discovery.shard`, so two discovery paths with
    /// shards `0`/`1` over different hosts must stay apart - but two `remote_servers` entries over one
    /// path read the same znodes and cannot disagree about which shard a node belongs to, so such a
    /// caller passes the discovery path and keeps parallel replicas through either name. A caller
    /// without such a key leaves it empty and `params.cluster_name` is used.
    Cluster(
        const Settings & settings,
        const HostsByShard & names,
        const ClusterConnectionParameters & params,
        const Strings & shard_keys = {},
        const String & shard_scope_key = {});


    /// The shards are renumbered `1..N` here as well, so the shard-scope identity comes from each
    /// shard's `DatabaseReplicaInfo::shard_name` rather than from `params.cluster_name`.
    ///
    /// `shard_scope_key` says whose shard names those are. Shard names are chosen per database and
    /// repeat across databases, so the key must tell one database from another, but it must not tell
    /// apart two spellings of the same one: a `Replicated` database is reachable both as `<db>` and as
    /// `all_groups.<db>`, and when both spellings see the same ordered shards, a shard number means the
    /// same shard through either. Such a caller passes a spelling-independent key - the database's
    /// Keeper name and path (see `makeKeeperScopeKey`). A caller without one leaves it empty and
    /// `params.cluster_name` is used.
    Cluster(
        const Settings & settings,
        const std::vector<std::vector<DatabaseReplicaInfo>> & infos,
        const ClusterConnectionParameters & params,
        bool internal_replication = false,
        const String & shard_scope_key = {});

    /// The scope key of a cluster whose shards live in Keeper, for `ClusterDiscovery` and a `Replicated`
    /// database: the Keeper name together with the path. A path is unique only inside one Keeper - two
    /// unrelated databases can be mounted at `/clickhouse/db` on two auxiliary Keepers - so the path
    /// alone would let a `_shard_num` produced by one of them pass as scoped on the other. The name is
    /// length-prefixed so the boundary between the two parts cannot slide.
    static String makeKeeperScopeKey(const String & zookeeper_name, const String & zookeeper_path);

    Cluster(const Cluster &)= delete;
    Cluster & operator=(const Cluster &) = delete;

    /// is used to set a limit on the size of the timeout
    static Poco::Timespan saturate(Poco::Timespan v, Poco::Timespan limit);

    using SlotToShard = std::vector<UInt64>;

    struct Address
    {
        /** In configuration file,
        * addresses are located either in <node> elements:
        * <node>
        *     <host>example01-01-1</host>
        *     <port>9000</port>
        *     <!-- <user>, <password>, <default_database>, <compression>, <priority>. <secure>, <bind_host> if needed -->
        * </node>
        * ...
        * or in <shard> and inside in <replica> elements:
        * <shard>
        *     <replica>
        *         <host>example01-01-1</host>
        *         <port>9000</port>
        *         <!-- <user>, <password>, <default_database>, <compression>, <priority>. <secure>, <bind_host> if needed -->
        *    </replica>
        * </shard>
        */

        String host_name;
        String database_shard_name;
        String database_replica_name;
        UInt16 port{0};
        /// Optional per-node ports for the distributed-plan engine: the interserver port the
        /// initiator dispatches tasks to, and the streaming-exchange listener port. Zero means
        /// "not configured" (the initiator then falls back to the server-level ports).
        UInt16 stateless_worker_port{0};
        UInt16 streaming_exchange_port{0};
        String user;
        String password;
        String proto_send_chunked = "notchunked";
        String proto_recv_chunked = "notchunked";
        String quota_key;

        /// For inter-server authorization
        String cluster;
        String cluster_secret;

        UInt32 shard_index{}; /// shard serial number in configuration file, starting from 1.
        UInt32 replica_index{}; /// replica serial number in this shard, starting from 1; zero means no replicas.

        /// This database is selected when no database is specified for Distributed table
        String default_database;
        /// The locality is determined at the initialization, and is not changed even if DNS is changed
        /// The locality can be auto-reinitialized by reloading cluster config if DNSCacheUpdater is enabled
        bool is_local = false;
        bool user_specified = false;

        Protocol::Compression compression = Protocol::Compression::Enable;
        Protocol::Secure secure = Protocol::Secure::Disable;

        String bind_host;

        Priority priority{1};

        Address() = default;

        Address(
            const Poco::Util::AbstractConfiguration & config,
            const String & config_prefix,
            const String & cluster_,
            const String & cluster_secret_,
            UInt32 shard_index_ = 0,
            UInt32 replica_index_ = 0);

        Address(
            const DatabaseReplicaInfo & info,
            const ClusterConnectionParameters & params,
            UInt32 shard_index_,
            UInt32 replica_index_);

        /// Returns 'escaped_host_name:port'
        String toString() const;

        /// Returns 'host_name:port'
        String readableString() const;

        static String toString(const String & host_name, UInt16 port);

        static std::pair<String, UInt16> fromString(const String & host_port_string);

        /// Returns escaped shard{shard_index}_replica{replica_index} or escaped
        /// user:password@resolved_host_address:resolved_host_port#default_database
        /// depending on use_compact_format flag
        String toFullString(bool use_compact_format) const;

        /// Returns address with only shard index and replica index or full address without shard index and replica index
        static Address fromFullString(std::string_view full_string);

        /// Returns resolved address if it does resolve.
        std::optional<Poco::Net::SocketAddress> getResolvedAddress() const;

        auto tuple() const { return std::tie(host_name, port, secure, user, password, default_database, bind_host); }
        bool operator==(const Address & other) const { return tuple() == other.tuple(); }

    private:
        bool isLocal(UInt16 clickhouse_port) const;
    };

    using Addresses = std::vector<Address>;
    using AddressesWithFailover = std::vector<Addresses>;

    /// Name of directory for asynchronous write to StorageDistributed if has_internal_replication
    ///
    /// Contains different path for permutations of:
    /// - prefer_localhost_replica
    ///   Notes with prefer_localhost_replica==0 will contains local nodes.
    /// - use_compact_format_in_distributed_parts_names
    ///   See toFullString()
    ///
    /// This is cached to avoid looping by replicas in insertPathForInternalReplication().
    struct ShardInfoInsertPathForInternalReplication
    {
        /// prefer_localhost_replica == 1 && use_compact_format_in_distributed_parts_names=0
        std::string prefer_localhost_replica;
        /// prefer_localhost_replica == 0 && use_compact_format_in_distributed_parts_names=0
        std::string no_prefer_localhost_replica;
        /// use_compact_format_in_distributed_parts_names=1
        std::string compact;
    };

    struct ShardInfo
    {
    public:
        bool isLocal() const { return !local_addresses.empty(); }
        bool hasRemoteConnections() const { return local_addresses.size() != per_replica_pools.size(); }
        size_t getLocalNodeCount() const { return local_addresses.size(); }
        size_t getRemoteNodeCount() const { return per_replica_pools.size() - local_addresses.size(); }
        size_t getAllNodeCount() const { return per_replica_pools.size(); }
        bool hasInternalReplication() const { return has_internal_replication; }
        /// Name of directory for asynchronous write to StorageDistributed if has_internal_replication
        const std::string & insertPathForInternalReplication(bool prefer_localhost_replica, bool use_compact_format) const;

        ShardInfoInsertPathForInternalReplication insert_path_for_internal_replication;
        /// Number of the shard, the indexation begins with 1
        UInt32 shard_num = 0;
        String name;
        UInt32 weight = 1;
        Addresses local_addresses;
        /// nullptr if there are no remote addresses
        ConnectionPoolWithFailoverPtr pool;
        /// Connection pool for each replica, contains nullptr for local replicas
        ConnectionPoolPtrs per_replica_pools;
        bool has_internal_replication = false;
        String default_database;
    };

    using ShardsInfo = std::vector<ShardInfo>;

    const ShardsInfo & getShardsInfo() const { return shards_info; }
    const AddressesWithFailover & getShardsAddresses() const { return addresses_with_failover; }

    /// Returns addresses of some replicas according to specified `only_shard_num` and `only_replica_num`.
    /// `only_shard_num` is 1-based index of a shard, 0 means all shards.
    /// `only_replica_num` is 1-based index of a replica, 0 means all replicas.
    std::vector<const Address *> filterAddressesByShardOrReplica(size_t only_shard_num, size_t only_replica_num) const;

    const ShardInfo & getAnyShardInfo() const
    {
        if (shards_info.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cluster is empty");
        return shards_info.front();
    }

    /// The number of remote shards.
    size_t getRemoteShardCount() const { return remote_shard_count; }

    /// The number of clickhouse nodes located locally
    /// we access the local nodes directly.
    size_t getLocalShardCount() const { return local_shard_count; }

    /// The number of all shards.
    size_t getShardCount() const { return shards_info.size(); }

    /// Returns an array of arrays of strings in the format 'escaped_host_name:port' for all replicas of all shards in the cluster.
    std::vector<Strings> getHostIDs() const;

    const String & getSecret() const { return secret; }

    /// Get a subcluster consisting of one shard - index by count (from 0) of the shard of this cluster.
    std::unique_ptr<Cluster> getClusterWithSingleShard(size_t index) const;

    /// Get a subcluster consisting of one or multiple shards - indexes by count (from 0) of the shard of this cluster.
    std::unique_ptr<Cluster> getClusterWithMultipleShards(const std::vector<size_t> & indices) const;

    /// Get a new Cluster that contains all servers (all shards with all replicas) from existing cluster as independent shards.
    std::unique_ptr<Cluster> getClusterWithReplicasAsShards(const Settings & settings, size_t max_replicas_from_shard = 0) const;

    /// Returns false if cluster configuration doesn't allow to use it for cross-replication.
    /// NOTE: true does not mean, that it's actually a cross-replication cluster.
    bool maybeCrossReplication() const;

    /// Are distributed DDL Queries (ON CLUSTER Clause) allowed for this cluster
    bool areDistributedDDLQueriesAllowed() const { return allow_distributed_ddl_queries; }

    const String & getName() const { return name; }

    /// Identifies the shard NUMBERING rather than the cluster: two clusters share it only when a shard
    /// number denotes the same shard in both. Deriving a cluster keeps the name but may renumber the
    /// shards, so the name cannot serve this purpose. Empty identifies nothing and never compares equal.
    const String & getShardScopeIdentity() const { return shard_scope_identity; }

private:
    SlotToShard slot_to_shard;

public:
    const SlotToShard & getSlotToShard() const { return slot_to_shard; }

private:
    void initMisc();

    /// Namespaces of `shard_scope_identity` values. A cluster name and a `Replicated` database name share
    /// one namespace, so an identity a reader can spell is also one a user can name a database - and then
    /// that database's cluster would authenticate a shard number it never produced. Every identity is
    /// therefore prefixed with the shape that built it, and no identity is a bare name: a name is equal on
    /// both sides of a hop by construction and so identifies no numbering.
    static constexpr auto CONFIG_SHARDS_SCOPE = "config-shards ";
    static constexpr auto HOSTS_BY_SHARD_SCOPE = "hosts-by-shard ";
    static constexpr auto REPLICAS_BY_SHARD_SCOPE = "replicas-by-shard ";

    /// Builds a shard-scope identity out of the ordered shard keys a constructor renumbered away.
    /// Every part is written length-prefixed, so no two different (prefix, key, keys) triples can spell
    /// the same identity however the parts are punctuated. No keys means no identity.
    ///
    /// `scope_key` is the namespace the shard keys are chosen in, and is empty when they need none:
    /// replica sets identify a shard wherever they are read, while shard `<name>`s and the shard names
    /// of a `Replicated` database only identify one within their cluster or database. Leaving it out
    /// where it is not needed is what lets two names for the same ordered shards compare equal.
    static String makeShardScopeIdentity(std::string_view prefix, const String & scope_key, const Strings & shard_keys);

    /// For getClusterWithMultipleShards implementation.
    struct SubclusterTag {};
    Cluster(SubclusterTag, const Cluster & from, const std::vector<size_t> & indices);

    /// For getClusterWithReplicasAsShards implementation
    struct ReplicasAsShardsTag {};
    Cluster(ReplicasAsShardsTag, const Cluster & from, const Settings & settings, size_t max_replicas_from_shard);

    void addShard(
        const Settings & settings,
        Addresses addresses,
        bool treat_local_as_remote,
        UInt32 current_shard_num,
        String current_shard_name = "",
        UInt32 weight = 1,
        bool internal_replication = false);

    /// Inter-server secret
    String secret;

    /// Description of the cluster shards.
    ShardsInfo shards_info;
    /// Any remote shard.
    ShardInfo * any_remote_shard_info = nullptr;

    /// Non-empty is either addresses or addresses_with_failover.
    /// The size and order of the elements in the corresponding array corresponds to shards_info.

    /// An array of shards. For each shard, an array of replica addresses (servers that are considered identical).
    AddressesWithFailover addresses_with_failover;

    bool allow_distributed_ddl_queries = true;

    size_t remote_shard_count = 0;
    size_t local_shard_count = 0;

    String name;
    String shard_scope_identity;
};

using ClusterPtr = std::shared_ptr<Cluster>;


class Clusters
{
public:
    Clusters(const Poco::Util::AbstractConfiguration & config, const Settings & settings, MultiVersion<Macros>::Version macros, const String & config_prefix = "remote_servers");

    Clusters(const Clusters &) = delete;
    Clusters & operator=(const Clusters &) = delete;

    ClusterPtr getCluster(const std::string & cluster_name) const;
    void setCluster(const String & cluster_name, const ClusterPtr & cluster);

    void updateClusters(const Poco::Util::AbstractConfiguration & new_config, const Settings & settings, const String & config_prefix, Poco::Util::AbstractConfiguration * old_config = nullptr);

    using Impl = std::map<String, ClusterPtr>;

    Impl getContainer() const;

protected:

    /// setup outside of this class, stored to prevent deleting from impl on config update
    std::unordered_set<std::string> automatic_clusters;

    MultiVersion<Macros>::Version macros_;

    Impl impl;
    mutable std::mutex mutex;
};

}
