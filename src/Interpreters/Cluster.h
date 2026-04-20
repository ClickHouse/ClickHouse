#pragma once

#include <Client/ConnectionPool_fwd.h>
#include <Core/Protocol.h>
#include <Common/Macros.h>
#include <Common/Exception.h>
#include <Common/MultiVersion.h>
#include <Common/Priority.h>

#include <Poco/Net/SocketAddress.h>
#include <Poco/Timespan.h>

#include <map>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_set>
#include <vector>

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

/// Where the definition of a resolvable cluster name comes from.
/// Used by the unified `Clusters` registry (owned by `ClusterFactory`) to tell entries from different upstreams apart
/// so reloads from one upstream only touch clusters of that source.
enum class ClusterDefinitionSource : uint8_t
{
    RemoteServersConfig,
    SQLCatalog,
    Discovery,
};

struct DatabaseReplicaInfo
{
    String hostname;
    String shard_name;
    String replica_name;
    std::optional<bool> is_local;
};

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
    Cluster(
        const Settings & settings,
        const std::vector<std::vector<String>> & names,
        const ClusterConnectionParameters & params);


    Cluster(
        const Settings & settings,
        const std::vector<std::vector<DatabaseReplicaInfo>> & infos,
        const ClusterConnectionParameters & params,
        bool internal_replication = false);

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

        /// Recompute `is_local` after fields are set (e.g. when building `Address` outside config parsing).
        void recomputeIsLocal(UInt16 clickhouse_port) { is_local = isLocal(clickhouse_port); }

        auto tuple() const { return std::tie(host_name, port, secure, user, password, default_database, bind_host); }
        bool operator==(const Address & other) const { return tuple() == other.tuple(); }

    private:
        bool isLocal(UInt16 clickhouse_port) const;
    };

    using Addresses = std::vector<Address>;
    using AddressesWithFailover = std::vector<Addresses>;

    /// Input for one shard before connection pools exist. Carries resolved `Address` list plus weight and
    /// `internal_replication`; `Cluster(settings, name, secret, std::vector<ShardInitSpec>)` passes each row to `addShard`, which
    /// builds the corresponding `ShardInfo` (pools, local vs remote split, insert paths, etc.).
    /// Used by `ClusterFactory` when materializing catalog clusters from shard definitions.
    struct ShardInitSpec
    {
        Addresses addresses;
        UInt32 weight = 1;
        bool internal_replication = false;
    };

    /// Build from pre-resolved `ShardInitSpec` rows (no XML config). Used by `ClusterFactory`.
    Cluster(
        const Settings & settings,
        const String & cluster_name_,
        const String & cluster_secret_,
        std::vector<ShardInitSpec> && shard_specs,
        bool allow_distributed_ddl_queries_ = true);

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

    /// Where this cluster was defined (config file, SQL catalog, or cluster discovery).
    /// Default `RemoteServersConfig` matches the common case: `Cluster` built from `<remote_servers>` XML.
    /// SQL catalog and cluster discovery call `setDefinitionMetadata` right after construction.
    ClusterDefinitionSource getDefinitionSource() const { return definition_source; }

    /// Version tag tied to the upstream that defined this cluster:
    /// - `RemoteServersConfig` → `Context::getClustersVersion()` at the time config was applied
    /// - `SQLCatalog` → `ClusterFactory::sql_catalog_mutation_counter` snapshot at materialization time
    /// - `Discovery` → currently 0 (watch state carries its own freshness)
    UInt64 getDefinitionVersion() const { return definition_version; }

    void setDefinitionMetadata(ClusterDefinitionSource source, UInt64 version)
    {
        definition_source = source;
        definition_version = version;
    }

private:
    SlotToShard slot_to_shard;

public:
    const SlotToShard & getSlotToShard() const { return slot_to_shard; }

private:
    void initMisc();

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

    ClusterDefinitionSource definition_source = ClusterDefinitionSource::RemoteServersConfig;
    UInt64 definition_version = 0;
};

using ClusterPtr = std::shared_ptr<Cluster>;


/// Materialised registry of named `Cluster` objects. Intended lifecycle (managed exclusively by `ClusterFactory`):
///   1. Writer constructs or clones a `Clusters` instance it owns alone (the "builder").
///   2. Writer calls mutators (`addCluster`, `removeClusterEntry`, `mergeConfigClusters`) on the builder.
///   3. Writer publishes the builder as `shared_ptr<const Clusters>` through `MultiVersion<ClustersSnapshot>`.
///   4. Readers observe only published instances and call only the `const` accessors.
///
/// There is no internal mutex: published instances are immutable, and builders are single-owner by contract
/// (enforced by `ClusterFactory::clusters_writer_mutex` serialising writes). Calling a mutator on a published
/// `Clusters` is a bug — type system prevents it when callers use `shared_ptr<const Clusters>`.
class Clusters
{
public:
    Clusters(const Poco::Util::AbstractConfiguration & config, const Settings & settings, MultiVersion<Macros>::Version macros, const String & config_prefix = "remote_servers");

    /// Builder seed: copies `impl` (shallow, `shared_ptr<Cluster>` sharing the underlying objects),
    /// `automatic_clusters` and `macros_`. Precondition: `other` is either an already-published snapshot
    /// (immutable, nobody will mutate it) or a writer-owned builder on the same thread — no synchronisation
    /// needed. Intended only for `ClusterFactory::cloneClustersForWriteLocked`.
    Clusters(const Clusters & other);
    Clusters & operator=(const Clusters &) = delete;

    /// --- Read-only accessors, safe on any published snapshot ---
    ClusterPtr getCluster(const std::string & cluster_name) const;
    /// Existence-only probe: avoids the `getCluster(name) != nullptr` idiom on hot read paths and makes call
    /// sites read as a predicate instead of leaking the materialised `ClusterPtr` when callers don't need it.
    bool hasCluster(const std::string & cluster_name) const;

    using Impl = std::map<String, ClusterPtr>;
    Impl getContainer() const;

    /// --- Builder-only mutators ---
    /// Valid only while the caller is the sole owner of `*this` (i.e. before publishing it through
    /// `MultiVersion<ClustersSnapshot>`). Names deliberately differ from the classical `setCluster` / etc. to
    /// make the write-side contract obvious at every call site.
    void addCluster(const String & cluster_name, const ClusterPtr & cluster);
    void removeClusterEntry(const String & cluster_name);
    /// Diff `new_config` against `old_config` (nullptr => treat as empty) and apply the resulting add / remove /
    /// replace operations to `impl`. Entries registered via ClusterDiscovery (`automatic_clusters`) are preserved.
    void mergeConfigClusters(
        const Poco::Util::AbstractConfiguration & new_config,
        const Settings & settings,
        const String & config_prefix,
        const Poco::Util::AbstractConfiguration * old_config = nullptr);

private:
    /// Set of cluster names registered via ClusterDiscovery. Kept across `mergeConfigClusters` so the config
    /// diff does not evict Discovery-owned entries.
    std::unordered_set<std::string> automatic_clusters;

    MultiVersion<Macros>::Version macros_; // NOLINT(readability-identifier-naming)

    Impl impl;
};

}
