#pragma once

#include <Common/Clusters/ClusterCatalogTypes.h>
#include <Common/Clusters/ClusterMetadataImporter.h>
#include <Common/Clusters/ClusterMetadataMutation.h>
#include <Common/Clusters/ClusterMetadataDDLWorker.h>
#include <Common/Clusters/ClusterMetadataStorage.h>
#include <Common/SettingsChanges.h>
#include <Common/logger_useful.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context_fwd.h>
#include <boost/noncopyable.hpp>

#include <map>
#include <mutex>
#include <optional>
#include <string_view>
#include <vector>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

class ASTAlterClusterQuery;
class ASTAlterShardQuery;

/// Parsed `<cluster_metadata>` server configuration.
struct ClusterMetadataConfig
{
    bool enabled = false;
    String keeper_name;
    String root_path;
    bool encrypted = false;
    String encryption_key_hex;
    String encryption_algorithm = "aes_128_ctr";
    String replica_group;
    std::vector<String> imports;

    /// Resolved Keeper root for the local metadata replication group.
    String local_root;
};

/// Top-level owner and Interpreter-facing facade for SQL-managed cluster metadata.
///
/// Owns configuration parsing, local snapshot cache, the local DDL worker and public API boundaries.
/// Concrete mutation APIs are wired in later steps.
class ClusterMetadataManager : boost::noncopyable
{
public:
    static constexpr std::string_view CONFIG_PREFIX = "cluster_metadata";
    static constexpr std::string_view DEFAULT_REPLICA_GROUP = "default";

    static ClusterMetadataManager & instance();

    ~ClusterMetadataManager();

    void initialize();
    void shutdown();

    static ClusterMetadataConfig parseConfig(
        const Poco::Util::AbstractConfiguration & config,
        std::string_view config_prefix = CONFIG_PREFIX);

    String reloadSnapshot();

    bool hasShard(const String & shard_name) const;
    std::optional<EndpointCatalogDefinition> tryGetEndpoint(const String & endpoint_name) const;
    std::optional<ShardCatalogDefinition> tryGetShard(const String & shard_name) const;
    std::optional<ClusterCatalogDefinition> tryGetCluster(const String & cluster_name) const;

    std::vector<String> listEndpointNames() const;
    std::vector<String> listShardNames() const;
    std::vector<String> listClusterNames() const;

    String getShowCreateShard(const String & shard_name) const;
    String getShowCreateCluster(const String & cluster_name) const;
    std::vector<ShardCatalogDefinition> listShardsForSystemTable() const;
    std::vector<EndpointCatalogSystemTableRow> listEndpointsForSystemTable() const;
    std::vector<String> listSQLClustersContainingMember(const String & member_name) const;

    /// DDL -- endpoint.
    bool createEndpoint(const String & endpoint_name, const EndpointCatalogDefinition & definition, bool if_not_exists = false);
    bool dropEndpoint(const String & endpoint_name, bool if_exists = false);
    void alterEndpoint(const String & endpoint_name, const SettingsChanges & properties);

    /// DDL -- shard (SQL catalog; `replica_collections` are endpoint names).
    /// Returns false when `if_not_exists` is true and the SQL shard catalog row already exists (no-op).
    bool createShard(
        const String & shard_name,
        const std::vector<String> & replica_collections,
        UInt32 weight,
        bool internal_replication,
        bool if_not_exists = false);
    void dropShard(const String & shard_name, bool if_exists);
    /// `ALTER SHARD name MODIFY PROPERTIES (...)` — merge into existing catalog row; returns false if `IF EXISTS` and shard missing.
    bool updateShardPropertiesFromSQL(const ASTAlterShardQuery & query);
    /// `ALTER SHARD name ADD REPLICA collection` — append endpoint to replica list; returns false if `IF EXISTS` and shard missing.
    bool addReplicaToShardFromSQL(const ASTAlterShardQuery & query);
    /// `ALTER SHARD name DROP REPLICA collection` — remove from replica list (endpoint is not dropped); returns false if `IF EXISTS` and shard missing.
    bool dropReplicaFromShardFromSQL(const ASTAlterShardQuery & query);
    /// `ALTER SHARD name REPLACE ... TO ...` — rename which endpoints back replicas (pairwise, simultaneous); optional trailing shard `MODIFY PROPERTIES`; returns false if `IF EXISTS` and shard missing.
    bool replaceShardReplicasFromSQL(const ASTAlterShardQuery & query);

    /// DDL -- cluster.
    /// Returns false when `if_not_exists` is true and the SQL cluster catalog row already exists (no-op).
    bool createCluster(
        const String & cluster_name,
        const std::vector<String> & members,
        const String & cluster_secret = {},
        bool allow_distributed_ddl_queries = true,
        bool if_not_exists = false);
    bool dropCluster(const String & cluster_name, bool if_exists);
    /// `ALTER CLUSTER ... ADD SHARD s1, ...` — append members; returns false if `IF EXISTS` and cluster missing.
    bool addClusterMembersFromSQL(const ASTAlterClusterQuery & query);
    /// `ALTER CLUSTER ... DROP SHARD s1, ...` — remove members; returns false if `IF EXISTS` and cluster missing.
    bool dropClusterMembersFromSQL(const ASTAlterClusterQuery & query);
    /// `ALTER CLUSTER ... REPLACE ... TO ...` — remap members; optional cluster `MODIFY PROPERTIES`; returns false if `IF EXISTS` and cluster missing.
    bool replaceClusterMembersFromSQL(const ASTAlterClusterQuery & query);

private:
    ClusterMetadataManager() = default;

    mutable std::mutex mutex;
    bool initialized = false;

    ContextPtr context;
    ClusterMetadataConfig config;
    ClusterMetadataStorage::Snapshot snapshot;
    UInt64 snapshot_version = 0;
    ClusterMetadataStoragePtr storage;
    ClusterMetadataDDLWorkerPtr ddl_worker;
    ClusterMetadataImporterPtr importer;

    const LoggerPtr log = getLogger("ClusterMetadataManager");

    bool isEnabled() const;
    [[noreturn]] void throwIfDisabled() const;
    void commitMutation(const ClusterMetadataMutation & mutation);
    void commitAlterShard(const ShardCatalogDefinition & definition);
    void commitAlterCluster(const String & cluster_name, const ClusterCatalogDefinition & definition);
    String applyMutation(const ClusterMetadataMutation & mutation);
    void applyMutationUnlocked(const ClusterMetadataMutation & mutation);
    void reloadSnapshotUnlocked();
    void publishSnapshotToClusterFactory() const;
    ClusterPtr materializeClusterFromSnapshot(
        const String & cluster_name,
        const ClusterCatalogDefinition & record,
        const ClusterMetadataStorage::Snapshot & local_snapshot,
        ContextPtr query_context) const;
    ClusterPtr materializeCluster(const String & cluster_name, ContextPtr context) const;

    void assertShardNameAvailable(const String & shard_name) const;
    void assertClusterNameAvailable(const String & cluster_name) const;
    void assertEndpointNameAvailable(const String & endpoint_name) const;
    void validateClusterMemberShardExists(const String & shard_name) const;
    void validateClusterTotalShardWeight(
        const String & cluster_name,
        const std::vector<String> & members,
        const ShardCatalogDefinition * shard_override = nullptr) const;
    std::vector<String> listClustersContainingShard(const String & shard_name) const;

    ShardCatalogDefinition buildShardDefinition(
        const String & shard_name,
        const std::vector<String> & endpoint_names,
        UInt32 weight,
        bool internal_replication) const;
    void resolveEndpointsForShard(ShardCatalogDefinition & shard) const;
    static void resolveShardEndpoints(
        ShardCatalogDefinition & shard,
        const std::unordered_map<String, EndpointCatalogDefinition> & endpoints);
    static bool endpointsMatch(const EndpointCatalogDefinition & lhs, const EndpointCatalogDefinition & rhs);

    void materializeSnapshotClusters(
        const ClusterMetadataStorage::Snapshot & source_snapshot,
        ContextPtr query_context,
        std::map<String, ClusterPtr> & out) const;
};

}
