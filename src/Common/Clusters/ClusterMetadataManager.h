#pragma once

#include <Common/Clusters/ClusterCatalogTypes.h>
#include <Common/Clusters/ClusterMetadataImporter.h>
#include <Common/Clusters/ClusterMetadataMutation.h>
#include <Common/Clusters/ClusterMetadataDDLWorker.h>
#include <Common/Clusters/ClusterMetadataStorage.h>
#include <Common/SettingsChanges.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context_fwd.h>
#include <QueryPipeline/BlockIO.h>
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
    UInt32 max_log_entries_per_batch = 64;

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
    /// When `sync` is true, waits for all registered replica-group nodes and returns a status pipeline.
    BlockIO createEndpoint(const String & endpoint_name, const EndpointCatalogDefinition & definition, bool if_not_exists = false, bool sync = false, ContextPtr query_context = {});
    BlockIO dropEndpoint(const String & endpoint_name, bool if_exists = false, bool sync = false, ContextPtr query_context = {});
    BlockIO alterEndpoint(const String & endpoint_name, const SettingsChanges & properties, bool sync = false, ContextPtr query_context = {});

    /// DDL -- shard (SQL catalog; `replica_collections` are endpoint names).
    /// Returns empty BlockIO when `if_not_exists` is true and the SQL shard catalog row already exists (no-op).
    BlockIO createShard(
        const String & shard_name,
        const std::vector<String> & replica_collections,
        UInt32 weight,
        bool internal_replication,
        bool if_not_exists = false,
        bool sync = false,
        ContextPtr query_context = {});
    BlockIO dropShard(const String & shard_name, bool if_exists, bool sync = false, ContextPtr query_context = {});
    /// `ALTER SHARD name MODIFY PROPERTIES (...)` — merge into existing catalog row; empty BlockIO if `IF EXISTS` and shard missing.
    BlockIO updateShardPropertiesFromSQL(const ASTAlterShardQuery & query, bool sync = false, ContextPtr query_context = {});
    /// `ALTER SHARD name ADD REPLICA collection` — append endpoint to replica list; empty BlockIO if `IF EXISTS` and shard missing.
    BlockIO addReplicaToShardFromSQL(const ASTAlterShardQuery & query, bool sync = false, ContextPtr query_context = {});
    /// `ALTER SHARD name DROP REPLICA collection` — remove from replica list (endpoint is not dropped); empty BlockIO if `IF EXISTS` and shard missing.
    BlockIO dropReplicaFromShardFromSQL(const ASTAlterShardQuery & query, bool sync = false, ContextPtr query_context = {});
    /// `ALTER SHARD name REPLACE ... TO ...` — rename which endpoints back replicas (pairwise, simultaneous); optional trailing shard `MODIFY PROPERTIES`; empty BlockIO if `IF EXISTS` and shard missing.
    BlockIO replaceShardReplicasFromSQL(const ASTAlterShardQuery & query, bool sync = false, ContextPtr query_context = {});

    /// DDL -- cluster.
    /// Returns empty BlockIO when `if_not_exists` is true and the SQL cluster catalog row already exists (no-op).
    BlockIO createCluster(
        const String & cluster_name,
        const std::vector<String> & members,
        const String & cluster_secret = {},
        bool allow_distributed_ddl_queries = true,
        bool if_not_exists = false,
        bool sync = false,
        ContextPtr query_context = {});
    BlockIO dropCluster(const String & cluster_name, bool if_exists, bool sync = false, ContextPtr query_context = {});
    /// `ALTER CLUSTER ... ADD SHARD s1, ...` — append members; empty BlockIO if `IF EXISTS` and cluster missing.
    BlockIO addClusterMembersFromSQL(const ASTAlterClusterQuery & query, bool sync = false, ContextPtr query_context = {});
    /// `ALTER CLUSTER ... DROP SHARD s1, ...` — remove members; empty BlockIO if `IF EXISTS` and cluster missing.
    BlockIO dropClusterMembersFromSQL(const ASTAlterClusterQuery & query, bool sync = false, ContextPtr query_context = {});
    /// `ALTER CLUSTER ... REPLACE ... TO ...` — remap members; optional cluster `MODIFY PROPERTIES`; empty BlockIO if `IF EXISTS` and cluster missing.
    BlockIO replaceClusterMembersFromSQL(const ASTAlterClusterQuery & query, bool sync = false, ContextPtr query_context = {});

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
    BackgroundSchedulePool::TaskHolder materialization_task;
    bool materialization_requested = false;
    UInt64 materialized_snapshot_version = 0;

    const LoggerPtr log = getLogger("ClusterMetadataManager");
    static constexpr UInt64 MATERIALIZATION_INTERVAL_MS = 1000;

    bool isEnabled() const;
    [[noreturn]] void throwIfDisabled() const;
    void commitMutation(const ClusterMetadataMutation & mutation);
    /// Enqueue mutation and return a status pipeline waiting for all registered replicas (`SYNC`).
    BlockIO commitMutationSync(const ClusterMetadataMutation & mutation, ContextPtr query_context);
    BlockIO finishCommit(const ClusterMetadataMutation & mutation, bool sync, ContextPtr query_context);
    ClusterMetadataDDLWorker::PreparedMutation prepareMutation(const ClusterMetadataMutation & mutation) const;
    String applyMutations(const std::vector<ClusterMetadataMutation> & mutations);
    void applyMutationToSnapshot(ClusterMetadataStorage::Snapshot & target, const ClusterMetadataMutation & mutation) const;
    ClusterMetadataMutation materializeMetadataMutation(
        const ClusterMetadataStorage::Snapshot & source_snapshot,
        const ClusterMetadataMutation & mutation) const;
    void normalizeSnapshot(ClusterMetadataStorage::Snapshot & target) const;
    void validateSnapshotReferences(const ClusterMetadataStorage::Snapshot & target) const;
    void reloadSnapshotUnlocked();
    void requestSnapshotMaterialization();
    void materializationTask();
    UInt64 publishSnapshotToClusterFactory() const;
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
    void resolveEndpointsForShard(ShardCatalogDefinition & shard, const ClusterMetadataStorage::Snapshot & source_snapshot) const;
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
