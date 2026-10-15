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

#include <atomic>
#include <map>
#include <memory>
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
    /// `IF NOT EXISTS` no-op is decided after DDL log catch-up in `prepareMutation`.
    BlockIO createShard(
        const String & shard_name,
        const std::vector<String> & replica_collections,
        UInt32 weight,
        bool internal_replication,
        bool if_not_exists = false,
        bool sync = false,
        ContextPtr query_context = {});
    BlockIO dropShard(const String & shard_name, bool if_exists, bool sync = false, ContextPtr query_context = {});
    /// `ALTER SHARD name MODIFY PROPERTIES (...)` — merge into existing catalog row; `IF EXISTS` no-op after catch-up.
    BlockIO updateShardPropertiesFromSQL(const ASTAlterShardQuery & query, bool sync = false, ContextPtr query_context = {});
    /// `ALTER SHARD name ADD REPLICA collection` — append endpoint to replica list; `IF EXISTS` no-op after catch-up.
    BlockIO addReplicaToShardFromSQL(const ASTAlterShardQuery & query, bool sync = false, ContextPtr query_context = {});
    /// `ALTER SHARD name DROP REPLICA collection` — remove from replica list (endpoint is not dropped); `IF EXISTS` no-op after catch-up.
    BlockIO dropReplicaFromShardFromSQL(const ASTAlterShardQuery & query, bool sync = false, ContextPtr query_context = {});
    /// `ALTER SHARD name REPLACE ... TO ...` — rename which endpoints back replicas (pairwise, simultaneous); optional trailing shard `MODIFY PROPERTIES`; `IF EXISTS` no-op after catch-up.
    BlockIO replaceShardReplicasFromSQL(const ASTAlterShardQuery & query, bool sync = false, ContextPtr query_context = {});

    /// DDL -- cluster.
    /// `IF NOT EXISTS` no-op is decided after DDL log catch-up in `prepareMutation`.
    BlockIO createCluster(
        const String & cluster_name,
        const std::vector<String> & members,
        const String & cluster_secret = {},
        bool allow_distributed_ddl_queries = true,
        bool if_not_exists = false,
        bool sync = false,
        ContextPtr query_context = {});
    BlockIO dropCluster(const String & cluster_name, bool if_exists, bool sync = false, ContextPtr query_context = {});
    /// `ALTER CLUSTER ... ADD SHARD s1, ...` — append members; `IF EXISTS` no-op after catch-up.
    BlockIO addClusterMembersFromSQL(const ASTAlterClusterQuery & query, bool sync = false, ContextPtr query_context = {});
    /// `ALTER CLUSTER ... DROP SHARD s1, ...` — remove members; `IF EXISTS` no-op after catch-up.
    BlockIO dropClusterMembersFromSQL(const ASTAlterClusterQuery & query, bool sync = false, ContextPtr query_context = {});
    /// `ALTER CLUSTER ... REPLACE ... TO ...` — remap members; optional cluster `MODIFY PROPERTIES`; `IF EXISTS` no-op after catch-up.
    BlockIO replaceClusterMembersFromSQL(const ASTAlterClusterQuery & query, bool sync = false, ContextPtr query_context = {});

private:
    ClusterMetadataManager() = default;

    mutable std::mutex mutex;
    /// Cleared at the start of `shutdown` so new DDL / callback entry points bail out via
    /// `throwIfDisabled` before `ddl_worker` (and other owned components) are torn down.
    std::atomic<bool> initialized{false};

    ContextPtr context;
    ClusterMetadataConfig config;
    ClusterMetadataStorage::Snapshot snapshot;
    UInt64 snapshot_version = 0;
    ClusterMetadataStoragePtr storage;
    std::unique_ptr<ClusterMetadataDDLWorker> ddl_worker;
    ClusterMetadataImporterPtr importer;
    BackgroundSchedulePool::TaskHolder materialization_task;
    bool materialization_requested = false;
    UInt64 materialized_snapshot_version = 0;

    const LoggerPtr log = getLogger("ClusterMetadataManager");
    static constexpr UInt64 MATERIALIZATION_INTERVAL_MS = 1000;

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

    ShardCatalogDefinition buildShardDefinition(
        const String & shard_name,
        const std::vector<String> & endpoint_names,
        UInt32 weight,
        bool internal_replication) const;
    static void resolveShardEndpoints(
        ShardCatalogDefinition & shard,
        const std::unordered_map<String, EndpointCatalogDefinition> & endpoints);

    void materializeSnapshotClusters(
        const ClusterMetadataStorage::Snapshot & source_snapshot,
        ContextPtr query_context,
        std::map<String, ClusterPtr> & out) const;
    /// Materialize imported (read-only) clusters into `out`. Names already present are left unchanged
    /// (local / earlier import wins); among imports, the first configured group wins.
    void materializeImportedClusters(
        const std::vector<ClusterMetadataImporter::ImportedSnapshot> & imported_snapshots,
        ContextPtr query_context,
        std::map<String, ClusterPtr> & out) const;
};

}
