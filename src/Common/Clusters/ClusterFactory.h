#pragma once

#include <Common/Clusters/ClusterCatalogTypes.h>
#include <Common/Clusters/ClustersMetadataStorage.h>
#include <Common/Clusters/ShardsMetadataStorage.h>
#include <Common/NamedCollections/NamedCollections_fwd.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <base/defines.h>

#include <Poco/AutoPtr.h>

#include <boost/noncopyable.hpp>

#include <atomic>
#include <cstdint>

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

class ASTAlterClusterQuery;
class ASTAlterShardQuery;

using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

/// Row for `system.shards` (SQL `CREATE SHARD` catalog).
struct SQLShardCatalogTableRow
{
    String name;
    std::vector<String> replica_collections;
    UInt32 weight = 0;
    bool internal_replication = false;
    std::vector<String> referenced_by_clusters;
};

/// Central registry for SQL `CREATE SHARD` / `CREATE CLUSTER` and config `remote_servers` name registration,
/// following the same split as `NamedCollectionFactory`: in-memory maps plus `ShardsMetadataStorage` /
/// `ClustersMetadataStorage` for on-disk `CREATE` text (`.sql` per entity).
///
/// Also owns the unified materialized-`ClusterPtr` registry (`clusters`): config `remote_servers`, SQL catalog,
/// and cluster discovery all publish their resolved `Cluster` objects here, so `Context::tryGetCluster` / `system.clusters`
/// see a single consistent container. Per-entry source is stored on each `Cluster` (see `Cluster::getDefinitionSource`).
class ClusterFactory : boost::noncopyable
{
public:
    static ClusterFactory & instance();

    ~ClusterFactory();

    void initialize(const String & data_path);
    void shutdown();

    void reloadFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, UInt64 remote_servers_definition_version);

    void reloadFromSQL();

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
    /// `ALTER SHARD name ADD REPLICA collection` — append named collection to replica list; returns false if `IF EXISTS` and shard missing.
    bool addReplicaToShardFromSQL(const ASTAlterShardQuery & query);
    /// `ALTER SHARD name DROP REPLICA collection` — remove from replica list (named collection is not dropped); returns false if `IF EXISTS` and shard missing.
    bool dropReplicaFromShardFromSQL(const ASTAlterShardQuery & query);
    /// `ALTER SHARD name REPLACE ... TO ...` — rename which named collections back replicas (pairwise, simultaneous); optional trailing shard `MODIFY PROPERTIES`; returns false if `IF EXISTS` and shard missing.
    bool replaceShardReplicasFromSQL(const ASTAlterShardQuery & query);

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

    bool hasShard(const String & name) const;
    bool hasCluster(const String & name) const;

    std::vector<String> listClusterNames() const;

    /// SQL `CREATE CLUSTER` definitions whose `members` contain `member_name` (SQL shard name or whole-shard named collection).
    std::vector<String> listSQLClustersContainingMember(const String & member_name) const;

    String getShowCreateShard(const String & name) const;
    String getShowCreateCluster(const String & name) const;

    ClusterPtr tryMaterializeCluster(const String & cluster_name, ContextPtr context) const;

    /// If `DROP NAMED COLLECTION` must be rejected because the name is still referenced in the in-memory SQL shard / cluster catalogs (`loaded_sql_shards`, `loaded_sql_clusters`), returns a short explanation; otherwise `nullopt`.
    std::optional<String> tryGetMessageIfNamedCollectionReferencedByClusterCatalog(const String & collection_name) const;

    std::vector<SQLShardCatalogTableRow> listShardsForSystemTable() const;

    /// Monotonic counter bumped on every mutation of the SQL catalog in-memory state (`loaded_sql_clusters` /
    /// `loaded_sql_shards`). Exposed so callers tagging materialized `Cluster` instances can read a consistent
    /// version without accessing internal state.
    UInt64 getSQLCatalogMutationCounter() const;

    /// Per-entry upsert for dynamic sources (primarily `ClusterDiscovery`). Refuses if an existing entry for
    /// `cluster_name` is owned by `RemoteServersConfig` and `source` isn't — i.e. config always wins on name collision.
    void setCluster(const String & cluster_name, ClusterPtr cluster, ClusterDefinitionSource source);

    /// Per-entry erase for dynamic sources. No-op if the currently-materialized entry belongs to a different source
    /// (prevents e.g. a Discovery `removeCluster` from evicting a config-defined cluster of the same name).
    void removeCluster(const String & cluster_name, ClusterDefinitionSource source);

    /// Generic remove: drops `cluster_name` regardless of source. Primarily used by the SQL catalog refresh path.
    void removeCluster(const String & cluster_name);

    /// Read-path accessors. Both read from the unified `clusters` registry; all sources (`<remote_servers>`, SQL
    /// catalog, discovery) keep it warm on every state transition, so no extra materialisation step is needed here.
    ClusterPtr tryGetCluster(const String & cluster_name) const;
    std::map<String, ClusterPtr> getClusters() const;

    /// Monotonic counter bumped on every `applyClustersConfig` / `reloadClustersConfig`. Consumers use it to detect
    /// remote_servers config changes (used by e.g. `ConnectionPoolWithFailover` rebuild paths).
    size_t getClustersVersion() const;

    /// Returns whether `cluster_name` is currently resolved from `<remote_servers>` config only (SQL catalog / discovery
    /// entries with the same name would return false). Invoked from DDL checks before creating a SQL `CREATE CLUSTER`.
    bool isClusterDefinedOnlyInRemoteServers(const String & cluster_name) const;

    /// Apply a new `<remote_servers>` config snapshot. Updates the in-memory clusters map (diffing old vs new config),
    /// bumps `clusters_version`, and re-materialises SQL catalog clusters so `system.clusters` stays in sync.
    /// `config_name` is the XML subtree key (typically `"remote_servers"`), forwarded as-is to `Clusters::updateClusters`.
    void applyClustersConfig(
        const ConfigurationPtr & config,
        const Settings & settings,
        MultiVersion<Macros>::Version macros,
        const String & config_name,
        ContextPtr context);

    /// Re-read the last-applied `clusters_config` (captured by `applyClustersConfig`) — used by the config reload loop
    /// when nothing else changed but we want to rebuild connection endpoints (macros / host resolution may differ).
    void reloadClustersConfig(ContextPtr context);

private:
    mutable std::mutex mutex;

    const LoggerPtr log = getLogger("ClusterFactory");

    bool initialized = false;
    std::atomic<bool> shutdown_called = false;

    std::unique_ptr<ShardsMetadataStorage> shards_metadata_storage;
    std::unique_ptr<ClustersMetadataStorage> clusters_metadata_storage;

    std::unordered_map<String, ShardCatalogDefinition> loaded_sql_shards;
    std::unordered_map<String, ClusterCatalogDefinition> loaded_sql_clusters;

    UInt64 sql_catalog_mutation_counter = 0;

    /// Unified materialized-`ClusterPtr` registry. Owns entries from all three upstreams — `<remote_servers>` config,
    /// SQL catalog DDL, and ClusterDiscovery — each tagged via `Cluster::getDefinitionSource`. Kept as `shared_ptr`
    /// so readers can snapshot cheaply without holding `clusters_mutex`.
    mutable std::mutex clusters_mutex;
    std::shared_ptr<Clusters> clusters TSA_GUARDED_BY(clusters_mutex);
    /// Last-applied `<remote_servers>` config: stored so `reloadClustersConfig` can re-run the diff without the caller
    /// re-fetching the config. Populated by `applyClustersConfig`; nullptr until the first call.
    ConfigurationPtr clusters_config TSA_GUARDED_BY(clusters_mutex);
    /// Monotonic version of `clusters_config`, returned by `getClustersVersion`. Read by connection-pool rebuild paths.
    size_t clusters_version TSA_GUARDED_BY(clusters_mutex) = 0;

    /// Returns whether `cluster_name`'s existing entry in `clusters` comes from `RemoteServersConfig`.
    /// Used by `setCluster(name, cluster, source)` to refuse SQL / Discovery overrides of config-defined names.
    bool isDefinedByRemoteServersConfigLocked(const String & cluster_name) const TSA_REQUIRES(clusters_mutex);

    /// Ensure `clusters` is constructed. Called from every read path so consumers never see a null map.
    /// Takes `clusters_mutex` internally; returns the `shared_ptr` atomically.
    std::shared_ptr<Clusters> ensureClustersLocked(const Settings & settings, MultiVersion<Macros>::Version macros) TSA_REQUIRES(clusters_mutex);

    /// Materialise SQL catalog clusters whose names are missing from `clusters`. Called from `applyClustersConfig`,
    /// `reloadClustersConfig`, and `getClusters` so `system.clusters` reflects the full catalog state at any time.
    void registerCatalogClustersLocked(ContextPtr context) TSA_REQUIRES(clusters_mutex);

    void reloadSQLDefinitionsLocked();

    /// Requires `mutex` locked. `m` must be an existing SQL shard name or a whole-shard named collection.
    void checkSQLClusterMemberNameLocked(const String & m) const;

    /// Re-materialise the SQL catalog cluster `cluster_name` and publish it into the unified `clusters`
    /// registry under the `SQLCatalog` source. Called at the tail of every catalog-mutating DDL
    /// (`CREATE CLUSTER`, `ALTER CLUSTER`, `ALTER SHARD` on a referenced shard) so the in-memory
    /// registry stays in sync on this node without waiting for the background refresh.
    /// MUST be called with `mutex` unlocked — `tryMaterializeCluster` and `setCluster(..., source)`
    /// re-acquire `mutex` / `clusters_mutex` internally, and `std::mutex` is non-recursive.
    void publishMaterializedSQLClusterAfterCatalogChange(const String & cluster_name);

    BackgroundSchedulePoolTaskHolder sql_catalog_update_task;
    void updateFunc();

    /// After `reloadSQLDefinitionsLocked`, push SQL catalog topology into the unified `clusters` registry so
    /// `system.clusters` / `tryGetCluster` match the Keeper-backed catalog on every node (not only the DDL initiator).
    /// Previously-present SQL clusters no longer in the catalog are evicted; config-defined names keep priority.
    void refreshSQLCatalogClusters(const std::unordered_set<String> & sql_clusters_before_reload);

    static void loadNamedCollectionsIfNeeded();
    static bool namedCollectionExists(const String & name);
    static NamedCollectionPtr getNamedCollection(const String & name);
};

}
