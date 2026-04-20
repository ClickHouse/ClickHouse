#pragma once

#include <Common/Clusters/ClusterCatalogTypes.h>
#include <Common/Clusters/ClustersMetadataStorage.h>
#include <Common/Clusters/ShardsMetadataStorage.h>
#include <Common/MultiVersion.h>
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

    /// Immutable snapshot of the unified materialized-`ClusterPtr` registry together with the last-applied
    /// `<remote_servers>` config and its monotonic version. Publication is done via `MultiVersion<ClustersSnapshot>`:
    /// readers atomically acquire a `shared_ptr<const ClustersSnapshot>` and never take a `ClusterFactory`-level mutex;
    /// writers build a new snapshot (copy-on-write on the underlying `Clusters`) and publish it with `set()`.
    struct ClustersSnapshot
    {
        /// Owns entries from all three upstreams — `<remote_servers>` config, SQL catalog DDL, and `ClusterDiscovery`;
        /// each tagged via `Cluster::getDefinitionSource`. Immutable once the snapshot is published — the `const`
        /// in the element type makes every call through a reader snapshot reject `Clusters` mutators at compile time.
        std::shared_ptr<const Clusters> clusters;

        /// Last-applied `<remote_servers>` config: stored so `reloadClustersConfig` can re-run the diff without the
        /// caller re-fetching the config. `nullptr` until `applyClustersConfig` has been called at least once.
        ConfigurationPtr clusters_config;

        /// Monotonic version of the `<remote_servers>` portion of the snapshot, returned by `getClustersVersion`.
        size_t clusters_version = 0;

        /// Existence-only probe that also handles the pre-init state where `clusters` itself is null, saving
        /// call sites the double null check `snap && snap->clusters && snap->clusters->hasCluster(...)`.
        bool hasCluster(const String & cluster_name) const { return clusters && clusters->hasCluster(cluster_name); }
    };

    /// Lock-free read path for `tryGetCluster` / `getClusters` / `hasCluster` / `getClustersVersion` /
    /// `isClusterDefinedOnlyInRemoteServers`: each call does a single `MultiVersion::get()` to snapshot the current
    /// version, then operates on the immutable payload without synchronising with writers.
    MultiVersion<ClustersSnapshot> clusters_state;

    /// Serialises writers so one write sees a consistent "current" snapshot while building the next one. Readers
    /// never take this mutex. Safe to acquire without `mutex`; `applyClustersConfig` briefly takes `mutex` only to
    /// snapshot SQL catalog names before releasing it again (see `registerCatalogClustersInto`). There is no lock
    /// order between `clusters_writer_mutex` and `mutex` — they are never held simultaneously.
    mutable std::mutex clusters_writer_mutex;

    /// Atomically publishes `ClustersSnapshot` with the given components. Called by write paths while holding
    /// `clusters_writer_mutex`. Increments the `clusters_version` field only when caller opts in.
    void publishClustersSnapshotLocked(
        std::shared_ptr<Clusters> new_clusters,
        ConfigurationPtr new_config,
        size_t new_version) TSA_REQUIRES(clusters_writer_mutex);

    /// Materialise SQL catalog clusters whose names are missing from `builder`. Expects `clusters_writer_mutex`
    /// held so no one else is concurrently building; internally takes `mutex` briefly to snapshot the SQL catalog
    /// name list before releasing it (the per-name materialisation re-acquires `mutex` on its own).
    void registerCatalogClustersInto(Clusters & builder, ContextPtr context) const TSA_REQUIRES(clusters_writer_mutex);

    /// Returns a `Clusters` suitable as the starting point for a copy-on-write write. If the current snapshot has
    /// a non-null `Clusters`, it is deep-copied (shallow on the underlying `Cluster` pointers); otherwise a fresh
    /// empty one is constructed.
    std::shared_ptr<Clusters> cloneClustersForWriteLocked(
        const Settings & settings,
        MultiVersion<Macros>::Version macros,
        const std::shared_ptr<const ClustersSnapshot> & current) TSA_REQUIRES(clusters_writer_mutex);

    void reloadSQLDefinitionsLocked();

    /// Requires `mutex` locked. `m` must be an existing SQL shard name or a whole-shard named collection.
    void checkSQLClusterMemberNameLocked(const String & m) const;

    /// Re-materialise the SQL catalog cluster `cluster_name` and publish it into the unified `clusters`
    /// registry under the `SQLCatalog` source. Called at the tail of every catalog-mutating DDL
    /// (`CREATE CLUSTER`, `ALTER CLUSTER`, `ALTER SHARD` on a referenced shard) so the in-memory
    /// registry stays in sync on this node without waiting for the background refresh.
    /// MUST be called with `mutex` unlocked — `tryMaterializeCluster` and `setCluster(..., source)` re-acquire
    /// `mutex` / `clusters_writer_mutex` internally, and `std::mutex` is non-recursive.
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
