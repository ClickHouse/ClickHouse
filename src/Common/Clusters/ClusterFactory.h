#pragma once

#include <Common/MultiVersion.h>
#include <Common/logger_useful.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context_fwd.h>
#include <base/defines.h>
#include <Poco/AutoPtr.h>
#include <boost/noncopyable.hpp>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

/// Unified materialized-`ClusterPtr` registry: config `remote_servers`, SQL-managed cluster metadata,
/// and cluster discovery all publish resolved `Cluster` objects here, so `Context::tryGetCluster` /
/// `system.clusters` see a single consistent container. Per-entry source is stored on each `Cluster`
/// (see `Cluster::getDefinitionSource`).
///
/// SQL catalog definitions are owned by `ClusterMetadataManager`; this class only receives materialized
/// `ClusterPtr` instances via `replaceSQLCatalogClusters`.
class ClusterFactory : boost::noncopyable
{
public:
    static ClusterFactory & instance();

    ~ClusterFactory();

    void initialize();
    void shutdown();

    /// Replace SQL-catalog-sourced clusters in the unified registry. Names absent from `clusters` are removed
    /// unless they are owned by `<remote_servers>`.
    void replaceSQLCatalogClusters(const std::map<String, ClusterPtr> & clusters);

    bool hasCluster(const String & name) const;

    /// Per-entry upsert for dynamic sources (primarily `ClusterDiscovery`). Refuses if an existing entry for
    /// `cluster_name` is owned by `RemoteServersConfig` and `source` isn't — i.e. config always wins on name collision.
    void setCluster(const String & cluster_name, ClusterPtr cluster, ClusterDefinitionSource source);

    /// Per-entry erase for dynamic sources. No-op if the currently-materialized entry belongs to a different source
    /// (prevents e.g. a Discovery `removeCluster` from evicting a config-defined cluster of the same name).
    void removeCluster(const String & cluster_name, ClusterDefinitionSource source);

    /// Generic remove: drops `cluster_name` regardless of source.
    void removeCluster(const String & cluster_name);

    /// Read-path accessors. Both read from the unified `clusters` registry; all sources (`<remote_servers>`, SQL
    /// catalog, discovery) keep it warm on every state transition, so no extra materialisation step is needed here.
    ClusterPtr tryGetCluster(const String & cluster_name) const;
    std::map<String, ClusterPtr> getClusters() const;

    /// Monotonic counter bumped on every `applyClustersConfig` / `reloadClustersConfig`. Consumers use it to detect
    /// remote_servers config changes (used by e.g. `ConnectionPoolWithFailover` rebuild paths).
    size_t getClustersVersion() const;

    /// Returns whether `cluster_name` is currently resolved from `<remote_servers>` config only (SQL catalog / discovery
    /// entries with the same name would return false).
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
    const LoggerPtr log = getLogger("ClusterFactory");

    std::atomic<bool> shutdown_called = false;

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
    /// never take this mutex.
    mutable std::mutex clusters_writer_mutex;

    /// Atomically publishes `ClustersSnapshot` with the given components. Called by write paths while holding
    /// `clusters_writer_mutex`.
    void publishClustersSnapshotLocked(
        std::shared_ptr<Clusters> new_clusters,
        ConfigurationPtr new_config,
        size_t new_version) TSA_REQUIRES(clusters_writer_mutex);

    /// Copy already-materialized SQL-catalog clusters from the current registry into `builder` when rebuilding
    /// after a `<remote_servers>` config change.
    void registerCatalogClustersInto(Clusters & builder) const TSA_REQUIRES(clusters_writer_mutex);

    /// Returns a `Clusters` suitable as the starting point for a copy-on-write write. If the current snapshot has
    /// a non-null `Clusters`, it is deep-copied (shallow on the underlying `Cluster` pointers); otherwise a fresh
    /// empty one is constructed.
    std::shared_ptr<Clusters> cloneClustersForWriteLocked(
        const Settings & settings,
        MultiVersion<Macros>::Version macros,
        const std::shared_ptr<const ClustersSnapshot> & current) TSA_REQUIRES(clusters_writer_mutex);
};

}
