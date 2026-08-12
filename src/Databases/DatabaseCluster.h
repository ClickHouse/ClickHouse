#pragma once

#include <Databases/DatabaseRemote.h>

#include <mutex>


namespace DB
{

/** A database engine that provides real-time access to the tables of a database on a cluster from
  * the server configuration. It is the named-cluster counterpart of the `Remote` database engine,
  * exactly as the `cluster` table function relates to the `remote` table function.
  *
  * The whole metadata and query machinery is shared with `DatabaseRemote`; the differences are:
  * - the cluster is resolved from the server configuration by name on every access, so the database
  *   follows configuration reloads (including cluster auto-discovery), like a `Distributed` table;
  * - connections use the per-replica settings of the cluster configuration (credentials, secure
  *   connections, compression, the inter-server secret) instead of credentials of its own, so the
  *   engine takes no credential arguments and stores no secrets;
  * - `SHOW CREATE TABLE` prints a `Distributed(cluster_name, database, table)` definition, which is
  *   the persistent table-engine counterpart of a named cluster.
  */
class DatabaseCluster final : public DatabaseRemote
{
public:
    DatabaseCluster(
        ContextPtr context_,
        const String & metadata_path_,
        const ASTStorage * database_engine_define_,
        const String & database_name_,
        const String & cluster_name_,
        const String & remote_database_,
        UUID uuid);

    String getEngineName() const override { return "Cluster"; }

protected:
    ASTPtr getCreateTableQueryImpl(const String & table_name, ContextPtr context, bool throw_on_error) const override;

    /// Resolves the named cluster from the current server configuration, so a configuration reload
    /// (or cluster auto-discovery) is picked up by the next operation. Throws when the cluster has
    /// disappeared from the configuration; the best-effort listing paths of `DatabaseRemote` turn
    /// that into an empty result, exactly like an unreachable server.
    ProxyClusters getProxyClusters() const override;

private:
    /// As written in the engine arguments; macros (e.g. `{cluster}`) are expanded on every
    /// resolution, like `StorageDistributed` does with its cluster name argument.
    const String cluster_name;

    /// Deriving the remote-only fallback builds new connection pools, so it is cached and rebuilt
    /// only when the configuration produces a new cluster object.
    mutable std::mutex cluster_cache_mutex;
    mutable ClusterPtr cached_cluster TSA_GUARDED_BY(cluster_cache_mutex);
    mutable ClusterPtr cached_remote_only_cluster TSA_GUARDED_BY(cluster_cache_mutex);
};

}
