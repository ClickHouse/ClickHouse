#pragma once

#include <Databases/DatabasesCommon.h>
#include <Interpreters/Cluster.h>
#include <Parsers/ASTCreateQuery.h>


namespace DB
{

class ColumnsDescription;
class Context;

/** A database engine that provides real-time access to the tables of a database on a remote
  * ClickHouse server over the native TCP protocol. It is the ClickHouse-to-ClickHouse counterpart
  * of the `MySQL` and `PostgreSQL` database engines.
  *
  * The list of tables and their structure are fetched from the remote server on demand (via
  * `system.tables` and `DESC TABLE`), so the database always reflects the current state of the
  * remote server. Each table is exposed as a `Distributed` storage over an ad-hoc cluster built
  * from the supplied addresses (the same machinery as the `remote`/`remoteSecure` table functions
  * and the `Remote`/`RemoteSecure` table engines), which forwards `SELECT` and `INSERT` queries to
  * the remote server.
  *
  * `Remote` and `RemoteSecure` differ only in whether the connection uses a plain or a TLS TCP port
  * by default. Both are handy for federating several ClickHouse clusters or for plugging a larger
  * ClickHouse cluster into `clickhouse-local` or a smaller cluster.
  *
  * The class also serves as the base of the `Cluster` database engine (see `DatabaseCluster`), which
  * differs only in where the cluster comes from (the server configuration instead of the engine
  * arguments) and in the table engine that `SHOW CREATE TABLE` prints.
  */
class DatabaseRemote : public DatabaseWithAltersOnDiskBase, protected WithContext
{
public:
    DatabaseRemote(
        ContextPtr context_,
        const String & metadata_path_,
        const ASTStorage * database_engine_define_,
        const String & database_name_,
        const String & remote_database_,
        const String & username_,
        const String & password_,
        ClusterPtr cluster_,
        ClusterPtr remote_only_cluster_,
        bool secure_,
        UUID uuid);

    String getEngineName() const override { return secure ? "RemoteSecure" : "Remote"; }
    UUID getUUID() const override { return db_uuid; }

    bool isRemoteDatabase() const override { return true; }

    String getMetadataPath() const override { return metadata_path; }

    /// The table list lives on the remote server, so this database is never required to be empty
    /// before it is detached or dropped.
    bool shouldBeEmptyOnDetach() const override { return false; }

    bool empty() const override;

    void loadStoredObjects(ContextMutablePtr, LoadingStrictnessLevel /*mode*/) override {}

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const override;

    /// The `system.tables` path: unlike the plain iterator, it keeps a table whose structure could not
    /// be fetched, with a null storage object (`StorageSystemTables` null-guards every column that
    /// needs it), so the row does not silently vanish from `system.tables`. It is also the path of the
    /// explicit `SHOW TABLES`, so a remote failure is propagated instead of being reported as an empty
    /// list of tables.
    DatabaseTablesIteratorPtr getTablesIteratorWithHint(
        ContextPtr context,
        const FilterByNameFunction & filter_by_table_name,
        bool skip_not_loaded,
        const TablesFilter & tables_filter) const override;

    /// Drives `SHOW TABLES` directly from `fetchTablesList`. The default implementation goes through
    /// `getTablesIterator`, which drops every table whose structure could not be fetched (e.g. a remote
    /// user with `SHOW TABLES` but no `SHOW COLUMNS` on one table, or a transient `DESC TABLE` failure),
    /// even though the remote `system.tables` query has already returned the name. A remote failure is
    /// propagated: `SHOW TABLES` is a user-facing listing, and answering it with an empty result would
    /// contradict `EXISTS TABLE` / `SELECT` on the same database, which report the real error.
    std::vector<LightWeightTableDetails>
    getLightweightTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const override;

    /// The default implementation infers the structure of every table through `getTablesIterator`,
    /// but only the names are needed, e.g. by the name hints for a missing table. Best-effort: on a
    /// remote error, returns an empty list instead of throwing.
    VectorWithMemoryTracking<String> getAllTableNames(ContextPtr context) const override;

    /// Answers from the table names only, without resolving the structure, so a table that exists
    /// but cannot be described still reports `1`. A transport/authentication failure is propagated
    /// as the real remote error instead of being reported as "does not exist".
    bool isTableExist(const String & name, ContextPtr context) const override;

    /// Returns `nullptr` only for a genuinely missing table; a transport/authentication failure is
    /// propagated as the real remote error, so user queries do not misreport it as `UNKNOWN_TABLE`.
    StoragePtr tryGetTable(const String & name, ContextPtr context) const override;

    /// The engine is a read-through view of the remote server; it does not manage the remote schema.
    void createTable(ContextPtr, const String & table_name, const StoragePtr & storage, const ASTPtr & create_query) override;
    void dropTable(ContextPtr, const String & table_name, bool sync) override;
    void attachTable(ContextPtr context, const String & table_name, const StoragePtr & storage, const String & relative_table_path) override;
    StoragePtr detachTable(ContextPtr context, const String & table_name) override;

    void drop(ContextPtr /*context*/) override;
    void shutdown() override {}

    std::vector<std::pair<ASTPtr, StoragePtr>> getTablesForBackup(const FilterByNameFunction &, const ContextPtr &) const override { return {}; }

protected:
    ASTPtr getCreateDatabaseQueryImpl() const override TSA_REQUIRES(mutex);
    ASTPtr getCreateTableQueryImpl(const String & table_name, ContextPtr context, bool throw_on_error) const override;

    /// One consistent snapshot of the clusters a metadata operation works with: the cluster with
    /// every configured replica, and its metadata-lookup fallback with the replicas that point to
    /// this server stripped from their shards, while every other shard stays intact (see
    /// `Cluster::tryGetClusterWithoutLocalReplicas` for when the fallback is null). The `Remote`
    /// engine returns the fixed clusters built from the engine arguments; the `Cluster` engine
    /// resolves the named cluster from the current server configuration, so its databases follow
    /// configuration reloads.
    struct ProxyClusters
    {
        ClusterPtr cluster;
        ClusterPtr remote_only_cluster;
    };
    virtual ProxyClusters getProxyClusters() const { return {cluster, remote_only_cluster}; }

    /// Build a `Distributed` storage that forwards to `remote_database.table_name` on the remote
    /// server, inferring the column structure from it. Returns `nullptr` if the table genuinely
    /// does not exist. When `throw_on_error` is set, a transport/authentication failure is
    /// propagated instead of being reported as a missing table; when it is not set (the best-effort
    /// path used by `tryGetTable`/`system.tables`), any failure returns `nullptr`.
    StoragePtr fetchTable(const String & table_name, ContextPtr local_context, bool throw_on_error = false) const;

    const ASTPtr database_engine_define;
    const String remote_database;
    LoggerPtr log;

private:
    const String metadata_path;
    /// The stored credentials of the engine, kept for `SHOW CREATE TABLE`: when the addresses are
    /// given as a named collection but the live proxy is bound to the fallback cluster, the emitted
    /// definition switches to the positional form (see `getCreateTableQueryImpl`), which has to
    /// carry them explicitly.
    const String username;
    const String password;
    /// The fixed clusters of the `Remote` engine, unused by the `Cluster` engine, whose clusters are
    /// dynamic; every metadata operation takes its snapshot through `getProxyClusters` instead.
    const ClusterPtr cluster;
    const ClusterPtr remote_only_cluster;
    const bool secure;
    bool persistent = true;
    const UUID db_uuid;

    /// Shared implementation of `getTablesIterator` / `getTablesIteratorWithHint`;
    /// `keep_unresolved_tables` controls whether a table whose structure could not be fetched is kept
    /// with a null storage object or dropped, and `throw_on_error` whether a failure of the remote
    /// listing is propagated or turned into an empty result.
    DatabaseTablesIteratorPtr getTablesIteratorImpl(
        ContextPtr context, const FilterByNameFunction & filter_by_table_name, bool keep_unresolved_tables, bool throw_on_error) const;

    /// Resolve `remote_database` as a database of this server when the cluster has a local shard,
    /// rejecting a database that refers to itself.
    DatabasePtr tryGetLocalDatabase() const;

    /// Fetch the names of the tables of `remote_database` from the remote server. When `only_table`
    /// is set, fetches only that name (the cheap existence check of `isTableExist`, which must not
    /// resolve the structure of the table). The list comes from an arbitrary shard (a local one is
    /// preferred, another shard is consulted only when it is unavailable), like the structure does in
    /// `getStructureOfRemoteTable`: the shards of a cluster normally serve the same set of tables, and
    /// asking every one of them would multiply the cost of every listing by the number of shards.
    /// `ignore_visibility` (meaningful only together with `only_table`) answers from the local shard
    /// regardless of whether the caller is allowed to see the table; see
    /// `isTableExistIgnoringVisibility`.
    Strings fetchTablesList(ContextPtr local_context, const String * only_table = nullptr, bool ignore_visibility = false) const;

    /// Whether `remote_database.table_name` exists at all, ignoring whether the caller is allowed to
    /// see it. Used by an outer `Remote` database whose local shard is this database: `isTableExist`
    /// and `tryGetTable` answer "missing" both for a hidden and for a genuinely missing table, but
    /// only the latter may be looked up on the other replicas of the shard — serving a hidden one
    /// under the stored engine credentials would bypass the visibility rule of this database.
    bool isTableExistIgnoringVisibility(const String & table_name, ContextPtr local_context) const;

    /// Infer the column structure of `remote_database.table_name`, from the local catalog for a local
    /// shard (without the name-hint machinery of `DatabaseCatalog::getTable`, which would recurse back
    /// into this database) and via `DESC TABLE` on the remote server otherwise. When the local replica
    /// does not have the database or the table, falls back to the remote replicas, like the
    /// `Distributed` read path does. Like the listing, the structure is inferred from an arbitrary
    /// shard. Returns an empty set when the table does not exist, and sets
    /// `table_cluster` to the cluster through which the table should be accessed (the remote-only
    /// fallback cluster when the structure came from the fallback, the full cluster otherwise).
    ColumnsDescription fetchTableStructure(const String & table_name, ContextPtr local_context, ClusterPtr & table_cluster) const;
};

}
