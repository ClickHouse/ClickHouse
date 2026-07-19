#pragma once

#include <Databases/DatabasesCommon.h>
#include <Interpreters/Cluster.h>
#include <Parsers/ASTCreateQuery.h>


namespace DB
{

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
  */
class DatabaseRemote final : public DatabaseWithAltersOnDiskBase, WithContext
{
public:
    DatabaseRemote(
        ContextPtr context_,
        const String & metadata_path_,
        const ASTStorage * database_engine_define_,
        const String & database_name_,
        const String & remote_database_,
        ClusterPtr cluster_,
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

    bool isTableExist(const String & name, ContextPtr context) const override;
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

private:
    const String metadata_path;
    const ASTPtr database_engine_define;
    const String remote_database;
    const ClusterPtr cluster;
    const bool secure;
    LoggerPtr log;
    bool persistent = true;
    const UUID db_uuid;

    /// Fetch the names of the tables of `remote_database` from the remote server.
    Strings fetchTablesList(ContextPtr local_context) const;

    /// Build a `Distributed` storage that forwards to `remote_database.table_name` on the remote
    /// server, inferring the column structure from it. Returns `nullptr` if the table genuinely
    /// does not exist. When `throw_on_error` is set, a transport/authentication failure is
    /// propagated instead of being reported as a missing table; when it is not set (the best-effort
    /// path used by `tryGetTable`/`system.tables`), any failure returns `nullptr`.
    StoragePtr fetchTable(const String & table_name, ContextPtr local_context, bool throw_on_error = false) const;
};

}
