#pragma once

#include "config.h"

#if USE_LIBPQXX

#include <Storages/PostgreSQL/PostgreSQLReplicationHandler.h>

#include <Databases/DatabasesCommon.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Parsers/ASTCreateQuery.h>
#include <Databases/IDatabase.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseAtomic.h>

#include <atomic>


namespace DB
{

struct MaterializedPostgreSQLSettings;
class PostgreSQLConnection;
using PostgreSQLConnectionPtr = std::shared_ptr<PostgreSQLConnection>;


class DatabaseMaterializedPostgreSQL : public DatabaseAtomic
{

public:
    DatabaseMaterializedPostgreSQL(
        ContextPtr context_,
        const String & metadata_path_,
        UUID uuid_,
        bool is_attach_,
        const String & database_name_,
        const String & postgres_database_name,
        const postgres::ConnectionInfo & connection_info,
        std::unique_ptr<MaterializedPostgreSQLSettings> settings_);

    String getEngineName() const override { return "MaterializedPostgreSQL"; }

    String getMetadataPath() const override { return metadata_path; }

    LoadTaskPtr startupDatabaseAsync(AsyncLoader & async_loader, LoadJobSet startup_after, LoadingStrictnessLevel mode) override;
    void waitDatabaseStarted() const override;
    void stopLoading() override;

    DatabaseTablesIteratorPtr
    getTablesIterator(ContextPtr context, const DatabaseOnDisk::FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const override;

    /// The iterator above exposes the nested ReplacingMergeTree tables; reading them requires the
    /// `StorageMaterializedPostgreSQL` wrapper. See `IDatabase::getTableForRead`.
    StoragePtr getTableForRead(const String & table_name, const StoragePtr & table, ContextPtr local_context) const override;

    /// Fail closed for BACKUP DATABASE / BACKUP TABLE if a configured table has no nested ReplacingMergeTree yet,
    /// instead of silently omitting it from the backup (the base implementation only enumerates nested tables).
    std::vector<std::pair<ASTPtr, StoragePtr>>
    getTablesForBackup(const FilterByNameFunction & filter, const ContextPtr & local_context) const override;

    StoragePtr tryGetTable(const String & name, ContextPtr context) const override;

    void createTable(ContextPtr context, const String & table_name, const StoragePtr & table, const ASTPtr & query) override;

    void attachTable(ContextPtr context, const String & table_name, const StoragePtr & table, const String & relative_table_path) override;

    void detachTablePermanently(ContextPtr context, const String & table_name) override;

    StoragePtr detachTable(ContextPtr context, const String & table_name) override;

    void dropTable(ContextPtr local_context, const String & name, bool sync) override;

    void renameTable(
        ContextPtr local_context, const String & table_name, IDatabase & to_database,
        const String & to_table_name, bool exchange, bool dictionary) override;

    /// Fail-close for DROP DATABASE in coordinated mode: called before the generic DROP path removes any
    /// nested table, it aborts the drop if Keeper is unreachable, so a Keeper outage can never delete the last
    /// local copy of the data while the shared slot/publication/marker survive (a later recreate would then
    /// resume into empty tables). The last-replica teardown itself still happens in `drop`.
    void beforeDropDatabase(ContextPtr local_context) override;

    /// Reject TRUNCATE DATABASE / TRUNCATE ALL TABLES FROM db in coordinated mode. The generic database-wide
    /// truncate walks the nested Replicated/Shared tables through an internal context and drops/truncates each
    /// one directly, bypassing the per-table `StorageMaterializedPostgreSQL` guards, which would locally wipe the
    /// shared replicated data while the shared slot/publication/marker (and the live consumer) stay in place.
    void beforeTruncateDatabase(ContextPtr local_context) override;

    /// Recover a coordinated database when a DROP is refused after `beforeDropDatabase` had already stopped the
    /// replication handler: if the subsequent nested-table drop throws, re-arm the retrying startup path so the
    /// database resumes replicating instead of staying mounted but silently dead. Idempotent.
    void onDropDatabaseFailed(ContextPtr local_context) override;

    void drop(ContextPtr local_context) override;

    bool hasReplicationThread() const override { return true; }

    void stopReplication() override;

    void applySettingsChanges(const SettingsChanges & settings_changes, ContextPtr query_context) override;

    void shutdown() override;

    String getPostgreSQLDatabaseName() const { return remote_database_name; }

    /// True when the database uses Keeper-coordinated HA (materialized_postgresql_keeper_path is set):
    /// nested tables are Replicated/Shared and the replication slot is shared between replicas.
    bool isCoordinated() const;

protected:
    ASTPtr getCreateTableQueryImpl(const String & table_name, ContextPtr local_context, bool throw_on_error) const override;

private:
    void tryStartSynchronization();
    void startSynchronization();

    /// Construct a replication handler from the persisted settings without fetching tables or starting
    /// replication. Constructing it only resolves the coordination path / replica-name macros, which is enough to
    /// run the coordinated teardown even before the background startup task has built the live handler.
    std::shared_ptr<PostgreSQLReplicationHandler> makeReplicationHandler();

    /// Recovery shared by `beforeDropDatabase`'s catch and `onDropDatabaseFailed`: if the refused drop had
    /// already stopped the handler, discard it and clear `synchronization_started`; then re-arm the startup
    /// task so replication is rebuilt. Must be called under `handler_mutex`.
    void recoverAfterRefusedDrop(bool force_resnapshot = false);

    ASTPtr createAlterSettingsQuery(const SettingChange & new_setting);

    String getFormattedTablesList(const String & except = {}) const TSA_REQUIRES(tables_mutex);

    bool is_attach;
    String remote_database_name;
    postgres::ConnectionInfo connection_info;
    std::unique_ptr<MaterializedPostgreSQLSettings> settings;

    std::shared_ptr<PostgreSQLReplicationHandler> replication_handler;

    mutable std::mutex tables_mutex;

    /// Wrappers over the nested `ReplacingMergeTree` tables. `tables_mutex` is the only mutex that
    /// guards this map. Readers - `tryGetTable`, `getTableForRead`, `getTablesForBackup` -
    /// dereference the stored `StorageMaterializedPostgreSQL` pointers while holding it, so every
    /// writer must hold it as well. Guarding the writes with `handler_mutex` instead used to let
    /// `DROP DATABASE` destroy a wrapper while another thread was walking the map, which the
    /// sanitizers reported as a heap use-after-free. `ServerAsynchronousMetrics` enumerates the
    /// tables of every database once per second, so the window was hit routinely.
    /// When both mutexes are needed, `handler_mutex` is taken first.
    std::map<std::string, StoragePtr> materialized_tables TSA_GUARDED_BY(tables_mutex);

    /// Set after the generic `DROP DATABASE` path successfully removes a nested table. A refused drop
    /// needs a fresh snapshot only in this case; otherwise attach-style recovery preserves the still-live
    /// publication and nested tables.
    std::atomic_bool nested_table_removed_during_drop{false};

    /// Distinguishes the two states in which `materialized_tables` is empty. After `stopReplication`
    /// (server shutdown or `DROP DATABASE`) user-facing access legitimately falls back to the nested
    /// `ReplacingMergeTree` tables. But the map is also empty right after `CREATE` / `ATTACH DATABASE`
    /// and after a server restart, until `startSynchronization` publishes the wrappers; in that
    /// startup window user-facing reads must wrap the nested tables on the fly instead of exposing
    /// them directly (see `tryGetTable` and `getTableForRead`).
    bool replication_stopped TSA_GUARDED_BY(tables_mutex) = false;

    mutable std::mutex handler_mutex;
    /// Whether `startSynchronization` completed successfully (guarded by `handler_mutex`). Makes the startup
    /// task idempotent: it may be rescheduled after synchronization has already started (e.g. by a refused
    /// fail-close DROP DATABASE restoring the task it had deactivated), and replacing a live handler would
    /// leak its running consumer.
    bool synchronization_started = false;

    /// Set when a refused `DROP DATABASE` removed some of the nested tables of a database with a
    /// user-managed `materialized_postgresql_replication_slot`, for which they cannot be reloaded
    /// automatically (no snapshot can be exported from a slot this server does not manage, and the
    /// original `materialized_postgresql_snapshot` token may already be invalid). `startSynchronization`
    /// then refuses to resume replication instead of replicating the surviving subset of tables while
    /// the slot advances past the changes of the removed ones. Guarded by `handler_mutex`.
    bool manual_repair_required = false;

    BackgroundSchedulePoolTaskHolder startup_task;
    std::atomic<bool> shutdown_called = false;

    LoadTaskPtr startup_postgresql_database_task TSA_GUARDED_BY(mutex);
};

}

#endif
