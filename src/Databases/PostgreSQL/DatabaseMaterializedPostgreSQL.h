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
        std::unique_ptr<MaterializedPostgreSQLSettings> settings_,
        ASTPtr table_overrides_);

    String getEngineName() const override { return "MaterializedPostgreSQL"; }

    String getMetadataPath() const override { return metadata_path; }

    LoadTaskPtr startupDatabaseAsync(AsyncLoader & async_loader, LoadJobSet startup_after, LoadingStrictnessLevel mode) override;
    void waitDatabaseStarted() const override;
    void stopLoading() override;

    DatabaseTablesIteratorPtr
    getTablesIterator(ContextPtr context, const DatabaseOnDisk::FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const override;

    /// Name-only listing (`SHOW TABLES`). In coordinated mode it hides the tables that user-facing lookups do not
    /// expose yet (see `isCoordinatedTableAvailable`), so a table appears in the listing only once this replica
    /// has caught up with the initial snapshot.
    std::vector<LightWeightTableDetails>
    getLightweightTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const override;

    /// Names for the "Maybe you meant ...?" hints. Hides the same tables as `getLightweightTablesIterator`.
    VectorWithMemoryTracking<String> getAllTableNames(ContextPtr context) const override;

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

    /// Refuses `DETACH TABLE ... PERMANENTLY` of the last replicated table (see `detachTablePermanently`). Called by
    /// `StorageMaterializedPostgreSQL::checkTableCanBeDetachedPermanently`, before the table is shut down. A table that
    /// passes the check is reserved for the current query (see `pending_permanent_detaches`).
    void checkTableCanBeDetachedPermanently(const String & table_name) const;

    StoragePtr detachTable(ContextPtr context, const String & table_name) override;

    void dropTable(ContextPtr local_context, const String & name, bool sync) override;

    void renameTable(
        ContextPtr local_context, const String & table_name, IDatabase & to_database,
        const String & to_table_name, bool exchange, bool dictionary) override;

    /// Refuses the rename of a coordinated database (see the implementation for the reasons).
    void renameDatabase(ContextPtr query_context, const String & new_name) override;

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

    /// True once `startSynchronization` has published the wrappers and started (or rejoined) replication,
    /// so the startup window of `StorageMaterializedPostgreSQL::checkTableCanBeDetachedPermanently` is
    /// over. Wrappers that are built outside `startSynchronization` - the per-query one of
    /// `DatabaseCatalog::getTableImpl` and the on-the-fly ones of `tryGetTable` / `getTableForRead` -
    /// must carry this state, otherwise the object `InterpreterDropQuery` actually sees would keep
    /// refusing the supported `DETACH TABLE ... PERMANENTLY` with the transient startup-window error.
    bool isReplicationReady() const { return synchronization_started.load(); }

    /// Whether a user-facing lookup may expose the table, by the same rules as `tryGetTable`: in coordinated mode a
    /// replicated table is available only once this replica has caught up with the initial snapshot (see
    /// `PostgreSQLReplicationHandler::markCaughtUpNestedTablesAvailable`), and no table is available in the startup
    /// window. Used by the lookups that resolve the nested table by UUID (`DatabaseCatalog::getTableImpl`) and
    /// wrap it themselves.
    bool isCoordinatedTableAvailable(const String & table_name) const;

    /// Whether user-facing lookups must consult `isCoordinatedTableAvailable` before exposing a table: always in
    /// coordinated mode, and while the database recovers from a partially applied `DROP DATABASE`
    /// (see `partial_drop_recovery_in_progress`).
    bool isTableVisibilityGated() const;

    /// Whether a user-facing lookup that resolves the nested table by UUID (`DatabaseCatalog::getTableImpl`) must not
    /// wrap it: the cases of `isTableVisibilityGated` / `isCoordinatedTableAvailable`, and also a published wrapper
    /// whose nested table does not hold a consistent snapshot - not loaded yet, or cleared for a reload (e.g. by the
    /// create-style startup that finishes the recovery from a partially applied `DROP DATABASE`) - which the
    /// name-based lookup hides as well.
    bool isTableHiddenFromUserAccess(const String & table_name) const;

    Exception makeCoordinatedTableNotAvailableException(const String & table_name) const;

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

    /// A `DROP DATABASE` that was refused after it had already removed some of the nested tables leaves the
    /// database in a state that only a create-style startup (or, for a user-managed replication slot, an
    /// operator) can repair. The startup that performs the repair is asynchronous and can be interrupted by
    /// a server restart, which rebuilds this object from its metadata and forgets `is_attach` /
    /// `manual_repair_required`, so the decision is also recorded as a marker file in the database metadata
    /// directory and re-applied on every startup until one of them succeeds. The compensating reload of a failed
    /// `DETACH TABLE ... PERMANENTLY` persists the same marker before it truncates the surviving nested table.
    String getPartialDropRecoveryMarkerPath() const;
    void persistPartialDropRecoveryMarker();
    void removePartialDropRecoveryMarker();
    /// Must be called under `handler_mutex`.
    void applyPartialDropRecoveryMode();

    ASTPtr createAlterSettingsQuery(const SettingsChanges & new_settings);

    String getFormattedTablesList(const String & except = {}) const TSA_REQUIRES(tables_mutex);

    /// Adds the change of `materialized_postgresql_unmaterialized_tables_list` to persist `unmaterialized_published_tables`
    /// (except `except`) together with `materialized_postgresql_tables_list`, if there is anything to persist or to clear.
    void addUnmaterializedTablesListChange(SettingsChanges & changes, const String & except) const TSA_REQUIRES(tables_mutex);

    /// The changes that restore `materialized_postgresql_tables_list` and `materialized_postgresql_unmaterialized_tables_list`
    /// captured before a failed `ATTACH TABLE` or `DETACH TABLE PERMANENTLY`.
    SettingsChanges createTablesListsRollback(const String & original_tables_list, const String & original_unmaterialized_tables_list) const;

    bool is_attach;
    /// `is_attach` as it was when the database was loaded: unlike `is_attach`, the partial-drop recovery does not
    /// reset it, because the database stays the same deployment with the same replication identity (see
    /// `PostgreSQLReplicationHandler::setAdoptLegacyReplicationIdentity`).
    const bool adopt_legacy_replication_identity;
    String remote_database_name;
    postgres::ConnectionInfo connection_info;
    std::unique_ptr<MaterializedPostgreSQLSettings> settings;
    /// The `TABLE OVERRIDE` list of the database definition (may be null). It shapes the DDL of the nested tables,
    /// so in coordinated mode it is part of the naming fingerprint all replicas must agree on.
    ASTPtr table_overrides;

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

    /// Tables that passed `checkTableCanBeDetachedPermanently` and whose `DETACH TABLE ... PERMANENTLY` is still in
    /// flight, with the query that detaches each of them. Concurrent detaches of different tables take different
    /// DDL guards, so without the reservation each of the last two tables could pass the last-table check, and both
    /// queries would shut their nested tables down before one of them is refused by `detachTablePermanently`. The
    /// refused table would stay mounted with its merges stopped for good. A reservation is removed when
    /// `detachTablePermanently` finishes, and a reservation of a query that ended before reaching it expires.
    mutable std::map<String, std::weak_ptr<QueryStatus>> pending_permanent_detaches TSA_GUARDED_BY(tables_mutex);

    /// Set after the generic `DROP DATABASE` path successfully removes a nested table. A refused drop
    /// needs a fresh snapshot only in this case; otherwise attach-style recovery preserves the still-live
    /// publication and nested tables.
    std::atomic_bool nested_table_removed_during_drop{false};

    /// Set when the current `DROP DATABASE` attempt has written the partial-drop recovery marker itself, before
    /// it removed the first nested table (see `dropTable`). A refused drop that turns out not to have removed any
    /// table removes the marker again in `onDropDatabaseFailed`; a marker left by an earlier attempt is kept.
    std::atomic_bool partial_drop_marker_written_by_drop{false};

    /// Distinguishes the two states in which `materialized_tables` is empty. After `stopReplication`
    /// (server shutdown or `DROP DATABASE`) user-facing access legitimately falls back to the nested
    /// `ReplacingMergeTree` tables. But the map is also empty right after `CREATE` / `ATTACH DATABASE`
    /// and after a server restart, until `startSynchronization` publishes the wrappers; in that
    /// startup window user-facing reads must wrap the nested tables on the fly instead of exposing
    /// them directly (see `tryGetTable` and `getTableForRead`).
    bool replication_stopped TSA_GUARDED_BY(tables_mutex) = false;

    /// A third state of an empty `materialized_tables`, besides the startup window and the stopped replication:
    /// a refused `DROP DATABASE` has removed some of the nested tables. The surviving ones are stale - a
    /// create-style startup clears and reloads them, and with a user-managed replication slot they stay stale
    /// until the operator repairs the database - so user-facing lookups (`tryGetTable`, `getTableForRead`,
    /// `SHOW TABLES`, the UUID lookup in `DatabaseCatalog`) fail closed instead of wrapping them on the fly.
    /// Set by `applyPartialDropRecoveryMode` (and from the persisted marker on construction), cleared once
    /// `startSynchronization` publishes a consistent set of wrappers.
    bool partial_drop_recovery_in_progress TSA_GUARDED_BY(tables_mutex) = false;

    /// Serializes the whole `ATTACH TABLE` / `DETACH TABLE ... PERMANENTLY` transactions of this database. Their
    /// rollbacks restore the persisted `materialized_postgresql_tables_list` captured at the start, which would
    /// otherwise overwrite a concurrent change of the membership of another table. Taken before `handler_mutex`
    /// and `tables_mutex`.
    std::mutex table_membership_mutex;

    /// The tables of a whole-schema database that are in the publication but are not replicated, because they
    /// do not have nested tables (for example, their initial snapshot has failed). They are not in
    /// `materialized_tables`, but `getTablesForBackup` must not silently omit them.
    std::set<String> unmaterialized_published_tables TSA_GUARDED_BY(tables_mutex);

    mutable std::mutex handler_mutex;
    /// Whether `startSynchronization` completed successfully (guarded by `handler_mutex`). Makes the startup
    /// task idempotent: it may be rescheduled after synchronization has already started (e.g. by a refused
    /// fail-close DROP DATABASE restoring the task it had deactivated), and replacing a live handler would
    /// leak its running consumer.
    /// Atomic so that `isReplicationReady` can be read without `handler_mutex` (see its comment); every
    /// write still happens under `handler_mutex`.
    std::atomic_bool synchronization_started = false;

    /// Set when a refused `DROP DATABASE` removed some of the nested tables of a database with a
    /// user-managed `materialized_postgresql_replication_slot`, for which they cannot be reloaded
    /// automatically (no snapshot can be exported from a slot this server does not manage, and the
    /// original `materialized_postgresql_snapshot` token may already be invalid). `startSynchronization`
    /// then refuses to resume replication instead of replicating the surviving subset of tables while
    /// the slot advances past the changes of the removed ones. Guarded by `handler_mutex`.
    bool manual_repair_required = false;

    /// Set while the partial-drop recovery marker is still on disk after replication has been started
    /// again. Removing the marker is the last step of the recovery, and it can fail on its own (a
    /// transient metadata-disk error), so the failure must not be swallowed: it propagates to
    /// `tryStartSynchronization`, which reschedules the startup task, and the rescheduled run retries
    /// only the removal (replication is already running). Otherwise a leftover marker would silently
    /// force a create-style startup - a full resnapshot - after every subsequent server restart.
    /// Guarded by `handler_mutex`.
    bool partial_drop_recovery_marker_removal_pending = false;

    BackgroundSchedulePoolTaskHolder startup_task;
    std::atomic<bool> shutdown_called = false;

    /// Set by `beforeDropDatabase` and cleared by `onDropDatabaseFailed`. `DatabaseCatalog::detachDatabase`
    /// calls `shutdown` before `drop`, and if `drop` then throws (e.g. the coordinated teardown in
    /// `shutdownFinal` cannot reach Keeper), the catalog reattaches this very object. `shutdown_by_drop`
    /// records that `shutdown_called` was set by such a drop rather than by the server shutdown, so that
    /// `onDropDatabaseFailed` can undo it and rebuild replication instead of leaving the database empty.
    std::atomic_bool drop_in_progress{false};
    std::atomic_bool shutdown_by_drop{false};

    LoadTaskPtr startup_postgresql_database_task TSA_GUARDED_BY(mutex);
};

}

#endif
