#pragma once

#include <Storages/IStorage_fwd.h>
#include <Databases/IDatabase.h>

namespace DB
{

/**
 * Implements the IDatabase interface and combines multiple other databases
 * Searches for tables in each database in order until found, and delegates operations to the appropriate database
 * Useful for combining databases
 *
 * Used in clickhouse-local to combine DatabaseFileSystem and DatabaseMemory
 */
class DatabaseOverlay : public IDatabase, protected WithContext
{
public:
    DatabaseOverlay(const String & name_, ContextPtr context_, bool readonly_ = false);

    /// Register an underlying database directly. Used by `clickhouse-local`
    /// (non-readonly mode), where the underlying databases are owned by the
    /// `Overlay` itself and are not registered in `DatabaseCatalog`.
    /// Not thread-safe. Use only as factory to initialize database.
    DatabaseOverlay & registerNextDatabase(DatabasePtr database);

    /// Register an underlying database by name. Used by `registerDatabaseOverlay`
    /// (server-side, readonly mode). The database is resolved lazily via
    /// `DatabaseCatalog` on each operation, so the Overlay does not depend on
    /// startup ordering during `loadMetadata`. Missing sources are skipped at
    /// access time, allowing the Overlay to come up before its sources do.
    /// Not thread-safe. Use only as factory to initialize database.
    DatabaseOverlay & registerNextDatabaseByName(const String & source_name);

    String getEngineName() const override { return "Overlay"; }

    bool isTableExist(const String & table_name, ContextPtr context) const override;

    StoragePtr tryGetTable(const String & table_name, ContextPtr context) const override;

    void createTable(ContextPtr context, const String & table_name, const StoragePtr & table, const ASTPtr & query) override;

    void dropTable(ContextPtr context, const String & table_name, bool sync) override;

    void attachTable(ContextPtr context, const String & table_name, const StoragePtr & table, const String & relative_table_path) override;

    StoragePtr detachTable(ContextPtr context, const String & table_name) override;

    void renameTable(
        ContextPtr current_context,
        const String & name,
        IDatabase & to_database,
        const String & to_name,
        bool exchange,
        bool dictionary) override;

    ASTPtr getCreateTableQueryImpl(const String & name, ContextPtr context, bool throw_on_error) const override;

    String getTableDataPath(const String & table_name) const override;
    String getTableDataPath(const ASTCreateQuery & query) const override;

    UUID getUUID() const override;
    UUID tryGetTableUUID(const String & table_name) const override;

    void drop(ContextPtr context) override;

    void alterTable(ContextPtr local_context, const StorageID & table_id, const StorageInMemoryMetadata & metadata, bool validate_new_create_query) override;

    std::vector<std::pair<ASTPtr, StoragePtr>> getTablesForBackup(const FilterByNameFunction & filter, const ContextPtr & local_context) const override;

    void createTableRestoredFromBackup(const ASTPtr & create_table_query, ContextMutablePtr local_context, std::shared_ptr<IRestoreCoordination> restore_coordination, UInt64 timeout_ms) override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const override;

    bool empty() const override;

    void shutdown() override;

    /// Return false if at least one underlying database is not external, otherwise return true
    bool isExternal() const override;

    void loadStoredObjects(ContextMutablePtr local_context, LoadingStrictnessLevel mode) override;
    bool supportsLoadingInTopologicalOrder() const override;
    void beforeLoadingMetadata(ContextMutablePtr local_context, LoadingStrictnessLevel mode) override;
    void loadTablesMetadata(ContextPtr local_context, ParsedTablesMetadata & metadata, bool is_startup) override;
    void loadTableFromMetadata(
        ContextMutablePtr local_context,
        const String & file_path,
        const QualifiedTableName & name,
        const ASTPtr & ast,
        LoadingStrictnessLevel mode) override;
    LoadTaskPtr loadTableFromMetadataAsync(
        AsyncLoader & async_loader,
        LoadJobSet load_after,
        ContextMutablePtr local_context,
        const String & file_path,
        const QualifiedTableName & name,
        const ASTPtr & ast,
        LoadingStrictnessLevel mode) override;
    [[nodiscard]] LoadTaskPtr startupTableAsync(
        AsyncLoader & async_loader,
        LoadJobSet startup_after,
        const QualifiedTableName & name,
        LoadingStrictnessLevel mode) override;
    [[nodiscard]] LoadTaskPtr startupDatabaseAsync(
        AsyncLoader & async_loader,
        LoadJobSet startup_after,
        LoadingStrictnessLevel mode) override;
    void waitTableStarted(const String & name) const override;
    void waitDatabaseStarted() const override;
    void stopLoading() override;
    void checkMetadataFilenameAvailability(const String & table_name) const override;
    bool isReadOnly() const override;

    /// In read-only (facade) mode the underlying databases own the tables, so DROP DATABASE on the
    /// Overlay must not try to iterate and drop them via this facade.
    bool shouldBeEmptyOnDetach() const override { return !readonly; }

protected:
    ASTPtr getCreateDatabaseQueryImpl() const override TSA_REQUIRES(mutex);

    /// Returns the current snapshot of underlying databases, preserving
    /// registration order. In readonly mode, each call resolves source names via
    /// `DatabaseCatalog::tryGetDatabase` (lazy), so updates to the catalog become
    /// visible without re-registering the Overlay. Missing sources are skipped.
    /// In non-readonly mode (clickhouse-local), returns the directly-registered
    /// databases stored in `databases`.
    std::vector<DatabasePtr> resolveDatabases() const;

    /// Directly registered underlying databases (clickhouse-local non-readonly mode).
    /// Empty in readonly mode.
    std::vector<DatabasePtr> databases;

    /// Source database names for lazy resolution (server-side readonly mode).
    /// Empty in non-readonly mode.
    std::vector<String> source_names;

    LoggerPtr log;
    const bool readonly;
};

}
