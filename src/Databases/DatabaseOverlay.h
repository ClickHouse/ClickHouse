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
    DatabaseOverlay(const String & name_, ContextPtr context_);

    /// Not thread-safe. Use only as factory to initialize database
    DatabaseOverlay & registerNextDatabase(DatabasePtr database);

    String getEngineName() const override { return "Overlay"; }

    bool isTableExist(const String & table_name, ContextPtr context) const override;

    StoragePtr tryGetTable(const String & table_name, ContextPtr context) const override;

    void createTable(ContextPtr context, const String & table_name, const StoragePtr & table, const ASTPtr & query) override;

    void dropTable(ContextPtr context, const String & table_name, bool sync) override;

    void attachTable(ContextPtr context, const String & table_name, const StoragePtr & table, const String & relative_table_path) override;

    StoragePtr detachTable(ContextPtr context, const String & table_name) override;
    DatabaseDetachedTablesSnapshotIteratorPtr getDetachedTablesIterator(
        ContextPtr context, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const override;

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
    void checkTableNameLength(const String & table_name) const override;

protected:
    ASTPtr getCreateDatabaseQueryImpl() const override TSA_REQUIRES(mutex);

    std::vector<DatabasePtr> databases;
    LoggerPtr log;
};

/// `CREATE DATABASE db ENGINE = Overlay(db1, db2, ...)`: a read-only database that exposes the union of the tables of the
/// source databases, resolved by name on every access. A name is resolved in the first source that has it, and the table
/// is represented by a `StorageAlias` to it, which requires the grants on the source table in addition to the grants
/// on the overlay database, and combines the row policies of both.
class DatabaseOverlayReadOnly final : public IDatabase, protected WithContext
{
public:
    DatabaseOverlayReadOnly(const String & name_, Strings source_databases_, ContextPtr context_);

    String getEngineName() const override { return "Overlay"; }

    bool isTableExist(const String & table_name, ContextPtr context) const override;
    StoragePtr tryGetTable(const String & table_name, ContextPtr context) const override;
    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const override;
    ASTPtr getCreateTableQueryImpl(const String & table_name, ContextPtr context, bool throw_on_error) const override;

    bool shouldBeEmptyOnDetach() const override { return false; }
    bool empty() const override { return true; }
    bool isReadOnly() const override { return true; }
    void shutdown() override {}

    std::vector<std::pair<ASTPtr, StoragePtr>> getTablesForBackup(const FilterByNameFunction &, const ContextPtr &) const override { return {}; }

protected:
    ASTPtr getCreateDatabaseQueryImpl() const override TSA_REQUIRES(mutex);

private:
    /// The name of the first source database that has the table, or an empty string.
    String findSourceDatabase(const String & table_name, ContextPtr context) const;

    const Strings source_databases;
};

}
