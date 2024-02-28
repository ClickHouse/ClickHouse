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
class DatabasesOverlay : public IDatabase, protected WithContext
{
public:
    DatabasesOverlay(const String & name_, ContextPtr context_);

    /// Not thread-safe. Use only as factory to initialize database
    DatabasesOverlay & registerNextDatabase(DatabasePtr database);

    String getEngineName() const override { return "Overlay"; }

public:
    bool isTableExist(const String & table_name, ContextPtr context) const override;

    StoragePtr tryGetTable(const String & table_name, ContextPtr context) const override;

    void createTable(ContextPtr context, const String & table_name, const StoragePtr & table, const ASTPtr & query) override;

    void dropTable(ContextPtr context, const String & table_name, bool sync) override;

    void attachTable(ContextPtr context, const String & table_name, const StoragePtr & table, const String & relative_table_path) override;

    StoragePtr detachTable(ContextPtr context, const String & table_name) override;

    ASTPtr getCreateTableQueryImpl(const String & name, ContextPtr context, bool throw_on_error) const override;
    ASTPtr getCreateDatabaseQuery() const override;

    String getTableDataPath(const String & table_name) const override;
    String getTableDataPath(const ASTCreateQuery & query) const override;

    UUID tryGetTableUUID(const String & table_name) const override;

    void drop(ContextPtr context) override;

    void alterTable(ContextPtr local_context, const StorageID & table_id, const StorageInMemoryMetadata & metadata) override;

    std::vector<std::pair<ASTPtr, StoragePtr>> getTablesForBackup(const FilterByNameFunction & filter, const ContextPtr & local_context) const override;

    void createTableRestoredFromBackup(const ASTPtr & create_table_query, ContextMutablePtr local_context, std::shared_ptr<IRestoreCoordination> restore_coordination, UInt64 timeout_ms) override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name) const override;

    bool empty() const override;

    void shutdown() override;

protected:
    std::vector<DatabasePtr> databases;
    LoggerPtr log;
};

}
