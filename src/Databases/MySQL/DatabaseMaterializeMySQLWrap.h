#pragma once

#include <Databases/IDatabase.h>

namespace DB
{

class DatabaseMaterializeMySQLWrap : public IDatabase
{
public:
    ASTPtr getCreateDatabaseQuery() const override;

    void loadStoredObjects(Context & context, bool has_force_restore_data_flag) override;

    DatabaseMaterializeMySQLWrap(const DatabasePtr & nested_database_, const ASTPtr & database_engine_define_, const String & log_name);
protected:
    DatabasePtr nested_database;
    ASTPtr database_engine_define;
    Poco::Logger * log;

    mutable std::mutex mutex;
    std::exception_ptr exception;

    DatabasePtr getNestedDatabase() const;

    void setException(const std::exception_ptr & exception);

public:
    void shutdown() override;

    bool empty() const override;

    String getDataPath() const override;

    String getTableDataPath(const String & table_name) const override;

    String getTableDataPath(const ASTCreateQuery & query) const override;

    UUID tryGetTableUUID(const String & table_name) const override;

    void createTable(const Context & context, const String & name, const StoragePtr & table, const ASTPtr & query) override;

    void dropTable(const Context & context, const String & name, bool no_delay) override;

    void attachTable(const String & name, const StoragePtr & table, const String & relative_table_path) override;

    StoragePtr detachTable(const String & name) override;

    void renameTable(const Context & context, const String & name, IDatabase & to_database, const String & to_name, bool exchange) override;

    void alterTable(const Context & context, const StorageID & table_id, const StorageInMemoryMetadata & metadata) override;

    time_t getObjectMetadataModificationTime(const String & name) const override;

    String getMetadataPath() const override;

    String getObjectMetadataPath(const String & table_name) const override;

    bool shouldBeEmptyOnDetach() const override;

    void drop(const Context & context) override;

    bool isTableExist(const String & name) const override;

    StoragePtr tryGetTable(const String & name) const override;

    DatabaseTablesIteratorPtr getTablesIterator(const FilterByNameFunction & filter_by_table_name) override;
};

}
