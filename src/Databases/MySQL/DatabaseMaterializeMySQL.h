#pragma once

#include "config_core.h"

#if USE_MYSQL

#include <mysqlxx/Pool.h>
#include <Core/MySQL/MySQLClient.h>
#include <Databases/IDatabase.h>
#include <Databases/MySQL/MaterializeMySQLSettings.h>
#include <Databases/MySQL/MaterializeMySQLSyncThread.h>

namespace DB
{

/** Real-time pull table structure and data from remote MySQL
 *
 *  All table structure and data will be written to the local file system
 */
class DatabaseMaterializeMySQL : public IDatabase
{
public:
    DatabaseMaterializeMySQL(
        const Context & context, const String & database_name_, const String & metadata_path_,
        const IAST * database_engine_define_, const String & mysql_database_name_, mysqlxx::Pool && pool_,
        MySQLClient && client_, std::unique_ptr<MaterializeMySQLSettings> settings_);

    void rethrowExceptionIfNeed() const;

    void setException(const std::exception_ptr & exception);
protected:
    const Context & global_context;

    ASTPtr engine_define;
    DatabasePtr nested_database;
    std::unique_ptr<MaterializeMySQLSettings> settings;

    Poco::Logger * log;
    MaterializeMySQLSyncThread materialize_thread;

    std::exception_ptr exception;

public:
    String getEngineName() const override { return "MaterializeMySQL"; }

    ASTPtr getCreateDatabaseQuery() const override;

    void loadStoredObjects(Context & context, bool has_force_restore_data_flag, bool force_attach) override;

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

    void renameTable(const Context & context, const String & name, IDatabase & to_database, const String & to_name, bool exchange, bool dictionary) override;

    void alterTable(const Context & context, const StorageID & table_id, const StorageInMemoryMetadata & metadata) override;

    time_t getObjectMetadataModificationTime(const String & name) const override;

    String getMetadataPath() const override;

    String getObjectMetadataPath(const String & table_name) const override;

    bool shouldBeEmptyOnDetach() const override;

    void drop(const Context & context) override;

    bool isTableExist(const String & name, const Context & context) const override;

    StoragePtr tryGetTable(const String & name, const Context & context) const override;

    DatabaseTablesIteratorPtr getTablesIterator(const Context & context, const FilterByNameFunction & filter_by_table_name) override;
};

}

#endif
