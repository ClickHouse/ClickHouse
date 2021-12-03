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
template<typename Base>
class DatabaseMaterializeMySQL : public Base
{
public:

    DatabaseMaterializeMySQL(
        const Context & context, const String & database_name_, const String & metadata_path_, UUID uuid,
        const String & mysql_database_name_, mysqlxx::Pool && pool_,
        MySQLClient && client_, std::unique_ptr<MaterializeMySQLSettings> settings_);

    void rethrowExceptionIfNeed() const;

    void setException(const std::exception_ptr & exception);
protected:

    std::unique_ptr<MaterializeMySQLSettings> settings;

    MaterializeMySQLSyncThread materialize_thread;

    std::exception_ptr exception;

    std::atomic_bool started_up{false};

public:
    String getEngineName() const override { return "MaterializeMySQL"; }

    void loadStoredObjects(Context & context, bool has_force_restore_data_flag, bool force_attach) override;

    void createTable(const Context & context, const String & name, const StoragePtr & table, const ASTPtr & query) override;

    void dropTable(const Context & context, const String & name, bool no_delay) override;

    void attachTable(const String & name, const StoragePtr & table, const String & relative_table_path) override;

    StoragePtr detachTable(const String & name) override;

    void renameTable(const Context & context, const String & name, IDatabase & to_database, const String & to_name, bool exchange, bool dictionary) override;

    void alterTable(const Context & context, const StorageID & table_id, const StorageInMemoryMetadata & metadata) override;

    void drop(const Context & context) override;

    StoragePtr tryGetTable(const String & name, const Context & context) const override;

    DatabaseTablesIteratorPtr getTablesIterator(const Context & context, const DatabaseOnDisk::FilterByNameFunction & filter_by_table_name) override;

    void assertCalledFromSyncThreadOrDrop(const char * method) const;

    void shutdownSynchronizationThread();
};


void setSynchronizationThreadException(const DatabasePtr & materialize_mysql_db, const std::exception_ptr & exception);
void stopDatabaseSynchronization(const DatabasePtr & materialize_mysql_db);
void rethrowSyncExceptionIfNeed(const IDatabase * materialize_mysql_db);

}

#endif
