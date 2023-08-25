#pragma once

#include "config_core.h"

#if USE_MYSQL

#include <mysqlxx/Pool.h>
#include <Core/MySQL/MySQLClient.h>
#include <base/UUID.h>
#include <Databases/IDatabase.h>
#include <Databases/DatabaseAtomic.h>
#include <Databases/MySQL/MaterializedMySQLSettings.h>
#include <Databases/MySQL/MaterializedMySQLSyncThread.h>

namespace DB
{

/** Real-time pull table structure and data from remote MySQL
 *
 *  All table structure and data will be written to the local file system
 */
class DatabaseMaterializedMySQL : public DatabaseAtomic
{
public:
    DatabaseMaterializedMySQL(
        ContextPtr context,
        const String & database_name_,
        const String & metadata_path_,
        UUID uuid,
        const String & mysql_database_name_,
        mysqlxx::Pool && pool_,
        MySQLClient && client_,
        std::unique_ptr<MaterializedMySQLSettings> settings_);

    void rethrowExceptionIfNeeded() const;

    void setException(const std::exception_ptr & exception);
protected:

    std::unique_ptr<MaterializedMySQLSettings> settings;

    MaterializedMySQLSyncThread materialize_thread;

    std::exception_ptr exception;

    std::atomic_bool started_up{false};

public:
    String getEngineName() const override { return "MaterializedMySQL"; }

    void startupTables(ThreadPool & thread_pool, LoadingStrictnessLevel mode) override;

    void createTable(ContextPtr context_, const String & name, const StoragePtr & table, const ASTPtr & query) override;

    void dropTable(ContextPtr context_, const String & name, bool sync) override;

    void attachTable(ContextPtr context_, const String & name, const StoragePtr & table, const String & relative_table_path) override;

    StoragePtr detachTable(ContextPtr context_, const String & name) override;

    void renameTable(ContextPtr context_, const String & name, IDatabase & to_database, const String & to_name, bool exchange, bool dictionary) override;

    void alterTable(ContextPtr context_, const StorageID & table_id, const StorageInMemoryMetadata & metadata) override;

    void drop(ContextPtr context_) override;

    StoragePtr tryGetTable(const String & name, ContextPtr context_) const override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context_, const DatabaseOnDisk::FilterByNameFunction & filter_by_table_name) const override;

    void checkIsInternalQuery(ContextPtr context_, const char * method) const;

    bool hasReplicationThread() const override { return true; }

    void stopReplication() override;

    friend class DatabaseMaterializedTablesIterator;
};

}

#endif
