#pragma once

#include "config_core.h"

#if USE_LIBPQXX

#include <Databases/DatabasesCommon.h>
#include <Core/BackgroundSchedulePool.h>
#include <Parsers/ASTCreateQuery.h>
#include <Core/PostgreSQL/PoolWithFailover.h>
#include <Storages/ExternalDataSourceConfiguration.h>

namespace DB
{

class Context;


/** Real-time access to table list and table structure from remote PostgreSQL.
 *  All tables are created after pull-out structure from remote PostgreSQL.
 *  If `cache_tables` == 1 (default: 0) table structure is cached and not checked for being modififed,
 *  but it will be updated during detach->attach.
 */
class DatabasePostgreSQL final : public IDatabase, WithContext
{

public:
    DatabasePostgreSQL(
        ContextPtr context,
        const String & metadata_path_,
        const ASTStorage * database_engine_define,
        const String & dbname_,
        const StoragePostgreSQLConfiguration & configuration,
        postgres::PoolWithFailoverPtr pool_,
        bool cache_tables_);

    String getEngineName() const override { return "PostgreSQL"; }
    String getMetadataPath() const override { return metadata_path; }

    bool canContainMergeTreeTables() const override { return false; }
    bool canContainDistributedTables() const override { return false; }
    bool shouldBeEmptyOnDetach() const override { return false; }

    ASTPtr getCreateDatabaseQuery() const override;

    bool empty() const override;

    void loadStoredObjects(ContextMutablePtr, bool, bool force_attach, bool skip_startup_tables) override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name) const override;

    bool isTableExist(const String & name, ContextPtr context) const override;
    StoragePtr tryGetTable(const String & name, ContextPtr context) const override;

    void createTable(ContextPtr, const String & table_name, const StoragePtr & storage, const ASTPtr & create_query) override;
    void dropTable(ContextPtr, const String & table_name, bool no_delay) override;

    void attachTable(const String & table_name, const StoragePtr & storage, const String & relative_table_path) override;
    StoragePtr detachTable(const String & table_name) override;

    void drop(ContextPtr /*context*/) override;
    void shutdown() override;

protected:
    ASTPtr getCreateTableQueryImpl(const String & table_name, ContextPtr context, bool throw_on_error) const override;

private:
    String metadata_path;
    ASTPtr database_engine_define;
    StoragePostgreSQLConfiguration configuration;
    postgres::PoolWithFailoverPtr pool;
    const bool cache_tables;

    mutable Tables cached_tables;
    std::unordered_set<std::string> detached_or_dropped;
    BackgroundSchedulePool::TaskHolder cleaner_task;

    String getTableNameForLogs(const String & table_name) const;

    String formatTableName(const String & table_name, bool quoted = true) const;

    bool checkPostgresTable(const String & table_name) const;

    StoragePtr fetchTable(const String & table_name, ContextPtr context, const bool table_checked) const;

    void removeOutdatedTables();

    ASTPtr getColumnDeclaration(const DataTypePtr & data_type) const;
};

}

#endif
