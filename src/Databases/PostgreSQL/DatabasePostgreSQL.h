#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX

#include <Databases/DatabasesCommon.h>
#include <Core/BackgroundSchedulePool.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/PostgreSQL/PoolWithFailover.h>


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
        const String & postgres_dbname,
        postgres::PoolWithFailoverPtr connection_pool_,
        bool cache_tables_);

    String getEngineName() const override { return "PostgreSQL"; }
    String getMetadataPath() const override { return metadata_path; }

    bool canContainMergeTreeTables() const override { return false; }
    bool canContainDistributedTables() const override { return false; }
    bool shouldBeEmptyOnDetach() const override { return false; }

    ASTPtr getCreateDatabaseQuery() const override;

    bool empty() const override;

    void loadStoredObjects(ContextPtr, bool, bool force_attach) override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name) override;

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
    String dbname;
    postgres::PoolWithFailoverPtr connection_pool;
    const bool cache_tables;

    mutable Tables cached_tables;
    std::unordered_set<std::string> detached_or_dropped;
    BackgroundSchedulePool::TaskHolder cleaner_task;

    bool checkPostgresTable(const String & table_name) const;
    std::unordered_set<std::string> fetchTablesList() const;
    StoragePtr fetchTable(const String & table_name, ContextPtr context, bool table_checked) const;
    void removeOutdatedTables();
    ASTPtr getColumnDeclaration(const DataTypePtr & data_type) const;
};

}

#endif
