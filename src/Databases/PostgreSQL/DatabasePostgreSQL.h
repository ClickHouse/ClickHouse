#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX

#include <Databases/DatabasesCommon.h>
#include <Core/BackgroundSchedulePool.h>
#include <Parsers/ASTCreateQuery.h>


namespace DB
{

class Context;
class PostgreSQLConnection;
using PostgreSQLConnectionPtr = std::shared_ptr<PostgreSQLConnection>;


/** Real-time access to table list and table structure from remote PostgreSQL.
 *  All tables are created after pull-out structure from remote PostgreSQL.
 *  If `cache_tables` == 1 (default: 0) table structure is cached and not checked for being modififed,
 *  but it will be updated during detach->attach.
 */
class DatabasePostgreSQL final : public IDatabase
{

public:
    DatabasePostgreSQL(
        const Context & context,
        const String & metadata_path_,
        const ASTStorage * database_engine_define,
        const String & dbname_,
        const String & postgres_dbname,
        PostgreSQLConnectionPtr connection_,
        const bool cache_tables_);

    String getEngineName() const override { return "PostgreSQL"; }
    String getMetadataPath() const override { return metadata_path; }

    bool canContainMergeTreeTables() const override { return false; }
    bool canContainDistributedTables() const override { return false; }
    bool shouldBeEmptyOnDetach() const override { return false; }

    ASTPtr getCreateDatabaseQuery() const override;

    bool empty() const override;

    void loadStoredObjects(Context &, bool, bool force_attach) override;

    DatabaseTablesIteratorPtr getTablesIterator(const Context & context, const FilterByNameFunction & filter_by_table_name) override;

    bool isTableExist(const String & name, const Context & context) const override;
    StoragePtr tryGetTable(const String & name, const Context & context) const override;

    void createTable(const Context &, const String & table_name, const StoragePtr & storage, const ASTPtr & create_query) override;
    void dropTable(const Context &, const String & table_name, bool no_delay) override;

    void attachTable(const String & table_name, const StoragePtr & storage, const String & relative_table_path) override;
    StoragePtr detachTable(const String & table_name) override;

    void drop(const Context & /*context*/) override;
    void shutdown() override;

protected:
    ASTPtr getCreateTableQueryImpl(const String & table_name, const Context & context, bool throw_on_error) const override;

private:
    const Context & global_context;
    String metadata_path;
    ASTPtr database_engine_define;
    String dbname;
    PostgreSQLConnectionPtr connection;
    const bool cache_tables;

    mutable Tables cached_tables;
    std::unordered_set<std::string> detached_or_dropped;
    BackgroundSchedulePool::TaskHolder cleaner_task;

    bool checkPostgresTable(const String & table_name) const;
    StoragePtr fetchTable(const String & table_name, const Context & context, const bool table_checked) const;
    void removeOutdatedTables();
    ASTPtr getColumnDeclaration(const DataTypePtr & data_type) const;
};

}

#endif
