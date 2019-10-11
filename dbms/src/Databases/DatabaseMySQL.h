#pragma once

#include "config_core.h"
#if USE_MYSQL

#include <mysqlxx/Pool.h>
#include <Databases/DatabasesCommon.h>
#include <Interpreters/Context.h>


namespace DB
{

/** Real-time access to table list and table structure from remote MySQL
 *  It doesn't make any manipulations with filesystem.
 *  All tables are created by calling code after real-time pull-out structure from remote MySQL
 */
class DatabaseMySQL : public IDatabase
{
public:
    ~DatabaseMySQL() override;

    DatabaseMySQL(const Context & context_, const String & database_name_, const String & mysql_host_name_, const UInt16 & mysql_port_,
        const String & mysql_database_name_, const String & mysql_user_name_, const String & mysql_user_password_);

    String getEngineName() const override { return "MySQL"; }

    String getDatabaseName() const override { return database_name; }

    bool empty(const Context & context) const override;

    DatabaseIteratorPtr getIterator(const Context & context, const FilterByNameFunction & filter_by_table_name = {}) override;

    ASTPtr getCreateDatabaseQuery(const Context & context) const override;

    bool isTableExist(const Context & context, const String & name) const override;

    StoragePtr tryGetTable(const Context & context, const String & name) const override;

    ASTPtr tryGetCreateTableQuery(const Context & context, const String & name) const override;

    time_t getTableMetadataModificationTime(const Context & context, const String & name) override;

    void shutdown() override;

    StoragePtr detachTable(const String &) override
    {
        throw Exception("MySQL database engine does not support detach table.", ErrorCodes::NOT_IMPLEMENTED);
    }

    void loadTables(Context &, bool) override
    {
        /// do nothing
    }

    void removeTable(const Context &, const String &) override
    {
        throw Exception("MySQL database engine does not support remove table.", ErrorCodes::NOT_IMPLEMENTED);
    }

    void attachTable(const String &, const StoragePtr &) override
    {
        throw Exception("MySQL database engine does not support attach table.", ErrorCodes::NOT_IMPLEMENTED);
    }

    void createTable(const Context &, const String &, const StoragePtr &, const ASTPtr &) override
    {
        throw Exception("MySQL database engine does not support create table.", ErrorCodes::NOT_IMPLEMENTED);
    }

private:
    struct MySQLStorageInfo
    {
        StoragePtr storage;
        UInt64 modification_time;
        ASTPtr create_table_query;
    };

    const Context global_context;
    const String database_name;
    const String mysql_host_name;
    const UInt16 mysql_port;
    const String mysql_database_name;
    const String mysql_user_name;
    const String mysql_user_password;

    mutable std::mutex mutex;
    std::atomic<bool> quit{false};
    std::condition_variable cond;

    mutable mysqlxx::Pool mysql_pool;
    mutable std::vector<StoragePtr> outdated_tables;
    mutable std::map<String, MySQLStorageInfo> local_tables_cache;


    void cleanOutdatedTables();

    void fetchTablesIntoLocalCache() const;

    std::map<String, UInt64> fetchTablesWithModificationTime() const;

    DatabaseMySQL::MySQLStorageInfo createStorageInfo(
        const String & table_name, const NamesAndTypesList & columns_name_and_type, const UInt64 & table_modification_time) const;

    std::map<String, NamesAndTypesList> fetchTablesColumnsList(const std::vector<String> & tables_name) const;

    void destroyLocalCacheExtraTables(const std::map<String, UInt64> & tables_with_modification_time) const;

    void fetchLatestTablesStructureIntoCache(const std::map<String, UInt64> & tables_modification_time) const;

    ThreadFromGlobalPool thread{&DatabaseMySQL::cleanOutdatedTables, this};
};

}

#endif
