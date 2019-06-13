#pragma once

#include <Common/config.h>

#if USE_MYSQL

#include <mysqlxx/Pool.h>
#include <Databases/DatabasesCommon.h>

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

    bool empty(const Context & context) override;

    DatabaseIteratorPtr getIterator(const Context & context) override;

    ASTPtr getCreateDatabaseQuery(const Context & context) const override;

    bool isTableExist(const Context & context, const String & name) override;

    StoragePtr tryGetTable(const Context & context, const String & name) override;

    ASTPtr tryGetCreateTableQuery(const Context & context, const String & name) override;

    time_t getTableMetadataModificationTime(const Context & context, const String & name) override;

    void shutdown() override;

    StoragePtr detachTable(const String &) override
    {
        throw Exception("MySQL database engine does not support detach table.", ErrorCodes::NOT_IMPLEMENTED);
    }

    void loadTables(Context &, ThreadPool *, bool) override
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

    void renameTable(const Context &, const String &, IDatabase &, const String &) override
    {
        throw Exception("MySQL database engine does not support rename table.", ErrorCodes::NOT_IMPLEMENTED);
    }

    void createTable(const Context &, const String &, const StoragePtr &, const ASTPtr &) override
    {
        throw Exception("MySQL database engine does not support create table.", ErrorCodes::NOT_IMPLEMENTED);
    }

    void alterTable(const Context &, const String &, const ColumnsDescription &, const IndicesDescription &, const ASTModifier &) override
    {
        throw Exception("MySQL database engine does not support alter table.", ErrorCodes::NOT_IMPLEMENTED);
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

    std::mutex mutex;
    std::atomic<bool> quit{false};
    std::condition_variable cond;

    mysqlxx::Pool mysql_pool;
    std::vector<StoragePtr> outdated_tables;
    std::map<String, MySQLStorageInfo> local_tables_cache;


    void cleanOutdatedTables();

    void fetchTablesIntoLocalCache();

    std::map<String, UInt64> fetchTablesWithModificationTime();

    DatabaseMySQL::MySQLStorageInfo createStorageInfo(
        const String & table_name, const NamesAndTypesList & columns_name_and_type, const UInt64 & table_modification_time);

    std::map<String, NamesAndTypesList> fetchTablesColumnsList(const std::vector<String> & tables_name);

    void destroyLocalCacheExtraTables(const std::map<String, UInt64> & tables_with_modification_time);

    void fetchLatestTablesStructureIntoCache(const std::map<String, UInt64> & tables_modification_time);

    ThreadFromGlobalPool thread{&DatabaseMySQL::cleanOutdatedTables, this};
};

}

#endif
