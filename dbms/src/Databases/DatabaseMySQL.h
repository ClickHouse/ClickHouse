#pragma once

#include "config_core.h"
#if USE_MYSQL

#include <mysqlxx/Pool.h>
#include <Databases/DatabasesCommon.h>
#include <Interpreters/Context.h>
#include <memory>
#include <Parsers/ASTCreateQuery.h>


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

    DatabaseMySQL(
        const Context & global_context, const String & database_name, const String & metadata_path,
        const ASTStorage * database_engine_define, const String & database_name_in_mysql, mysqlxx::Pool && pool);

    String getEngineName() const override { return "MySQL"; }

    bool empty(const Context & context) const override;

    DatabaseTablesIteratorPtr getTablesIterator(const Context & context, const FilterByNameFunction & filter_by_table_name) override;

    ASTPtr getCreateDatabaseQuery(const Context & /*context*/) const override;

    bool isTableExist(const Context & context, const String & name) const override;

    StoragePtr tryGetTable(const Context & context, const String & name) const override;

    time_t getObjectMetadataModificationTime(const String & name) const override;

    void shutdown() override;

    void drop(const Context & /*context*/) override;

    String getMetadataPath() const override;

    void createTable(const Context &, const String & table_name, const StoragePtr & storage, const ASTPtr & create_query) override;

    void loadStoredObjects(Context &, bool) override;

    StoragePtr detachTable(const String & table_name) override;

    void removeTable(const Context &, const String & table_name) override;

    void attachTable(const String & table_name, const StoragePtr & storage) override;


protected:
    ASTPtr getCreateTableQueryImpl(const Context & context, const String & name, bool throw_on_error) const override;

private:
    Context global_context;
    String metadata_path;
    ASTPtr database_engine_define;
    String database_name_in_mysql;

    mutable std::mutex mutex;
    std::atomic<bool> quit{false};
    std::condition_variable cond;

    using MySQLPool = mysqlxx::Pool;
    using ModifyTimeAndStorage = std::pair<UInt64, StoragePtr>;

    mutable MySQLPool mysql_pool;
    mutable std::vector<StoragePtr> outdated_tables;
    mutable std::map<String, ModifyTimeAndStorage> local_tables_cache;

    std::unordered_set<String> remove_or_detach_tables;

    void cleanOutdatedTables();

    void fetchTablesIntoLocalCache() const;

    std::map<String, UInt64> fetchTablesWithModificationTime() const;

    std::map<String, NamesAndTypesList> fetchTablesColumnsList(const std::vector<String> & tables_name) const;

    void destroyLocalCacheExtraTables(const std::map<String, UInt64> & tables_with_modification_time) const;

    void fetchLatestTablesStructureIntoCache(const std::map<String, UInt64> & tables_modification_time) const;

    ThreadFromGlobalPool thread{&DatabaseMySQL::cleanOutdatedTables, this};
};

}

#endif
