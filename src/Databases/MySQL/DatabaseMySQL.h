#pragma once

#include "config.h"
#if USE_MYSQL

#include <mysqlxx/Pool.h>

#include <Common/ThreadPool.h>
#include <Storages/ColumnsDescription.h>
#include <Databases/DatabasesCommon.h>
#include <Parsers/ASTCreateQuery.h>
#include <mysqlxx/PoolWithFailover.h>

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

namespace DB
{

class Context;
struct AlterCommand;
struct MySQLSettings;
enum class MySQLDataTypesSupport : uint8_t;

/** Real-time access to table list and table structure from remote MySQL
 *  It doesn't make any manipulations with filesystem.
 *  All tables are created by calling code after real-time pull-out structure from remote MySQL
 */
class DatabaseMySQL final : public IDatabase, WithContext
{
public:
    ~DatabaseMySQL() override;

    DatabaseMySQL(
        ContextPtr context,
        const String & database_name,
        const String & metadata_path,
        const ASTStorage * database_engine_define,
        const String & database_name_in_mysql,
        std::unique_ptr<MySQLSettings> settings_,
        mysqlxx::PoolWithFailover && pool,
        bool attach);

    String getEngineName() const override { return "MySQL"; }

    bool canContainMergeTreeTables() const override { return false; }

    bool canContainDistributedTables() const override { return false; }

    bool canContainRocksDBTables() const override { return false; }

    bool shouldBeEmptyOnDetach() const override { return false; }

    bool empty() const override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_nam, bool skip_not_loaded) const override;

    ASTPtr getCreateDatabaseQuery() const override;

    bool isTableExist(const String & name, ContextPtr context) const override;

    StoragePtr tryGetTable(const String & name, ContextPtr context) const override;

    time_t getObjectMetadataModificationTime(const String & name) const override;

    void shutdown() override;

    void drop(ContextPtr /*context*/) override;

    String getMetadataPath() const override;

    void createTable(ContextPtr, const String & table_name, const StoragePtr & storage, const ASTPtr & create_query) override;

    void loadStoredObjects(ContextMutablePtr, LoadingStrictnessLevel /*mode*/) override;

    StoragePtr detachTable(ContextPtr context, const String & table_name) override;

    void detachTablePermanently(ContextPtr context, const String & table_name) override;

    void dropTable(ContextPtr context, const String & table_name, bool sync) override;

    void attachTable(ContextPtr context, const String & table_name, const StoragePtr & storage, const String & relative_table_path) override;

    void alterDatabaseComment(const AlterCommand & command) override;

protected:
    ASTPtr getCreateTableQueryImpl(const String & name, ContextPtr context, bool throw_on_error) const override;

private:
    String metadata_path;
    ASTPtr database_engine_define;
    String database_name_in_mysql;
    std::unique_ptr<MySQLSettings> mysql_settings;

    std::atomic<bool> quit{false};
    std::condition_variable cond;

    using MySQLPool = mysqlxx::PoolWithFailover;
    using ModifyTimeAndStorage = std::pair<UInt64, StoragePtr>;

    mutable MySQLPool mysql_pool;
    mutable std::vector<StoragePtr> outdated_tables;
    mutable std::map<String, ModifyTimeAndStorage> local_tables_cache;

    std::unordered_set<String> remove_or_detach_tables;

    void cleanOutdatedTables();

    void fetchTablesIntoLocalCache(ContextPtr context) const TSA_REQUIRES(mutex);

    std::map<String, UInt64> fetchTablesWithModificationTime(ContextPtr local_context) const;

    std::map<String, ColumnsDescription> fetchTablesColumnsList(const std::vector<String> & tables_name, ContextPtr context) const;

    void destroyLocalCacheExtraTables(const std::map<String, UInt64> & tables_with_modification_time) const TSA_REQUIRES(mutex);

    void fetchLatestTablesStructureIntoCache(const std::map<String, UInt64> & tables_modification_time, ContextPtr context) const TSA_REQUIRES(mutex);

    ThreadFromGlobalPool thread;
};

}

#endif
