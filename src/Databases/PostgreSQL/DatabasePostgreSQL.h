#pragma once

#include "config.h"

#if USE_LIBPQXX

#include <Databases/DatabasesCommon.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Parsers/ASTCreateQuery.h>
#include <Core/PostgreSQL/PoolWithFailover.h>

namespace DB
{

class Context;
struct AlterCommand;


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
        const StoragePostgreSQL::Configuration & configuration,
        postgres::PoolWithFailoverPtr pool_,
        bool cache_tables_,
        UUID uuid);

    String getEngineName() const override { return "PostgreSQL"; }
    UUID getUUID() const override { return db_uuid; }

    String getMetadataPath() const override { return metadata_path; }

    bool canContainMergeTreeTables() const override { return false; }
    bool canContainDistributedTables() const override { return false; }
    bool canContainRocksDBTables() const override { return false; }
    bool shouldBeEmptyOnDetach() const override { return false; }

    ASTPtr getCreateDatabaseQuery() const override;

    bool empty() const override;

    void loadStoredObjects(ContextMutablePtr, LoadingStrictnessLevel /*mode*/) override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const override;

    bool isTableExist(const String & name, ContextPtr context) const override;
    StoragePtr tryGetTable(const String & name, ContextPtr context) const override;

    void createTable(ContextPtr, const String & table_name, const StoragePtr & storage, const ASTPtr & create_query) override;
    void dropTable(ContextPtr, const String & table_name, bool sync) override;

    void attachTable(ContextPtr context, const String & table_name, const StoragePtr & storage, const String & relative_table_path) override;
    StoragePtr detachTable(ContextPtr context, const String & table_name) override;

    void drop(ContextPtr /*context*/) override;
    void shutdown() override;

    void alterDatabaseComment(const AlterCommand & command) override;

    std::vector<std::pair<ASTPtr, StoragePtr>> getTablesForBackup(const FilterByNameFunction &, const ContextPtr &) const override { return {}; }

protected:
    ASTPtr getCreateTableQueryImpl(const String & table_name, ContextPtr context, bool throw_on_error) const override;

private:
    String metadata_path;
    ASTPtr database_engine_define;
    StoragePostgreSQL::Configuration configuration;
    postgres::PoolWithFailoverPtr pool;
    const bool cache_tables;

    mutable Tables cached_tables;
    std::unordered_set<std::string> detached_or_dropped;
    BackgroundSchedulePoolTaskHolder cleaner_task;
    LoggerPtr log;

    bool persistent = true;
    const UUID db_uuid;

    String getTableNameForLogs(const String & table_name) const;

    String formatTableName(const String & table_name, bool quoted = true) const;

    bool checkPostgresTable(const String & table_name) const;

    StoragePtr fetchTable(const String & table_name, ContextPtr context, bool table_checked) const TSA_REQUIRES(mutex);

    void removeOutdatedTables();

    ASTPtr getColumnDeclaration(const DataTypePtr & data_type) const;
};

}

#endif
