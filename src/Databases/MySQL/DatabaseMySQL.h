#pragma once

#include "config.h"
#if USE_MYSQL

#include <mysqlxx/Pool.h>

#include <Common/ThreadPool.h>
#include <Core/LogsLevel.h>
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

/// Log level for an exception that `DatabaseMySQL` tolerates: Warning for a connection failure to
/// the (unreachable) remote, Error for anything else. Must be called from within a catch block.
/// Exposed so that `gtest_mysql_tolerated_connection_failure` can pin the classification.
LogsLevel mysqlToleratedConnectionFailureLogLevel();

/** Real-time access to table list and table structure from remote MySQL
 *  It doesn't make any manipulations with filesystem.
 *  All tables are created by calling code after real-time pull-out structure from remote MySQL
 */
class DatabaseMySQL final : public DatabaseWithAltersOnDiskBase, WithContext
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
        bool attach,
        UUID uuid);

    String getEngineName() const override { return "MySQL"; }
    UUID getUUID() const override { return db_uuid; }

    bool isRemoteDatabase() const override { return true; }

    bool shouldBeEmptyOnDetach() const override { return false; }

    bool empty() const override;

    /// Best-effort: the server itself walks over every database through this iterator (asynchronous
    /// metrics, `SYSTEM` commands, `DROP DATABASE`, `system.kafka_consumers`, ...), so a single
    /// unreachable remote must not make those operations fail. Serves whatever the local cache holds.
    /// The user-facing listings below propagate the failure instead; see `DatabaseRemote`, which
    /// splits the two the same way.
    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_nam, bool skip_not_loaded) const override;

    /// The `system.tables` path. A remote failure is propagated, because answering an empty list of
    /// tables would report "this database has no tables" when the truth is "its server is
    /// unreachable". `StorageSystemTables` decides whether a whole-server scan may skip this
    /// database; see `handleCannotListTables`.
    DatabaseTablesIteratorPtr getTablesIteratorWithHint(
        ContextPtr context,
        const FilterByNameFunction & filter_by_table_name,
        bool skip_not_loaded,
        const TablesFilter & tables_filter) const override;

    /// Drives the explicit `SHOW TABLES`, which propagates a remote failure for the same reason.
    std::vector<LightWeightTableDetails>
    getLightweightTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const override;

    std::optional<LogsLevel> toleratedListTablesFailureLogLevel() const override
    {
        /// The classifier returns `warning` exactly for a tolerated connection failure to the
        /// remote, and `error` for anything else - which must not be hidden behind a skip.
        const auto level = mysqlToleratedConnectionFailureLogLevel();
        if (level == LogsLevel::warning)
            return level;
        return {};
    }

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

    std::vector<std::pair<ASTPtr, StoragePtr>> getTablesForBackup(const FilterByNameFunction &, const ContextPtr &) const override { return {}; }

protected:
    ASTPtr getCreateDatabaseQueryImpl() const override TSA_REQUIRES(mutex);
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

    /// Refreshes the local cache from the remote and returns its visible entries. `throw_on_error`
    /// selects between the two listing semantics described above.
    Tables listTablesImpl(ContextPtr context, const FilterByNameFunction & filter_by_table_name, bool throw_on_error) const;

    std::map<String, UInt64> fetchTablesWithModificationTime(ContextPtr local_context) const;

    std::map<String, ColumnsDescription> fetchTablesColumnsList(const std::vector<String> & tables_name, ContextPtr context) const;

    void destroyLocalCacheExtraTables(const std::map<String, UInt64> & tables_with_modification_time) const TSA_REQUIRES(mutex);

    void fetchLatestTablesStructureIntoCache(const std::map<String, UInt64> & tables_modification_time, ContextPtr context) const TSA_REQUIRES(mutex);

    ThreadFromGlobalPool thread;

    bool persistent = true;
    const UUID db_uuid;
};

}

#endif
