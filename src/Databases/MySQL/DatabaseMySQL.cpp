#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include "config.h"

#if USE_MYSQL
#    include <filesystem>
#    include <string>
#    include <Poco/Net/NetException.h>
#    include <Columns/IColumn.h>
#    include <Core/Settings.h>
#    include <DataTypes/DataTypeDateTime.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypeString.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <DataTypes/convertMySQLDataType.h>
#    include <Databases/DatabaseFactory.h>
#    include <Databases/MySQL/DatabaseMySQL.h>
#    include <Databases/MySQL/FetchTablesColumnsList.h>
#    include <mysqlxx/Exception.h>
#    include <Disks/IDisk.h>
#    include <IO/Operators.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/DatabaseCatalog.h>
#    include <Interpreters/evaluateConstantExpression.h>
#    include <Parsers/ASTCreateQuery.h>
#    include <Parsers/ASTFunction.h>
#    include <Parsers/ASTIdentifier.h>
#    include <Parsers/IAST_erase.h>
#    include <Parsers/ParserCreateQuery.h>
#    include <Parsers/parseQuery.h>
#    include <Processors/Executors/PullingPipelineExecutor.h>
#    include <Processors/Sources/MySQLSource.h>
#    include <QueryPipeline/QueryPipelineBuilder.h>
#    include <Storages/AlterCommands.h>
#    include <Storages/MySQL/MySQLHelpers.h>
#    include <Storages/MySQL/MySQLSettings.h>
#    include <Storages/NamedCollectionsHelpers.h>
#    include <Storages/StorageMySQL.h>
#    include <base/isSharedPtrUnique.h>
#    include <Common/escapeForFileName.h>
#    include <Common/filesystemHelpers.h>
#    include <Common/parseAddress.h>
#    include <Common/parseRemoteDescription.h>
#    include <Common/setThreadName.h>
#    include <Core/LogsLevel.h>

#if CLICKHOUSE_CLOUD
#    include <Interpreters/SharedDatabaseCatalog.h>
#endif

namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 glob_expansion_max_elements;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace MySQLSetting
{
    extern const MySQLSettingsMySQLDataTypesSupport mysql_datatypes_support_level;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int TABLE_IS_DROPPED;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int CANNOT_CREATE_DATABASE;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int ALL_CONNECTION_TRIES_FAILED;
}

/// Demote only a connection failure to the (unreachable) remote, so that anything else is not
/// hidden. Must be called from within a catch block: it rethrows the active exception to classify it.
/// A failed connect through `mysqlxx::PoolWithFailover::get` arrives rewrapped as
/// `ALL_CONNECTION_TRIES_FAILED`, a direct `mysqlxx::Pool` probe throws `ConnectionFailed` as is,
/// and a connection dropped mid-query throws `ConnectionLost`.
LogsLevel mysqlToleratedConnectionFailureLogLevel()
{
    try
    {
        throw;
    }
    catch (const mysqlxx::ConnectionFailed &)
    {
        return LogsLevel::warning;
    }
    catch (const mysqlxx::ConnectionLost &)
    {
        return LogsLevel::warning;
    }
    catch (const Poco::Net::NetException &)
    {
        return LogsLevel::warning;  // Expected during network connectivity issues
    }
    catch (const Exception & e)
    {
        return e.code() == ErrorCodes::ALL_CONNECTION_TRIES_FAILED ? LogsLevel::warning : LogsLevel::error;
    }
    catch (...)  // Ok - Unexpected failures (logic bugs, disk errors, etc.) must stay loud
    {
        return LogsLevel::error;
    }
}

constexpr static const auto suffix = ".remove_flag";
static constexpr const std::chrono::seconds cleaner_sleep_time{30};
static const Poco::Timespan lock_acquire_timeout{10ull, 0ull};

DatabaseMySQL::DatabaseMySQL(
    ContextPtr context_,
    const String & database_name_,
    const String & metadata_path_,
    const ASTStorage * database_engine_define_,
    const String & database_name_in_mysql_,
    std::unique_ptr<MySQLSettings> settings_,
    mysqlxx::PoolWithFailover && pool,
    bool attach,
    UUID uuid)
    : DatabaseWithAltersOnDiskBase(database_name_)
    , WithContext(context_->getGlobalContext())
    , metadata_path(metadata_path_)
    , database_engine_define(database_engine_define_->clone())
    , database_name_in_mysql(database_name_in_mysql_)
    , mysql_settings(std::move(settings_))
    , mysql_pool(std::move(pool)) /// NOLINT
    , db_uuid(uuid)
{
    try
    {
        /// Test that the database is working fine; it will also fetch tables.
        empty(); // NOLINT(bugprone-standalone-empty)
    }
    catch (...)
    {
        if (attach)
        {
            tryLogCurrentException("DatabaseMySQL", "", mysqlToleratedConnectionFailureLogLevel());
        }
#if CLICKHOUSE_CLOUD
        else if (SharedDatabaseCatalog::initialized() && !SharedDatabaseCatalog::isInitialQuery(context_))
        {
            tryLogCurrentException("DatabaseMySQL", "", mysqlToleratedConnectionFailureLogLevel());
        }
#endif
        else
            throw;
    }

    persistent = !context_->getClientInfo().is_shared_catalog_internal;
    if (persistent)
    {
        auto db_disk = getDisk();
        db_disk->createDirectories(metadata_path);
    }

    thread = ThreadFromGlobalPool{&DatabaseMySQL::cleanOutdatedTables, this};
}

bool DatabaseMySQL::empty() const
{
    std::lock_guard lock(mutex);

    fetchTablesIntoLocalCache(getContext());

    if (local_tables_cache.empty())
        return true;

    for (const auto & [table_name, storage_info] : local_tables_cache)
        if (!remove_or_detach_tables.contains(table_name))
            return false;

    return true;
}

DatabaseTablesIteratorPtr DatabaseMySQL::getTablesIterator(ContextPtr local_context, const FilterByNameFunction & filter_by_table_name, bool /* skip_not_loaded */) const
{
    Tables tables;
    std::lock_guard lock(mutex);

    fetchTablesIntoLocalCache(local_context);

    for (const auto & [table_name, modify_time_and_storage] : local_tables_cache)
        if (!remove_or_detach_tables.contains(table_name) && (!filter_by_table_name || filter_by_table_name(table_name)))
            tables[table_name] = modify_time_and_storage.second;

    return std::make_unique<DatabaseTablesSnapshotIterator>(tables, database_name);
}

/// Note: DatabaseMySQL does not own the underlying data -- it lives on the remote MySQL server.
/// dropTable() is implemented as detachTablePermanently() for this engine, so a "dropped" table
/// here is really just a locally-detached, remotely-recoverable table (same semantics as an
/// explicit DETACH TABLE PERMANENTLY). It is therefore correct for such tables to show up here
/// with is_permanently = 1, tracked via remove_or_detach_tables.
DatabaseDetachedTablesSnapshotIteratorPtr DatabaseMySQL::getDetachedTablesIterator(
    ContextPtr /* context */, const FilterByNameFunction & filter_by_table_name, bool /* skip_not_loaded */) const
{
    SnapshotDetachedTables snapshot;
    std::lock_guard lock(mutex);
    for (const auto & table_name : remove_or_detach_tables)
    {
        if (filter_by_table_name && !filter_by_table_name(table_name))
            continue;
        SnapshotDetachedTable snapshot_table;
        snapshot_table.database = database_name;
        snapshot_table.table = table_name;
        auto db_disk = getDisk();
        fs::path remove_flag_path = fs::path(getMetadataPath()) / (escapeForFileName(table_name) + suffix);
        snapshot_table.is_permanently = db_disk->existsFile(remove_flag_path);
        snapshot.emplace(table_name, std::move(snapshot_table));
    }
    return std::make_unique<DatabaseDetachedTablesSnapshotIterator>(std::move(snapshot));
}

bool DatabaseMySQL::isTableExist(const String & name, ContextPtr local_context) const
{
    return bool(tryGetTable(name, local_context));
}

StoragePtr DatabaseMySQL::tryGetTable(const String & mysql_table_name, ContextPtr local_context) const
{
    std::lock_guard lock(mutex);

    fetchTablesIntoLocalCache(local_context);

    if (!remove_or_detach_tables.contains(mysql_table_name) && local_tables_cache.contains(mysql_table_name))
        return local_tables_cache[mysql_table_name].second;

    return StoragePtr{};
}

ASTPtr DatabaseMySQL::getCreateTableQueryImpl(const String & table_name, ContextPtr local_context, bool throw_on_error) const
{
    std::lock_guard lock(mutex);

    try
    {
        /// This function can throw mysql exception, we don't have enough context to handle it.
        /// So we just catch and re-throw as known exception if needed.
        fetchTablesIntoLocalCache(local_context);
    }
    catch (...)
    {
        if (throw_on_error)
        {
            throw Exception(ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY,
                            "Received error while fetching table structure for table {} from MySQL: {}",
                            backQuote(table_name), getCurrentExceptionMessage(true));
        }

        tryLogCurrentException(__PRETTY_FUNCTION__, "", mysqlToleratedConnectionFailureLogLevel());
    }

    if (!local_tables_cache.contains(table_name))
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "MySQL table {}.{} doesn't exist.", database_name_in_mysql, table_name);
        return nullptr;
    }

    auto storage = local_tables_cache[table_name].second;
    auto table_storage_define = database_engine_define->clone();
    {
        ASTStorage * ast_storage = table_storage_define->as<ASTStorage>();
        ast_storage->engine->setKind(ASTFunction::Kind::TABLE_ENGINE);
        auto storage_engine_arguments = ast_storage->engine->arguments;

        /// Add table_name to engine arguments
        if (typeid_cast<ASTIdentifier *>(storage_engine_arguments->children[0].get()))
        {
            storage_engine_arguments->children.push_back(
                makeASTOperator("equals", make_intrusive<ASTIdentifier>("table"), make_intrusive<ASTLiteral>(table_name)));
        }
        else
        {
            auto mysql_table_name = make_intrusive<ASTLiteral>(table_name);
            storage_engine_arguments->children.insert(storage_engine_arguments->children.begin() + 2, mysql_table_name);
        }

        /// Unset settings
        ast_storage->reset(ast_storage->settings);
    }

    const Settings & settings = getContext()->getSettingsRef();
    auto create_table_query = DB::getCreateQueryFromStorage(
        storage,
        table_storage_define,
        true,
        static_cast<unsigned>(settings[Setting::max_parser_depth]),
        static_cast<unsigned>(settings[Setting::max_parser_backtracks]),
        throw_on_error,
        getContext());
    return create_table_query;
}

time_t DatabaseMySQL::getObjectMetadataModificationTime(const String & table_name) const
{
    std::lock_guard lock(mutex);

    fetchTablesIntoLocalCache(getContext());

    if (!local_tables_cache.contains(table_name))
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "MySQL table {}.{} doesn't exist.", database_name_in_mysql, table_name);

    return time_t(local_tables_cache[table_name].first);
}

ASTPtr DatabaseMySQL::getCreateDatabaseQueryImpl() const
{
    const auto & create_query = make_intrusive<ASTCreateQuery>();
    create_query->setDatabase(database_name);
    create_query->set(create_query->storage, database_engine_define);
    create_query->uuid = db_uuid;

    if (!comment.empty())
        create_query->set(create_query->comment, make_intrusive<ASTLiteral>(comment));

    return create_query;
}

void DatabaseMySQL::fetchTablesIntoLocalCache(ContextPtr local_context) const
{
    const auto & tables_with_modification_time = fetchTablesWithModificationTime(local_context);

    destroyLocalCacheExtraTables(tables_with_modification_time);
    fetchLatestTablesStructureIntoCache(tables_with_modification_time, local_context);
}

void DatabaseMySQL::destroyLocalCacheExtraTables(const std::map<String, UInt64> & tables_with_modification_time) const
{
    for (auto iterator = local_tables_cache.begin(); iterator != local_tables_cache.end();)
    {
        if (tables_with_modification_time.contains(iterator->first))
            ++iterator;
        else
        {
            outdated_tables.emplace_back(iterator->second.second);
            iterator = local_tables_cache.erase(iterator);
        }
    }

    /// Reconcile remove_or_detach_tables with the live remote schema:
    /// - If a table was only ordinarily DETACH'd (no .remove_flag marker), and the remote table
    ///   has disappeared, the entry is pruned (nothing left to ATTACH).
    /// - If a table was permanently detached (DETACH TABLE PERMANENTLY or DROP TABLE → .remove_flag exists),
    ///   the marker and entry are preserved even if the remote table disappears. This ensures that
    ///   if a same-name table is later recreated remotely, it stays hidden in ClickHouse until
    ///   explicit ATTACH TABLE — matching the documented behavior.
    /// Mirrors DatabasePostgreSQL::removeOutdatedTables logic for permanent detach.
    auto db_disk = getDisk();
    for (auto iterator = remove_or_detach_tables.begin(); iterator != remove_or_detach_tables.end();)
    {
        if (tables_with_modification_time.contains(*iterator))
            ++iterator;
        else
        {
            const auto & table_name = *iterator;
            fs::path remove_flag = fs::path(getMetadataPath()) / (escapeForFileName(table_name) + suffix);
            bool is_permanent = persistent && db_disk->existsFile(remove_flag);

            /// Only prune non-permanent detach entries when the remote table disappears.
            /// Permanent detach markers (.remove_flag) are preserved so a recreated remote table
            /// stays hidden until explicit ATTACH TABLE.
            if (!is_permanent)
            {
                if (persistent)
                    db_disk->removeFileIfExists(remove_flag);
                iterator = remove_or_detach_tables.erase(iterator);
            }
            else
            {
                /// Permanent detach: preserve the marker and keep the table in remove_or_detach_tables
                ++iterator;
            }
        }
    }
}

void DatabaseMySQL::fetchLatestTablesStructureIntoCache(
    const std::map<String, UInt64> & tables_modification_time, ContextPtr local_context) const
{
    std::vector<String> wait_update_tables_name;
    for (const auto & table_modification_time : tables_modification_time)
    {
        const auto & it = local_tables_cache.find(table_modification_time.first);

        /// Outdated or new table structures
        if (it == local_tables_cache.end() || table_modification_time.second > it->second.first)
            wait_update_tables_name.emplace_back(table_modification_time.first);
    }

    std::map<String, ColumnsDescription> tables_and_columns = fetchTablesColumnsList(wait_update_tables_name, local_context);

    for (const auto & table_and_columns : tables_and_columns)
    {
        const auto & table_name = table_and_columns.first;
        const auto & columns_name_and_type = table_and_columns.second;
        const auto & table_modification_time = tables_modification_time.at(table_name);

        const auto & iterator = local_tables_cache.find(table_name);
        if (iterator != local_tables_cache.end())
        {
            outdated_tables.emplace_back(iterator->second.second);
            local_tables_cache.erase(iterator);
        }

        local_tables_cache[table_name] = std::make_pair(
            table_modification_time,
            std::make_shared<StorageMySQL>(
                StorageID(database_name, table_name),
                std::move(mysql_pool),
                database_name_in_mysql,
                TableNameOrQuery(TableNameOrQuery::Type::TABLE, table_name),
                /* replace_query_ */ false,
                /* on_duplicate_clause = */ "",
                ColumnsDescription{columns_name_and_type},
                ConstraintsDescription{},
                String{},
                getContext(),
                MySQLSettings{}));
    }
}

std::map<String, UInt64> DatabaseMySQL::fetchTablesWithModificationTime(ContextPtr local_context) const
{
    Block tables_status_sample_block
    {
        { std::make_shared<DataTypeString>(),   "table_name" },
        { std::make_shared<DataTypeDateTime>(), "modification_time" },
    };

    WriteBufferFromOwnString query;
    query << "SELECT"
             " TABLE_NAME AS table_name, "
             " CREATE_TIME AS modification_time "
             " FROM INFORMATION_SCHEMA.TABLES "
             " WHERE TABLE_SCHEMA = " << quote << database_name_in_mysql;

    std::map<String, UInt64> tables_with_modification_time;
    MySQLStreamSettings mysql_input_stream_settings(local_context->getSettingsRef());
    auto result = std::make_unique<MySQLSource>(mysql_pool.get(), query.str(), tables_status_sample_block, mysql_input_stream_settings);
    QueryPipeline pipeline(std::move(result));

    Block block;
    PullingPipelineExecutor executor(pipeline);
    while (executor.pull(block))
    {
        size_t rows = block.rows();
        for (size_t index = 0; index < rows; ++index)
        {
            String table_name = (*block.getByPosition(0).column)[index].safeGet<String>();
            tables_with_modification_time[table_name] = (*block.getByPosition(1).column)[index].safeGet<UInt64>();
        }
    }

    return tables_with_modification_time;
}

std::map<String, ColumnsDescription>
DatabaseMySQL::fetchTablesColumnsList(const std::vector<String> & tables_name, ContextPtr local_context) const
{
    const auto & settings = local_context->getSettingsRef();

    return DB::fetchTablesColumnsList(
            mysql_pool,
            database_name_in_mysql,
            tables_name,
            settings,
            (*mysql_settings)[MySQLSetting::mysql_datatypes_support_level]);
}

void DatabaseMySQL::shutdown()
{
    std::map<String, ModifyTimeAndStorage> tables_snapshot;
    {
        std::lock_guard lock(mutex);
        tables_snapshot = local_tables_cache;
    }

    for (const auto & [table_name, modify_time_and_storage] : tables_snapshot)
        modify_time_and_storage.second->flushAndShutdown();

    std::lock_guard lock(mutex);
    local_tables_cache.clear();
}

void DatabaseMySQL::drop(ContextPtr)
{
    if (!persistent)
        return;

    auto db_disk = getDisk();
    db_disk->removeRecursive(getMetadataPath());
}

void DatabaseMySQL::cleanOutdatedTables()
{
    DB::setThreadName(ThreadName::MYSQL_DATABASE_CLEANUP);

    std::unique_lock lock{mutex};

    while (!quit.load(std::memory_order_relaxed))
    {
        for (auto iterator = outdated_tables.begin(); iterator != outdated_tables.end();)
        {
            if (!isSharedPtrUnique(*iterator))
                ++iterator;
            else
            {
                const auto table_lock = (*iterator)->lockExclusively(RWLockImpl::NO_QUERY, lock_acquire_timeout);

                (*iterator)->flushAndShutdown();
                (*iterator)->is_dropped = true;
                iterator = outdated_tables.erase(iterator);
            }
        }

        /// Background reconciliation: reconcile remove_or_detach_tables with the live remote schema.
        /// - If a table was only ordinarily DETACH'd (no .remove_flag marker), and the remote table
        ///   has disappeared, the entry is pruned (nothing left to ATTACH).
        /// - If a table was permanently detached (DETACH TABLE PERMANENTLY or DROP TABLE → .remove_flag exists),
        ///   the marker and entry are preserved even if the remote table disappears. This ensures that
        ///   if a same-name table is later recreated remotely, it stays hidden in ClickHouse until
        ///   explicit ATTACH TABLE — matching the documented behavior.
        /// Skip reconciliation if there are no detached tables, or if all detached tables are
        /// permanent markers (which can never be pruned), to avoid unnecessary network I/O and
        /// spurious error-level logging during expected remote outages.
        bool has_non_permanent_detach = false;
        if (!remove_or_detach_tables.empty() && persistent)
        {
            auto db_disk = getDisk();
            for (const auto & table_name : remove_or_detach_tables)
            {
                fs::path remove_flag = fs::path(getMetadataPath()) / (escapeForFileName(table_name) + suffix);
                if (!db_disk->existsFile(remove_flag))
                {
                    has_non_permanent_detach = true;
                    break;
                }
            }
        }
        else if (!remove_or_detach_tables.empty() && !persistent)
        {
            has_non_permanent_detach = true;  /// All entries are non-permanent in non-persistent mode
        }

        if (has_non_permanent_detach)
        {
            try
            {
                /// Release mutex before network I/O to avoid blocking other operations
                lock.unlock();

                auto tables_on_remote = fetchTablesWithModificationTime(getContext());

                /// Re-acquire mutex to update remove_or_detach_tables
                lock.lock();

                auto db_disk = getDisk();
                for (auto iter = remove_or_detach_tables.begin(); iter != remove_or_detach_tables.end();)
                {
                    if (!tables_on_remote.contains(*iter))
                    {
                        const auto & table_name = *iter;
                        fs::path remove_flag = fs::path(getMetadataPath()) / (escapeForFileName(table_name) + suffix);
                        bool is_permanent = persistent && db_disk->existsFile(remove_flag);

                        /// Only prune non-permanent detach entries when the remote table disappears.
                        /// Permanent detach markers (.remove_flag) are preserved so a recreated remote table
                        /// stays hidden until explicit ATTACH TABLE.
                        if (!is_permanent)
                        {
                            if (persistent)
                                db_disk->removeFileIfExists(remove_flag);
                            iter = remove_or_detach_tables.erase(iter);
                        }
                        else
                        {
                            /// Permanent detach: preserve the marker and keep the table in remove_or_detach_tables
                            ++iter;
                        }
                    }
                    else
                        ++iter;
                }
            }
            catch (...)
            {
                /// Determine appropriate log level: connection failures during MySQL outages are expected
                /// and logged at warning level, while logic bugs or unexpected errors remain at error level
                /// for visibility.
                auto log_level = mysqlToleratedConnectionFailureLogLevel();
                tryLogCurrentException("DatabaseMySQL", "Background reconciliation failed to fetch remote schema",
                                       log_level);
                /// Ensure we re-acquire the lock before wait_for
                if (!lock.owns_lock())
                    lock.lock();
            }
        }

        cond.wait_for(lock, cleaner_sleep_time);
    }
}

void DatabaseMySQL::attachTable(ContextPtr /* context_ */, const String & table_name, const StoragePtr & storage, const String &)
{
    std::lock_guard lock{mutex};

    if (!local_tables_cache.contains(table_name))
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Cannot attach table {}.{} because it does not exist.",
            backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));

    if (!remove_or_detach_tables.contains(table_name))
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Cannot attach table {}.{} because it already exists.",
            backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));

    /// We use the new storage to replace the original storage, because the original storage may have been dropped
    /// Although we still keep its
    local_tables_cache[table_name].second = storage;

    remove_or_detach_tables.erase(table_name);
    fs::path remove_flag = fs::path(getMetadataPath()) / (escapeForFileName(table_name) + suffix);

    if (!persistent)
        return;

    auto db_disk = getDisk();
    db_disk->removeFileIfExists(remove_flag);
}

StoragePtr DatabaseMySQL::detachTable(ContextPtr /* context */, const String & table_name)
{
    std::lock_guard lock{mutex};

    if (remove_or_detach_tables.contains(table_name))
        throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table {}.{} is dropped",
            backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));

    if (!local_tables_cache.contains(table_name))
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist.",
            backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));

    remove_or_detach_tables.emplace(table_name);
    return local_tables_cache[table_name].second;
}

String DatabaseMySQL::getMetadataPath() const
{
    return metadata_path;
}

void DatabaseMySQL::loadStoredObjects(ContextMutablePtr, LoadingStrictnessLevel /*mode*/)
{
    if (!persistent)
        return;

    auto db_disk = getDisk();
    std::lock_guard lock{mutex};
    for (const auto it = db_disk->iterateDirectory(metadata_path); it->isValid(); it->next())
    {
        auto path = fs::path(it->path());
        if (path.filename().empty())
            path = path.parent_path();

        if (db_disk->existsFile(path) && endsWith(path.filename(), suffix))
        {
            const auto & filename = path.filename().filename().string();
            const auto & table_name = unescapeForFileName(filename.substr(0, filename.size() - strlen(suffix)));
            remove_or_detach_tables.emplace(table_name);
        }
    }
}

void DatabaseMySQL::detachTablePermanently(ContextPtr, const String & table_name)
{
    if (!persistent)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DETACH TABLE is not supported for non-persistent MySQL database");

    auto db_disk = getDisk();
    std::lock_guard lock{mutex};

    fs::path remove_flag = fs::path(getMetadataPath()) / (escapeForFileName(table_name) + suffix);

    if (remove_or_detach_tables.contains(table_name))
        throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table {}.{} is dropped", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));

    if (db_disk->existsFile(remove_flag))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The remove flag file already exists but the {}.{} does not exist remove tables, it is bug.",
                        backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));

    auto table_iter = local_tables_cache.find(table_name);
    if (table_iter == local_tables_cache.end())
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));

    remove_or_detach_tables.emplace(table_name);

    try
    {
        table_iter->second.second->drop();
        db_disk->createFile(remove_flag);
    }
    catch (...)
    {
        remove_or_detach_tables.erase(table_name);
        throw;
    }
    table_iter->second.second->is_detached = true;
}

void DatabaseMySQL::dropTable(ContextPtr local_context, const String & table_name, bool /*sync*/)
{
    if (!persistent)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DROP TABLE is not supported for non-persistent MySQL database");

    auto component_guard = Coordination::setCurrentComponent("DatabaseMySQL::dropTable");
    detachTablePermanently(local_context, table_name);
}

DatabaseMySQL::~DatabaseMySQL()
{
    try
    {
        if (!quit)
        {
            {
                quit = true;
                std::lock_guard lock{mutex};
            }
            cond.notify_one();
            thread.join();
        }

        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void DatabaseMySQL::createTable(ContextPtr local_context, const String & table_name, const StoragePtr & storage, const ASTPtr & create_query)
{
    if (!persistent)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MySQL database engine does not support CREATE or ATTACH TABLE queries");

    const auto & create = create_query->as<ASTCreateQuery>();

    if (!create->attach)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "MySQL database engine does not support create table. "
                        "for tables that were detach or dropped before, you can use attach "
                        "to add them back to the MySQL database");

    /// XXX: hack
    /// In order to prevent users from broken the table structure by executing attach table database_name.table_name (...)
    /// we should compare the old and new create_query to make them completely consistent
    const auto & origin_create_query = getCreateTableQuery(table_name, getContext());
    origin_create_query->as<ASTCreateQuery>()->attach = true;

    if (origin_create_query->formatWithSecretsOneLine() != create_query->formatWithSecretsOneLine())
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
                        "The MySQL database engine can only execute attach statements "
                        "of type attach table database_name.table_name");

    attachTable(local_context, table_name, storage, {});
}

void registerDatabaseMySQL(DatabaseFactory & factory);
void registerDatabaseMySQL(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        auto * engine_define = args.create_query.storage;
        const ASTFunction * engine = engine_define->engine;
        const String & engine_name = engine_define->engine->name;
        if (!engine->arguments)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", engine_name);

        StorageMySQL::Configuration configuration;
        ASTs & arguments = engine->arguments->children;
        auto mysql_settings = std::make_unique<MySQLSettings>();

        if (auto named_collection = tryGetNamedCollectionWithOverrides(arguments, args.context))
        {
            configuration = StorageMySQL::processNamedCollectionResult(*named_collection, *mysql_settings, args.context, false);
        }
        else
        {
            /// The TLS credentials are trailing `key = value` arguments; the copy keeps them in the
            /// stored `CREATE DATABASE` query, where they are masked when it is formatted.
            ASTs positional_arguments = arguments;
            configuration.ssl_params = StorageMySQL::extractSSLParamsFromArguments(positional_arguments, args.context);

            if (positional_arguments.size() != 4)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "MySQL database require mysql_hostname, mysql_database_name, mysql_username, mysql_password arguments "
                    "(optionally followed by ssl_ca_pem = '...', ssl_cert_pem = '...', ssl_key_pem = '...').");


            arguments[1] = evaluateConstantExpressionOrIdentifierAsLiteral(arguments[1], args.context);
            const auto & host_port = safeGetLiteralValue<String>(arguments[0], engine_name);

            if (engine_name == "MySQL")
            {
                size_t max_addresses = args.context->getSettingsRef()[Setting::glob_expansion_max_elements];
                configuration.addresses = parseRemoteDescriptionForExternalDatabase(host_port, max_addresses, 3306);
            }
            else
            {
                const auto & [remote_host, remote_port] = parseAddress(host_port, 3306);
                configuration.host = remote_host;
                configuration.port = remote_port;
            }

            configuration.database = safeGetLiteralValue<String>(arguments[1], engine_name);
            configuration.username = safeGetLiteralValue<String>(arguments[2], engine_name);
            configuration.password = safeGetLiteralValue<String>(arguments[3], engine_name);
        }
        mysql_settings->loadFromQueryContext(args.context, *engine_define);
        if (engine_define->settings)
            mysql_settings->loadFromQuery(*engine_define);

        auto mysql_pool = createMySQLPoolWithFailover(configuration, *mysql_settings);

        try
        {
            return make_shared<DatabaseMySQL>(
                args.context,
                args.database_name,
                args.metadata_path,
                engine_define,
                configuration.database,
                std::move(mysql_settings),
                std::move(mysql_pool),
                args.create_query.attach,
                args.uuid);
        }
        catch (...)
        {
            const auto & exception_message = getCurrentExceptionMessage(true);
            throw Exception(ErrorCodes::CANNOT_CREATE_DATABASE, "Cannot create MySQL database, because {}", exception_message);
        }
    };
    factory.registerDatabase("MySQL", create_fn, {
        .supports_arguments = true,
        .supports_settings = true,
        .is_external = true,
        .source_access_type = AccessTypeObjects::Source::MYSQL,
    }, Documentation{
        .description = R"DOCS_MD(
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# MySQL database engine

<CloudNotSupportedBadge />

Allows to connect to databases on a remote MySQL server and perform `INSERT` and `SELECT` queries to exchange data between ClickHouse and MySQL.

The `MySQL` database engine translate queries to the MySQL server so you can perform operations such as `SHOW TABLES` or `SHOW CREATE TABLE`.

You cannot perform the following queries:

- `RENAME`
- `CREATE TABLE`
- `ALTER`

## Creating a database {#creating-a-database}

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MySQL('host:port', ['database' | database], 'user', 'password')
[SETTINGS enable_compression=0]
```

**Engine Parameters**

- `host:port` — MySQL server address.
- `database` — Remote database name.
- `user` — MySQL user.
- `password` — User password.

**Settings**

### `enable_compression` {#enable-compression}

Enables zlib compression for the MySQL protocol connection. When set to `1`, ClickHouse requests protocol-level compression from the MySQL server.

Default value: `0`.

Example:

```sql
CREATE DATABASE mysql_db
ENGINE = MySQL('localhost:3306', 'test', 'my_user', 'user_password')
SETTINGS enable_compression = 1;
```

## TLS/SSL {#tls-ssl}

The credentials of an encrypted connection to MySQL are passed as [named collection](/concepts/features/configuration/server-config/named-collections) keys (or as key-value arguments):

| Parameter | Description |
|-----------|-------------|
| `ssl_ca_pem` | Contents of the CA certificate that the MySQL server certificate is verified against. |
| `ssl_cert_pem` | Contents of the client certificate, for certificate-based authentication. |
| `ssl_key_pem` | Contents of the private key belonging to `ssl_cert_pem`. |

The values are the contents of the corresponding PEM files, which can be copied into a named collection or into a query. They are masked in logs and in `SHOW` queries, the same way passwords are.

The same credentials can also be given as paths to files on the server, in `ssl_ca`, `ssl_cert` and `ssl_key` — but **only in a named collection defined in the server configuration file**, and such a value cannot be overridden in a query. The server opens those files with its own privileges, so accepting a path from SQL would let any user who is able to define a MySQL source probe the local filesystem, and authenticate with a certificate and key they are not allowed to read themselves.

<a id="data_types-support"></a>
## Data types support {#data-types-support}

| MySQL                            | ClickHouse                                                   |
|----------------------------------|--------------------------------------------------------------|
| UNSIGNED TINYINT                 | [UInt8](/reference/data-types/int-uint)          |
| TINYINT                          | [Int8](/reference/data-types/int-uint)           |
| UNSIGNED SMALLINT                | [UInt16](/reference/data-types/int-uint)         |
| SMALLINT                         | [Int16](/reference/data-types/int-uint)          |
| UNSIGNED INT, UNSIGNED MEDIUMINT | [UInt32](/reference/data-types/int-uint)         |
| INT, MEDIUMINT                   | [Int32](/reference/data-types/int-uint)          |
| UNSIGNED BIGINT                  | [UInt64](/reference/data-types/int-uint)         |
| BIGINT                           | [Int64](/reference/data-types/int-uint)          |
| FLOAT                            | [Float32](/reference/data-types/float)           |
| DOUBLE                           | [Float64](/reference/data-types/float)           |
| DATE                             | [Date](/reference/data-types/date)               |
| DATETIME, TIMESTAMP              | [DateTime](/reference/data-types/datetime)       |
| BINARY                           | [FixedString](/reference/data-types/fixedstring) |
| POINT                            | [Point](/reference/data-types/geo#point)         |
| LINESTRING                       | [LineString](/reference/data-types/geo#linestring) |
| POLYGON                          | [Polygon](/reference/data-types/geo#polygon)     |
| MULTILINESTRING                  | [MultiLineString](/reference/data-types/geo#multilinestring) |
| MULTIPOLYGON                     | [MultiPolygon](/reference/data-types/geo#multipolygon) |
| MULTIPOINT                       | [MultiPoint](/reference/data-types/geo#multipoint) |
| GEOMETRY                         | [Geometry](/reference/data-types/geo#geometry)   |

The conversion of the spatial types (other than `POINT`, which is always converted) is controlled by the `geometry` flag of the [`mysql_datatypes_support_level`](/reference/settings/session-settings/mysql#mysql_datatypes_support_level) setting, enabled by default. The generic `GEOMETRY` column type is mapped to the umbrella [`Geometry`](/reference/data-types/geo#geometry) type (a `Variant` over the concrete geometric types). Because such a column can hold a value of any subtype, reading a value whose subtype has no ClickHouse counterpart (`GEOMETRYCOLLECTION`) throws an exception at read time; this incompatibility is accepted in exchange for a proper geometric type. Columns declared with the `GEOMETRYCOLLECTION` type are converted into [String](/reference/data-types/string) like all other MySQL data types.

[Nullable](/reference/data-types/nullable) is supported. A spatial column maps to `String` (`Nullable(String)` if it is nullable) instead of a geometric type in three cases: it is declared `GEOMETRYCOLLECTION`; the `geometry` flag is disabled and the type is not `POINT`; or the column is nullable and the type is not `POINT`, since `Point` is the only geometric type that can be nested inside `Nullable`. In all three the string holds the value exactly as MySQL returns it: a 4-byte SRID prefix followed by the WKB payload, so strip those 4 leading bytes before passing it to a WKB decoder.

## Global variables support {#global-variables-support}

For better compatibility you may address global variables in MySQL style, as `@@identifier`.

These variables are supported:
- `version`
- `max_allowed_packet`

:::note
By now these variables are stubs and don't correspond to anything.
:::

Example:

```sql
SELECT @@version;
```

## Examples of use {#examples-of-use}

Table in MySQL:

```text
mysql> USE test;
Database changed

mysql> CREATE TABLE `mysql_table` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `float` FLOAT NOT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into mysql_table (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from mysql_table;
+------+-----+
| int_id | value |
+------+-----+
|      1 |     2 |
+------+-----+
1 row in set (0,00 sec)
```

Database in ClickHouse, exchanging data with the MySQL server:

```sql
CREATE DATABASE mysql_db ENGINE = MySQL('localhost:3306', 'test', 'my_user', 'user_password') SETTINGS read_write_timeout=10000, connect_timeout=100;
```

```sql
SHOW DATABASES
```

```text
┌─name─────┐
│ default  │
│ mysql_db │
│ system   │
└──────────┘
```

```sql
SHOW TABLES FROM mysql_db
```

```text
┌─name─────────┐
│  mysql_table │
└──────────────┘
```

```sql
SELECT * FROM mysql_db.mysql_table
```

```text
┌─int_id─┬─value─┐
│      1 │     2 │
└────────┴───────┘
```

```sql
INSERT INTO mysql_db.mysql_table VALUES (3,4)
```

```sql
SELECT * FROM mysql_db.mysql_table
```

```text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```
)DOCS_MD",
        .syntax = "ENGINE = MySQL('host:port', 'database', 'user', 'password')",
        .related = {"PostgreSQL"}});
}
}

#endif
