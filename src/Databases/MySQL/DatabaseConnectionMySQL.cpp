#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL
#    include <string>
#    include <DataTypes/DataTypeDateTime.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypeString.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <DataTypes/convertMySQLDataType.h>
#    include <Databases/MySQL/DatabaseConnectionMySQL.h>
#    include <Databases/MySQL/FetchTablesColumnsList.h>
#    include <Formats/MySQLBlockInputStream.h>
#    include <IO/Operators.h>
#    include <Interpreters/Context.h>
#    include <Parsers/ASTCreateQuery.h>
#    include <Parsers/ASTFunction.h>
#    include <Parsers/ParserCreateQuery.h>
#    include <Parsers/parseQuery.h>
#    include <Parsers/queryToString.h>
#    include <Storages/StorageMySQL.h>
#    include <Common/escapeForFileName.h>
#    include <Common/parseAddress.h>
#    include <Common/setThreadName.h>

#    include <Poco/DirectoryIterator.h>
#    include <Poco/File.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int TABLE_IS_DROPPED;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNEXPECTED_AST_STRUCTURE;
}

constexpr static const auto suffix = ".remove_flag";
static constexpr const std::chrono::seconds cleaner_sleep_time{30};
static const std::chrono::seconds lock_acquire_timeout{10};

DatabaseConnectionMySQL::DatabaseConnectionMySQL(const Context & context, const String & database_name_, const String & metadata_path_,
    const ASTStorage * database_engine_define_, const String & database_name_in_mysql_, std::unique_ptr<ConnectionMySQLSettings> settings_, mysqlxx::Pool && pool)
    : IDatabase(database_name_)
    , global_context(context.getGlobalContext())
    , metadata_path(metadata_path_)
    , database_engine_define(database_engine_define_->clone())
    , database_name_in_mysql(database_name_in_mysql_)
    , database_settings(std::move(settings_))
    , mysql_pool(std::move(pool))
{
    empty(); /// test database is works fine.
    thread = ThreadFromGlobalPool{&DatabaseConnectionMySQL::cleanOutdatedTables, this};
}

bool DatabaseConnectionMySQL::empty() const
{
    std::lock_guard<std::mutex> lock(mutex);

    fetchTablesIntoLocalCache(global_context);

    if (local_tables_cache.empty())
        return true;

    for (const auto & [table_name, storage_info] : local_tables_cache)
        if (!remove_or_detach_tables.count(table_name))
            return false;

    return true;
}

DatabaseTablesIteratorPtr DatabaseConnectionMySQL::getTablesIterator(const Context & context, const FilterByNameFunction & filter_by_table_name)
{
    Tables tables;
    std::lock_guard<std::mutex> lock(mutex);

    fetchTablesIntoLocalCache(context);

    for (const auto & [table_name, modify_time_and_storage] : local_tables_cache)
        if (!remove_or_detach_tables.count(table_name) && (!filter_by_table_name || filter_by_table_name(table_name)))
            tables[table_name] = modify_time_and_storage.second;

    return std::make_unique<DatabaseTablesSnapshotIterator>(tables, database_name);
}

bool DatabaseConnectionMySQL::isTableExist(const String & name, const Context & context) const
{
    return bool(tryGetTable(name, context));
}

StoragePtr DatabaseConnectionMySQL::tryGetTable(const String & mysql_table_name, const Context & context) const
{
    std::lock_guard<std::mutex> lock(mutex);

    fetchTablesIntoLocalCache(context);

    if (!remove_or_detach_tables.count(mysql_table_name) && local_tables_cache.find(mysql_table_name) != local_tables_cache.end())
        return local_tables_cache[mysql_table_name].second;

    return StoragePtr{};
}

static ASTPtr getCreateQueryFromStorage(const StoragePtr & storage, const ASTPtr & database_engine_define)
{
    auto create_table_query = std::make_shared<ASTCreateQuery>();

    auto table_storage_define = database_engine_define->clone();
    create_table_query->set(create_table_query->storage, table_storage_define);

    auto columns_declare_list = std::make_shared<ASTColumns>();
    auto columns_expression_list = std::make_shared<ASTExpressionList>();

    columns_declare_list->set(columns_declare_list->columns, columns_expression_list);
    create_table_query->set(create_table_query->columns_list, columns_declare_list);

    {
        /// init create query.
        auto table_id = storage->getStorageID();
        create_table_query->table = table_id.table_name;
        create_table_query->database = table_id.database_name;

        auto metadata_snapshot = storage->getInMemoryMetadataPtr();
        for (const auto & column_type_and_name : metadata_snapshot->getColumns().getOrdinary())
        {
            const auto & column_declaration = std::make_shared<ASTColumnDeclaration>();
            column_declaration->name = column_type_and_name.name;
            column_declaration->type = dataTypeConvertToQuery(column_type_and_name.type);
            columns_expression_list->children.emplace_back(column_declaration);
        }

        ASTStorage * ast_storage = table_storage_define->as<ASTStorage>();
        ASTs storage_children = ast_storage->children;
        auto storage_engine_arguments = ast_storage->engine->arguments;

        /// Add table_name to engine arguments
        auto mysql_table_name = std::make_shared<ASTLiteral>(table_id.table_name);
        storage_engine_arguments->children.insert(storage_engine_arguments->children.begin() + 2, mysql_table_name);

        /// Unset settings
        storage_children.erase(
            std::remove_if(storage_children.begin(), storage_children.end(),
                [&](const ASTPtr & element) { return element.get() == ast_storage->settings; }),
            storage_children.end());
        ast_storage->settings = nullptr;
    }

    return create_table_query;
}

ASTPtr DatabaseConnectionMySQL::getCreateTableQueryImpl(const String & table_name, const Context & context, bool throw_on_error) const
{
    std::lock_guard<std::mutex> lock(mutex);

    fetchTablesIntoLocalCache(context);

    if (local_tables_cache.find(table_name) == local_tables_cache.end())
    {
        if (throw_on_error)
            throw Exception("MySQL table " + database_name_in_mysql + "." + table_name + " doesn't exist..",
                            ErrorCodes::UNKNOWN_TABLE);
        return nullptr;
    }

    return getCreateQueryFromStorage(local_tables_cache[table_name].second, database_engine_define);
}

time_t DatabaseConnectionMySQL::getObjectMetadataModificationTime(const String & table_name) const
{
    std::lock_guard<std::mutex> lock(mutex);

    fetchTablesIntoLocalCache(global_context);

    if (local_tables_cache.find(table_name) == local_tables_cache.end())
        throw Exception("MySQL table " + database_name_in_mysql + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

    return time_t(local_tables_cache[table_name].first);
}

ASTPtr DatabaseConnectionMySQL::getCreateDatabaseQuery() const
{
    const auto & create_query = std::make_shared<ASTCreateQuery>();
    create_query->database = getDatabaseName();
    create_query->set(create_query->storage, database_engine_define);
    return create_query;
}

void DatabaseConnectionMySQL::fetchTablesIntoLocalCache(const Context & context) const
{
    const auto & tables_with_modification_time = fetchTablesWithModificationTime();

    destroyLocalCacheExtraTables(tables_with_modification_time);
    fetchLatestTablesStructureIntoCache(tables_with_modification_time, context);
}

void DatabaseConnectionMySQL::destroyLocalCacheExtraTables(const std::map<String, UInt64> & tables_with_modification_time) const
{
    for (auto iterator = local_tables_cache.begin(); iterator != local_tables_cache.end();)
    {
        if (tables_with_modification_time.find(iterator->first) != tables_with_modification_time.end())
            ++iterator;
        else
        {
            outdated_tables.emplace_back(iterator->second.second);
            iterator = local_tables_cache.erase(iterator);
        }
    }
}

void DatabaseConnectionMySQL::fetchLatestTablesStructureIntoCache(const std::map<String, UInt64> &tables_modification_time, const Context & context) const
{
    std::vector<String> wait_update_tables_name;
    for (const auto & table_modification_time : tables_modification_time)
    {
        const auto & it = local_tables_cache.find(table_modification_time.first);

        /// Outdated or new table structures
        if (it == local_tables_cache.end() || table_modification_time.second > it->second.first)
            wait_update_tables_name.emplace_back(table_modification_time.first);
    }

    std::map<String, NamesAndTypesList> tables_and_columns = fetchTablesColumnsList(wait_update_tables_name, context);

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

        local_tables_cache[table_name] = std::make_pair(table_modification_time, StorageMySQL::create(
            StorageID(database_name, table_name), std::move(mysql_pool), database_name_in_mysql, table_name,
            false, "", ColumnsDescription{columns_name_and_type}, ConstraintsDescription{}, global_context));
    }
}

std::map<String, UInt64> DatabaseConnectionMySQL::fetchTablesWithModificationTime() const
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
    MySQLBlockInputStream result(mysql_pool.get(), query.str(), tables_status_sample_block, DEFAULT_BLOCK_SIZE);

    while (Block block = result.read())
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

std::map<String, NamesAndTypesList> DatabaseConnectionMySQL::fetchTablesColumnsList(const std::vector<String> & tables_name, const Context & context) const
{
    const auto & settings = context.getSettingsRef();

    return DB::fetchTablesColumnsList(
            mysql_pool,
            database_name_in_mysql,
            tables_name,
            settings.external_table_functions_use_nulls,
            database_settings->mysql_datatypes_support_level);
}

void DatabaseConnectionMySQL::shutdown()
{
    std::map<String, ModifyTimeAndStorage> tables_snapshot;
    {
        std::lock_guard lock(mutex);
        tables_snapshot = local_tables_cache;
    }

    for (const auto & [table_name, modify_time_and_storage] : tables_snapshot)
        modify_time_and_storage.second->shutdown();

    std::lock_guard lock(mutex);
    local_tables_cache.clear();
}

void DatabaseConnectionMySQL::drop(const Context & /*context*/)
{
    Poco::File(getMetadataPath()).remove(true);
}

void DatabaseConnectionMySQL::cleanOutdatedTables()
{
    setThreadName("MySQLDBCleaner");

    std::unique_lock lock{mutex};

    while (!quit.load(std::memory_order_relaxed))
    {
        for (auto iterator = outdated_tables.begin(); iterator != outdated_tables.end();)
        {
            if (!iterator->unique())
                ++iterator;
            else
            {
                const auto table_lock = (*iterator)->lockExclusively(RWLockImpl::NO_QUERY, lock_acquire_timeout);

                (*iterator)->shutdown();
                (*iterator)->is_dropped = true;
                iterator = outdated_tables.erase(iterator);
            }
        }

        cond.wait_for(lock, cleaner_sleep_time);
    }
}

void DatabaseConnectionMySQL::attachTable(const String & table_name, const StoragePtr & storage, const String &)
{
    std::lock_guard<std::mutex> lock{mutex};

    if (!local_tables_cache.count(table_name))
        throw Exception("Cannot attach table " + backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(table_name) +
            " because it does not exist.", ErrorCodes::UNKNOWN_TABLE);

    if (!remove_or_detach_tables.count(table_name))
        throw Exception("Cannot attach table " + backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(table_name) +
            " because it already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);

    /// We use the new storage to replace the original storage, because the original storage may have been dropped
    /// Although we still keep its
    local_tables_cache[table_name].second = storage;

    remove_or_detach_tables.erase(table_name);
    Poco::File remove_flag(getMetadataPath() + '/' + escapeForFileName(table_name) + suffix);

    if (remove_flag.exists())
        remove_flag.remove();
}

StoragePtr DatabaseConnectionMySQL::detachTable(const String & table_name)
{
    std::lock_guard<std::mutex> lock{mutex};

    if (remove_or_detach_tables.count(table_name))
        throw Exception("Table " + backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(table_name) + " is dropped",
            ErrorCodes::TABLE_IS_DROPPED);

    if (!local_tables_cache.count(table_name))
        throw Exception("Table " + backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(table_name) + " doesn't exist.",
            ErrorCodes::UNKNOWN_TABLE);

    remove_or_detach_tables.emplace(table_name);
    return local_tables_cache[table_name].second;
}

String DatabaseConnectionMySQL::getMetadataPath() const
{
    return metadata_path;
}

void DatabaseConnectionMySQL::loadStoredObjects(Context &, bool, bool /*force_attach*/)
{

    std::lock_guard<std::mutex> lock{mutex};
    Poco::DirectoryIterator iterator(getMetadataPath());

    for (Poco::DirectoryIterator end; iterator != end; ++iterator)
    {
        if (iterator->isFile() && endsWith(iterator.name(), suffix))
        {
            const auto & filename = iterator.name();
            const auto & table_name = unescapeForFileName(filename.substr(0, filename.size() - strlen(suffix)));
            remove_or_detach_tables.emplace(table_name);
        }
    }
}

void DatabaseConnectionMySQL::dropTable(const Context &, const String & table_name, bool /*no_delay*/)
{
    std::lock_guard<std::mutex> lock{mutex};

    Poco::File remove_flag(getMetadataPath() + '/' + escapeForFileName(table_name) + suffix);

    if (remove_or_detach_tables.count(table_name))
        throw Exception("Table " + backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(table_name) + " is dropped",
            ErrorCodes::TABLE_IS_DROPPED);

    if (remove_flag.exists())
        throw Exception("The remove flag file already exists but the " + backQuoteIfNeed(database_name) +
            "." + backQuoteIfNeed(table_name) + " does not exists remove tables, it is bug.", ErrorCodes::LOGICAL_ERROR);

    auto table_iter = local_tables_cache.find(table_name);
    if (table_iter == local_tables_cache.end())
        throw Exception("Table " + backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(table_name) + " doesn't exist.",
            ErrorCodes::UNKNOWN_TABLE);

    remove_or_detach_tables.emplace(table_name);

    try
    {
        table_iter->second.second->drop();
        remove_flag.createFile();
    }
    catch (...)
    {
        remove_or_detach_tables.erase(table_name);
        throw;
    }
    table_iter->second.second->is_dropped = true;
}

DatabaseConnectionMySQL::~DatabaseConnectionMySQL()
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

void DatabaseConnectionMySQL::createTable(const Context &, const String & table_name, const StoragePtr & storage, const ASTPtr & create_query)
{
    const auto & create = create_query->as<ASTCreateQuery>();

    if (!create->attach)
        throw Exception("MySQL database engine does not support create table. for tables that were detach or dropped before, "
            "you can use attach to add them back to the MySQL database", ErrorCodes::NOT_IMPLEMENTED);

    /// XXX: hack
    /// In order to prevent users from broken the table structure by executing attach table database_name.table_name (...)
    /// we should compare the old and new create_query to make them completely consistent
    const auto & origin_create_query = getCreateTableQuery(table_name, global_context);
    origin_create_query->as<ASTCreateQuery>()->attach = true;

    if (queryToString(origin_create_query) != queryToString(create_query))
        throw Exception("The MySQL database engine can only execute attach statements of type attach table database_name.table_name",
            ErrorCodes::UNEXPECTED_AST_STRUCTURE);

    attachTable(table_name, storage, {});
}

}

#endif
