#include "config_core.h"
#if USE_MYSQL

#include <Databases/DatabaseMySQL.h>
#include <Common/parseAddress.h>
#include <IO/Operators.h>
#include <Formats/MySQLBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Storages/StorageMySQL.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/setThreadName.h>
#include <Parsers/ASTCreateQuery.h>
#include <DataTypes/convertMySQLDataType.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
}

static constexpr const std::chrono::seconds cleaner_sleep_time{30};

String toQueryStringWithQuote(const std::vector<String> & quote_list)
{
    WriteBufferFromOwnString quote_list_query;
    quote_list_query << "(";

    for (size_t index = 0; index < quote_list.size(); ++index)
    {
        if (index)
            quote_list_query << ",";

        quote_list_query << quote << quote_list[index];
    }

    quote_list_query << ")";
    return quote_list_query.str();
}

DatabaseMySQL::DatabaseMySQL(
    const Context & context_, const String & database_name_, const String & mysql_host_name_, const UInt16 & mysql_port_,
    const String & mysql_database_name_, const String & mysql_user_name_, const String & mysql_user_password_)
    : global_context(context_), database_name(database_name_), mysql_host_name(mysql_host_name_), mysql_port(mysql_port_),
      mysql_database_name(mysql_database_name_), mysql_user_name(mysql_user_name_), mysql_user_password(mysql_user_password_),
      mysql_pool(mysql_database_name, mysql_host_name, mysql_user_name, mysql_user_password, mysql_port)
{
}

bool DatabaseMySQL::empty(const Context &) const
{
    std::lock_guard<std::mutex> lock(mutex);

    fetchTablesIntoLocalCache();

    return local_tables_cache.empty();
}

DatabaseIteratorPtr DatabaseMySQL::getIterator(const Context &, const FilterByNameFunction & filter_by_table_name)
{
    Tables tables;
    std::lock_guard<std::mutex> lock(mutex);

    fetchTablesIntoLocalCache();

    for (const auto & local_table : local_tables_cache)
        if (!filter_by_table_name || filter_by_table_name(local_table.first))
            tables[local_table.first] = local_table.second.storage;

    return std::make_unique<DatabaseSnapshotIterator>(tables);
}

bool DatabaseMySQL::isTableExist(const Context & context, const String & name) const
{
    return bool(tryGetTable(context, name));
}

StoragePtr DatabaseMySQL::tryGetTable(const Context &, const String & mysql_table_name) const
{
    std::lock_guard<std::mutex> lock(mutex);

    fetchTablesIntoLocalCache();

    if (local_tables_cache.find(mysql_table_name) != local_tables_cache.end())
        return local_tables_cache[mysql_table_name].storage;

    return StoragePtr{};
}

ASTPtr DatabaseMySQL::tryGetCreateTableQuery(const Context &, const String & table_name) const
{
    std::lock_guard<std::mutex> lock(mutex);

    fetchTablesIntoLocalCache();

    if (local_tables_cache.find(table_name) == local_tables_cache.end())
        throw Exception("MySQL table " + mysql_database_name + "." + table_name + " doesn't exist..", ErrorCodes::UNKNOWN_TABLE);

    return local_tables_cache[table_name].create_table_query;
}

time_t DatabaseMySQL::getTableMetadataModificationTime(const Context &, const String & table_name)
{
    std::lock_guard<std::mutex> lock(mutex);

    fetchTablesIntoLocalCache();

    if (local_tables_cache.find(table_name) == local_tables_cache.end())
        throw Exception("MySQL table " + mysql_database_name + "." + table_name + " doesn't exist..", ErrorCodes::UNKNOWN_TABLE);

    return time_t(local_tables_cache[table_name].modification_time);
}

ASTPtr DatabaseMySQL::getCreateDatabaseQuery(const Context &) const
{
    const auto & create_query = std::make_shared<ASTCreateQuery>();
    create_query->database = database_name;

    const auto & storage = std::make_shared<ASTStorage>();
    storage->set(storage->engine, makeASTFunction("MySQL",
        std::make_shared<ASTLiteral>(mysql_host_name + ":" + toString(mysql_port)), std::make_shared<ASTLiteral>(mysql_database_name),
        std::make_shared<ASTLiteral>(mysql_user_name), std::make_shared<ASTLiteral>(mysql_user_password)));

    create_query->set(create_query->storage, storage);
    return create_query;
}

void DatabaseMySQL::fetchTablesIntoLocalCache() const
{
    const auto & tables_with_modification_time = fetchTablesWithModificationTime();

    destroyLocalCacheExtraTables(tables_with_modification_time);
    fetchLatestTablesStructureIntoCache(tables_with_modification_time);
}

void DatabaseMySQL::destroyLocalCacheExtraTables(const std::map<String, UInt64> & tables_with_modification_time) const
{
    for (auto iterator = local_tables_cache.begin(); iterator != local_tables_cache.end();)
    {
        if (tables_with_modification_time.find(iterator->first) != tables_with_modification_time.end())
            ++iterator;
        else
        {
            outdated_tables.emplace_back(iterator->second.storage);
            iterator = local_tables_cache.erase(iterator);
        }
    }
}

void DatabaseMySQL::fetchLatestTablesStructureIntoCache(const std::map<String, UInt64> &tables_modification_time) const
{
    std::vector<String> wait_update_tables_name;
    for (const auto & table_modification_time : tables_modification_time)
    {
        const auto & it = local_tables_cache.find(table_modification_time.first);

        /// Outdated or new table structures
        if (it == local_tables_cache.end() || table_modification_time.second > it->second.modification_time)
            wait_update_tables_name.emplace_back(table_modification_time.first);
    }

    std::map<String, NamesAndTypesList> tables_and_columns = fetchTablesColumnsList(wait_update_tables_name);

    for (const auto & table_and_columns : tables_and_columns)
    {
        const auto & table_name = table_and_columns.first;
        const auto & columns_name_and_type = table_and_columns.second;
        const auto & table_modification_time = tables_modification_time.at(table_name);

        const auto & iterator = local_tables_cache.find(table_name);
        if (iterator != local_tables_cache.end())
        {
            outdated_tables.emplace_back(iterator->second.storage);
            local_tables_cache.erase(iterator);
        }

        local_tables_cache[table_name] = createStorageInfo(table_name, columns_name_and_type, table_modification_time);
    }
}

static ASTPtr getTableColumnsCreateQuery(const NamesAndTypesList & names_and_types_list)
{
    const auto & table_columns_list_ast = std::make_shared<ASTColumns>();
    const auto & columns_expression_list = std::make_shared<ASTExpressionList>();

    for (const auto & table_column_name_and_type : names_and_types_list)
    {
        const auto & column_declaration = std::make_shared<ASTColumnDeclaration>();
        column_declaration->name = table_column_name_and_type.name;
        column_declaration->type = dataTypeConvertToQuery(table_column_name_and_type.type);
        columns_expression_list->children.emplace_back(column_declaration);
    }

    table_columns_list_ast->set(table_columns_list_ast->columns, columns_expression_list);
    return table_columns_list_ast;
}

static ASTPtr getTableStorageCreateQuery(
    const String & host_name, const UInt16 & port,
    const String & database_name, const String & table_name,
    const String & user_name, const String & password)
{
    const auto & table_storage = std::make_shared<ASTStorage>();
    const auto & storage_engine = std::make_shared<ASTFunction>();

    storage_engine->name = "MySQL";
    storage_engine->arguments = std::make_shared<ASTExpressionList>();
    storage_engine->children.push_back(storage_engine->arguments);

    storage_engine->arguments->children = {
        std::make_shared<ASTLiteral>(host_name + ":" + toString(port)),
        std::make_shared<ASTLiteral>(database_name), std::make_shared<ASTLiteral>(table_name),
        std::make_shared<ASTLiteral>(user_name), std::make_shared<ASTLiteral>(password)
    };


    table_storage->set(table_storage->engine, storage_engine);
    return table_storage;
}

DatabaseMySQL::MySQLStorageInfo DatabaseMySQL::createStorageInfo(
    const String & table_name, const NamesAndTypesList & columns_name_and_type, const UInt64 & table_modification_time) const
{
    const auto & mysql_table = StorageMySQL::create(
        database_name, table_name, std::move(mysql_pool), mysql_database_name, table_name,
        false, "", ColumnsDescription{columns_name_and_type}, ConstraintsDescription{}, global_context);

    const auto & create_table_query = std::make_shared<ASTCreateQuery>();

    create_table_query->table = table_name;
    create_table_query->database = database_name;
    create_table_query->set(create_table_query->columns_list, getTableColumnsCreateQuery(columns_name_and_type));
    create_table_query->set(create_table_query->storage, getTableStorageCreateQuery(
        mysql_host_name, mysql_port, mysql_database_name, table_name, mysql_user_name, mysql_user_password));

    MySQLStorageInfo storage_info;
    storage_info.storage = mysql_table;
    storage_info.create_table_query = create_table_query;
    storage_info.modification_time = table_modification_time;

    return storage_info;
}

std::map<String, UInt64> DatabaseMySQL::fetchTablesWithModificationTime() const
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
             " WHERE TABLE_SCHEMA = " << quote << mysql_database_name;

    std::map<String, UInt64> tables_with_modification_time;
    MySQLBlockInputStream result(mysql_pool.Get(), query.str(), tables_status_sample_block, DEFAULT_BLOCK_SIZE);

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

std::map<String, NamesAndTypesList> DatabaseMySQL::fetchTablesColumnsList(const std::vector<String> & tables_name) const
{
    std::map<String, NamesAndTypesList> tables_and_columns;

    if (tables_name.empty())
        return tables_and_columns;

    Block tables_columns_sample_block
    {
        { std::make_shared<DataTypeString>(),   "table_name" },
        { std::make_shared<DataTypeString>(),   "column_name" },
        { std::make_shared<DataTypeString>(),   "column_type" },
        { std::make_shared<DataTypeUInt8>(),    "is_nullable" },
        { std::make_shared<DataTypeUInt8>(),    "is_unsigned" },
        { std::make_shared<DataTypeUInt64>(),   "length" },
    };

    WriteBufferFromOwnString query;
    query << "SELECT "
             " TABLE_NAME AS table_name,"
             " COLUMN_NAME AS column_name,"
             " DATA_TYPE AS column_type,"
             " IS_NULLABLE = 'YES' AS is_nullable,"
             " COLUMN_TYPE LIKE '%unsigned' AS is_unsigned,"
             " CHARACTER_MAXIMUM_LENGTH AS length"
             " FROM INFORMATION_SCHEMA.COLUMNS"
             " WHERE TABLE_SCHEMA = " << quote << mysql_database_name
          << " AND TABLE_NAME IN " << toQueryStringWithQuote(tables_name) << " ORDER BY ORDINAL_POSITION";

    const auto & external_table_functions_use_nulls = global_context.getSettings().external_table_functions_use_nulls;
    MySQLBlockInputStream result(mysql_pool.Get(), query.str(), tables_columns_sample_block, DEFAULT_BLOCK_SIZE);
    while (Block block = result.read())
    {
        size_t rows = block.rows();
        for (size_t i = 0; i < rows; ++i)
        {
            String table_name = (*block.getByPosition(0).column)[i].safeGet<String>();
            tables_and_columns[table_name].emplace_back((*block.getByPosition(1).column)[i].safeGet<String>(),
                                                        convertMySQLDataType(
                                                            (*block.getByPosition(2).column)[i].safeGet<String>(),
                                                            (*block.getByPosition(3).column)[i].safeGet<UInt64>() &&
                                                            external_table_functions_use_nulls,
                                                            (*block.getByPosition(4).column)[i].safeGet<UInt64>(),
                                                            (*block.getByPosition(5).column)[i].safeGet<UInt64>()));
        }
    }
    return tables_and_columns;
}

void DatabaseMySQL::shutdown()
{
    std::map<String, MySQLStorageInfo> tables_snapshot;
    {
        std::lock_guard lock(mutex);
        tables_snapshot = local_tables_cache;
    }

    for (const auto & table_snapshot : tables_snapshot)
        table_snapshot.second.storage->shutdown();

    std::lock_guard lock(mutex);
    local_tables_cache.clear();
}

void DatabaseMySQL::cleanOutdatedTables()
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
                const auto table_lock = (*iterator)->lockAlterIntention(RWLockImpl::NO_QUERY);

                (*iterator)->shutdown();
                (*iterator)->is_dropped = true;
                iterator = outdated_tables.erase(iterator);
            }
        }

        cond.wait_for(lock, cleaner_sleep_time);
    }
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

}

#endif
