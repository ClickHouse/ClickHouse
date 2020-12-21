#include <Databases/PostgreSQL/DatabasePostgreSQL.h>

#if USE_LIBPQXX
#include <string>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Common/escapeForFileName.h>
#include <Common/parseAddress.h>
#include <Common/setThreadName.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>

#include <common/logger_useful.h>

#include <DataStreams/PostgreSQLBlockInputStream.h>
#include <TableFunctions/TableFunctionPostgreSQL.h>
#include <Databases/PostgreSQL/FetchFromPostgreSQL.h>


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

DatabasePostgreSQL::DatabasePostgreSQL(
        const Context & context,
        const String & metadata_path_,
        const ASTStorage * database_engine_define_,
        const String & dbname_,
        const String & postgres_dbname,
        PGConnectionPtr connection_)
    : IDatabase(dbname_)
    , global_context(context.getGlobalContext())
    , metadata_path(metadata_path_)
    , database_engine_define(database_engine_define_->clone())
    , dbname(postgres_dbname)
    , connection(std::move(connection_))
{
}


bool DatabasePostgreSQL::empty() const
{
    LOG_TRACE(&Poco::Logger::get("kssenii"), "empty");
    std::lock_guard<std::mutex> lock(mutex);

    auto tables_list = fetchTablesList();

    for (const auto & table_name : tables_list)
        if (!detached_tables.count(table_name))
            return false;

    return true;
}


DatabaseTablesIteratorPtr DatabasePostgreSQL::getTablesIterator(
        const Context & context, const FilterByNameFunction & /* filter_by_table_name */)
{
    LOG_TRACE(&Poco::Logger::get("kssenii"), "getTablesIterator");
    std::lock_guard<std::mutex> lock(mutex);

    Tables tables;
    auto table_names = fetchTablesList();

    for (auto & table_name : table_names)
        if (!detached_tables.count(table_name))
            tables[table_name] = fetchTable(table_name, context);

    return std::make_unique<DatabaseTablesSnapshotIterator>(tables, database_name);
}


std::unordered_set<std::string> DatabasePostgreSQL::fetchTablesList() const
{
    LOG_TRACE(&Poco::Logger::get("kssenii"), "fetchTablesList");

    std::unordered_set<std::string> tables;
    std::string query = "SELECT tablename FROM pg_catalog.pg_tables "
        "WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'";
    pqxx::read_transaction tx(*connection->conn());

    for (auto table_name : tx.stream<std::string>(query))
        tables.insert(std::get<0>(table_name));

    return tables;
}


bool DatabasePostgreSQL::checkPostgresTable(const String & table_name) const
{
    pqxx::nontransaction tx(*connection->conn());
    pqxx::result result = tx.exec(fmt::format(
           "SELECT attname FROM pg_attribute "
           "WHERE attrelid = '{}'::regclass "
           "AND NOT attisdropped AND attnum > 0", table_name));

    if (result.empty())
        return false;

    return true;
}


bool DatabasePostgreSQL::isTableExist(const String & table_name, const Context & /* context */) const
{
    LOG_TRACE(&Poco::Logger::get("kssenii"), "isTableExists");
    std::lock_guard<std::mutex> lock(mutex);

    if (detached_tables.count(table_name))
        return false;

    return checkPostgresTable(table_name);
}


StoragePtr DatabasePostgreSQL::tryGetTable(const String & table_name, const Context & context) const
{
    LOG_TRACE(&Poco::Logger::get("kssenii"), "tryGetTable");
    std::lock_guard<std::mutex> lock(mutex);

    if (detached_tables.count(table_name))
        return StoragePtr{};
    else
        return fetchTable(table_name, context);
}


StoragePtr DatabasePostgreSQL::fetchTable(const String & table_name, const Context & context) const
{
    auto use_nulls = context.getSettingsRef().external_table_functions_use_nulls;
    auto columns = fetchTableStructure(connection->conn(), table_name, use_nulls);

    if (!columns)
        return StoragePtr{};

    return StoragePostgreSQL::create(
            StorageID(database_name, table_name), table_name,
            connection, ColumnsDescription{*columns}, ConstraintsDescription{}, context);
}


void DatabasePostgreSQL::attachTable(const String & table_name, const StoragePtr & /* storage */, const String &)
{
    LOG_TRACE(&Poco::Logger::get("kssenii"), "attachTable");
    std::lock_guard<std::mutex> lock{mutex};

    if (!checkPostgresTable(table_name))
        throw Exception(fmt::format("Cannot attach table {}.{} because it does not exist", database_name, table_name), ErrorCodes::UNKNOWN_TABLE);

    if (!detached_tables.count(table_name))
        throw Exception(fmt::format("Cannot attach table {}.{}. It already exists", database_name, table_name), ErrorCodes::TABLE_ALREADY_EXISTS);

    detached_tables.erase(table_name);
}


StoragePtr DatabasePostgreSQL::detachTable(const String & table_name)
{
    LOG_TRACE(&Poco::Logger::get("kssenii"), "detachTable");
    std::lock_guard<std::mutex> lock{mutex};

    if (!checkPostgresTable(table_name))
        throw Exception(fmt::format("Cannot detach table {}.{} because it does not exist", database_name, table_name), ErrorCodes::UNKNOWN_TABLE);

    if (detached_tables.count(table_name))
        throw Exception(fmt::format("Cannot detach table {}.{}. It is already dropped/detached", database_name, table_name), ErrorCodes::TABLE_IS_DROPPED);

    detached_tables.emplace(table_name);
    return StoragePtr{};
}


void DatabasePostgreSQL::createTable(const Context &, const String & table_name, const StoragePtr & storage, const ASTPtr & create_query)
{
    LOG_TRACE(&Poco::Logger::get("kssenii"), "createTable");

    const auto & create = create_query->as<ASTCreateQuery>();

    if (!create->attach)
        throw Exception("PostgreSQL database engine does not support create table", ErrorCodes::NOT_IMPLEMENTED);

    attachTable(table_name, storage, {});
}


void DatabasePostgreSQL::dropTable(const Context &, const String & table_name, bool /*no_delay*/)
{
    LOG_TRACE(&Poco::Logger::get("kssenii"), "detachPermanently");
    std::lock_guard<std::mutex> lock{mutex};

    if (!checkPostgresTable(table_name))
        throw Exception(fmt::format("Cannot drop table {}.{} because it does not exist", database_name, table_name), ErrorCodes::UNKNOWN_TABLE);

    if (detached_tables.count(table_name))
        throw Exception(fmt::format("Table {}.{} is already dropped/detached", database_name, table_name), ErrorCodes::TABLE_IS_DROPPED);

    detached_tables.emplace(table_name);
}


void DatabasePostgreSQL::drop(const Context & /*context*/)
{
    Poco::File(getMetadataPath()).remove(true);
}


ASTPtr DatabasePostgreSQL::getCreateDatabaseQuery() const
{
    LOG_TRACE(&Poco::Logger::get("kssenii"), "getDatabaseQuery");

    const auto & create_query = std::make_shared<ASTCreateQuery>();
    create_query->database = getDatabaseName();
    create_query->set(create_query->storage, database_engine_define);
    return create_query;
}


ASTPtr DatabasePostgreSQL::getCreateTableQueryImpl(const String & table_name, const Context & context, bool throw_on_error) const
{
    LOG_TRACE(&Poco::Logger::get("kssenii"), "getTableQueryImpl");

    auto storage = fetchTable(table_name, context);
    if (!storage)
    {
        if (throw_on_error)
            throw Exception(fmt::format("PostgreSQL table {}.{} does not exist", database_name, table_name), ErrorCodes::UNKNOWN_TABLE);

        return nullptr;
    }

    /// Get create table query from storage

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

            std::function<ASTPtr(const DataTypePtr &)> convert_datatype_to_query = [&](const DataTypePtr & data_type) -> ASTPtr
            {
                WhichDataType which(data_type);
                if (!which.isNullable())
                    return std::make_shared<ASTIdentifier>(data_type->getName());
                return makeASTFunction("Nullable", convert_datatype_to_query(typeid_cast<const DataTypeNullable *>(data_type.get())->getNestedType()));
            };

            column_declaration->type = convert_datatype_to_query(column_type_and_name.type);
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

}

#endif
