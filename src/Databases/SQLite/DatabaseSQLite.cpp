#include "DatabaseSQLite.h"

#if USE_SQLITE

#include <base/logger_useful.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Databases/SQLite/fetchSQLiteTableStructure.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Interpreters/Context.h>
#include <Storages/StorageSQLite.h>
#include <Databases/SQLite/SQLiteUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SQLITE_ENGINE_ERROR;
    extern const int UNKNOWN_TABLE;
}

DatabaseSQLite::DatabaseSQLite(
        ContextPtr context_,
        const ASTStorage * database_engine_define_,
        bool is_attach_,
        const String & database_path_)
    : IDatabase("SQLite")
    , WithContext(context_->getGlobalContext())
    , database_engine_define(database_engine_define_->clone())
    , database_path(database_path_)
    , log(&Poco::Logger::get("DatabaseSQLite"))
{
    sqlite_db = openSQLiteDB(database_path_, context_, !is_attach_);
}


bool DatabaseSQLite::empty() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return fetchTablesList().empty();
}


DatabaseTablesIteratorPtr DatabaseSQLite::getTablesIterator(ContextPtr local_context, const IDatabase::FilterByNameFunction &) const
{
    std::lock_guard<std::mutex> lock(mutex);

    Tables tables;
    auto table_names = fetchTablesList();
    for (const auto & table_name : table_names)
        tables[table_name] = fetchTable(table_name, local_context, true);

    return std::make_unique<DatabaseTablesSnapshotIterator>(tables, database_name);
}


std::unordered_set<std::string> DatabaseSQLite::fetchTablesList() const
{
    if (!sqlite_db)
        sqlite_db = openSQLiteDB(database_path, getContext(), /* throw_on_error */true);

    std::unordered_set<String> tables;
    std::string query = "SELECT name FROM sqlite_master "
                        "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'";

    auto callback_get_data = [](void * res, int col_num, char ** data_by_col, char ** /* col_names */) -> int
    {
        for (int i = 0; i < col_num; ++i)
            static_cast<std::unordered_set<std::string> *>(res)->insert(data_by_col[i]);
        return 0;
    };

    char * err_message = nullptr;
    int status = sqlite3_exec(sqlite_db.get(), query.c_str(), callback_get_data, &tables, &err_message);
    if (status != SQLITE_OK)
    {
        String err_msg(err_message);
        sqlite3_free(err_message);
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR,
                        "Cannot fetch sqlite database tables. Error status: {}. Message: {}",
                        status, err_msg);
    }

    return tables;
}


bool DatabaseSQLite::checkSQLiteTable(const String & table_name) const
{
    if (!sqlite_db)
        sqlite_db = openSQLiteDB(database_path, getContext(), /* throw_on_error */true);

    const String query = fmt::format("SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';", table_name);

    auto callback_get_data = [](void * res, int, char **, char **) -> int
    {
        *(static_cast<int *>(res)) += 1;
        return 0;
    };

    int count = 0;
    char * err_message = nullptr;
    int status = sqlite3_exec(sqlite_db.get(), query.c_str(), callback_get_data, &count, &err_message);
    if (status != SQLITE_OK)
    {
        String err_msg(err_message);
        sqlite3_free(err_message);
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR,
                        "Cannot check sqlite table. Error status: {}. Message: {}",
                        status, err_msg);
    }

    return (count != 0);
}


bool DatabaseSQLite::isTableExist(const String & table_name, ContextPtr) const
{
    std::lock_guard<std::mutex> lock(mutex);
    return checkSQLiteTable(table_name);
}


StoragePtr DatabaseSQLite::tryGetTable(const String & table_name, ContextPtr local_context) const
{
    std::lock_guard<std::mutex> lock(mutex);
    return fetchTable(table_name, local_context, false);
}


StoragePtr DatabaseSQLite::fetchTable(const String & table_name, ContextPtr local_context, bool table_checked) const
{
    if (!sqlite_db)
        sqlite_db = openSQLiteDB(database_path, getContext(), /* throw_on_error */true);

    if (!table_checked && !checkSQLiteTable(table_name))
        return StoragePtr{};

    auto columns = fetchSQLiteTableStructure(sqlite_db.get(), table_name);

    if (!columns)
        return StoragePtr{};

    auto storage = StorageSQLite::create(
        StorageID(database_name, table_name),
        sqlite_db,
        database_path,
        table_name,
        ColumnsDescription{*columns},
        ConstraintsDescription{},
        local_context);

    return storage;
}


ASTPtr DatabaseSQLite::getCreateDatabaseQuery() const
{
    const auto & create_query = std::make_shared<ASTCreateQuery>();
    create_query->database = getDatabaseName();
    create_query->set(create_query->storage, database_engine_define);

    if (const auto comment_value = getDatabaseComment(); !comment_value.empty())
        create_query->set(create_query->comment, std::make_shared<ASTLiteral>(comment_value));

    return create_query;
}


ASTPtr DatabaseSQLite::getCreateTableQueryImpl(const String & table_name, ContextPtr local_context, bool throw_on_error) const
{
    auto storage = fetchTable(table_name, local_context, false);
    if (!storage)
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "SQLite table {}.{} does not exist",
                            database_name, table_name);
        return nullptr;
    }

    auto create_table_query = std::make_shared<ASTCreateQuery>();
    auto table_storage_define = database_engine_define->clone();
    create_table_query->set(create_table_query->storage, table_storage_define);

    auto columns_declare_list = std::make_shared<ASTColumns>();
    auto columns_expression_list = std::make_shared<ASTExpressionList>();

    columns_declare_list->set(columns_declare_list->columns, columns_expression_list);
    create_table_query->set(create_table_query->columns_list, columns_declare_list);

    /// init create query.
    auto table_id = storage->getStorageID();
    create_table_query->table = table_id.table_name;
    create_table_query->database = table_id.database_name;

    auto metadata_snapshot = storage->getInMemoryMetadataPtr();
    for (const auto & column_type_and_name : metadata_snapshot->getColumns().getOrdinary())
    {
        const auto & column_declaration = std::make_shared<ASTColumnDeclaration>();
        column_declaration->name = column_type_and_name.name;
        column_declaration->type = getColumnDeclaration(column_type_and_name.type);
        columns_expression_list->children.emplace_back(column_declaration);
    }

    ASTStorage * ast_storage = table_storage_define->as<ASTStorage>();
    ASTs storage_children = ast_storage->children;
    auto storage_engine_arguments = ast_storage->engine->arguments;

    /// Add table_name to engine arguments
    storage_engine_arguments->children.insert(storage_engine_arguments->children.begin() + 1, std::make_shared<ASTLiteral>(table_id.table_name));

    return create_table_query;
}


ASTPtr DatabaseSQLite::getColumnDeclaration(const DataTypePtr & data_type) const
{
    WhichDataType which(data_type);

    if (which.isNullable())
        return makeASTFunction("Nullable", getColumnDeclaration(typeid_cast<const DataTypeNullable *>(data_type.get())->getNestedType()));

    return std::make_shared<ASTIdentifier>(data_type->getName());
}

}

#endif
