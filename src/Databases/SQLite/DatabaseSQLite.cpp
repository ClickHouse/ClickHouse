#include "DatabaseSQLite.h"

#if USE_SQLITE

#include <Common/logger_useful.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Databases/SQLite/fetchSQLiteTableStructure.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTFunction.h>
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

    const String query = fmt::format("SELECT name FROM sqlite_master WHERE type='table' AND name='{}';", table_name);

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

    auto storage = std::make_shared<StorageSQLite>(
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
    create_query->setDatabase(getDatabaseName());
    create_query->set(create_query->storage, database_engine_define);

    if (const auto comment_value = getDatabaseComment(); !comment_value.empty())
        create_query->set(create_query->comment, std::make_shared<ASTLiteral>(comment_value));

    return create_query;
}


ASTPtr DatabaseSQLite::getCreateTableQueryImpl(const String & table_name, ContextPtr local_context, bool throw_on_error) const
{
    StoragePtr storage;
    {
        std::lock_guard<std::mutex> lock(mutex);
        storage = fetchTable(table_name, local_context, false);
    }
    if (!storage)
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "SQLite table {}.{} does not exist",
                            getDatabaseName(), table_name);
        return nullptr;
    }
    auto table_storage_define = database_engine_define->clone();
    ASTStorage * ast_storage = table_storage_define->as<ASTStorage>();
    auto storage_engine_arguments = ast_storage->engine->arguments;
    auto table_id = storage->getStorageID();
    /// Add table_name to engine arguments
    storage_engine_arguments->children.insert(storage_engine_arguments->children.begin() + 1, std::make_shared<ASTLiteral>(table_id.table_name));

    auto create_table_query = DB::getCreateQueryFromStorage(storage, table_storage_define, true,
                                                            getContext()->getSettingsRef().max_parser_depth, throw_on_error);

    return create_table_query;
}

}

#endif
