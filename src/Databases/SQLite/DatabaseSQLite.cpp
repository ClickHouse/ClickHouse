#include "DatabaseSQLite.h"

#include <DataTypes/DataTypesNumber.h>
#include <Databases/SQLite/fetchSQLiteTableStructure.h>
#include <Interpreters/Context.h>
#include <Storages/StorageSQLite.h>
#include <common/logger_useful.h>


namespace DB
{
DatabaseSQLite::DatabaseSQLite(ContextPtr context_, const ASTStorage * database_engine_define_, const String & database_path_)
    : IDatabase("SQLite")
    , WithContext(context_->getGlobalContext())
    , database_engine_define(database_engine_define_->clone())
    , log(&Poco::Logger::get("DatabaseSQLite"))
{
    sqlite3 * tmp_db_ptr = nullptr;
    int status = sqlite3_open(database_path_.c_str(), &tmp_db_ptr);
    if (status != SQLITE_OK)
    {
        throw Exception(status, sqlite3_errstr(status));
    }

    db_ptr = std::shared_ptr<sqlite3>(tmp_db_ptr, sqlite3_close);
}

bool DatabaseSQLite::empty() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return fetchTablesList().empty();
}

DatabaseTablesIteratorPtr DatabaseSQLite::getTablesIterator(ContextPtr local_context, const IDatabase::FilterByNameFunction &)
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
    std::unordered_set<String> tables;
    std::string query = "SELECT name FROM sqlite_schema \n"
                        "WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%'\n"
                        "ORDER BY 1;";

    auto callback_get_data = [](void * res, int col_num, char ** data_by_col, char ** /* col_names */) -> int {
        for (int i = 0; i < col_num; ++i)
        {
            static_cast<std::unordered_set<std::string> *>(res)->insert(data_by_col[i]);
        }

        return 0;
    };

    char * err_message = nullptr;

    int status = sqlite3_exec(db_ptr.get(), query.c_str(), callback_get_data, &tables, &err_message);

    if (status != SQLITE_OK)
    {
        String err_msg(err_message);
        sqlite3_free(err_message);
        throw Exception(status, "SQLITE_ERR {}: {}", status, err_msg);
    }

    return tables;
}

bool DatabaseSQLite::checkSQLiteTable(const String & table_name) const
{
    const String query = fmt::format("SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';", table_name);

    auto callback_get_data = [](void * res, int, char **, char **) -> int {
        *(static_cast<int *>(res)) += 1;

        return 0;
    };

    int count = 0;

    char * err_message = nullptr;

    int status = sqlite3_exec(db_ptr.get(), query.c_str(), callback_get_data, &count, &err_message);

    if (status != SQLITE_OK)
    {
        String err_msg(err_message);
        sqlite3_free(err_message);
        throw Exception(status, "SQLITE_ERR {}: {}", status, err_msg);
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
    if (!table_checked && !checkSQLiteTable(table_name))
        return StoragePtr{};

    auto columns = fetchSQLiteTableStructure(db_ptr.get(), table_name);

    if (!columns)
        return StoragePtr{};

    auto storage = StorageSQLite::create(
        StorageID(database_name, table_name),
        db_ptr,
        table_name,
        ColumnsDescription{*columns},
        ConstraintsDescription{},
        local_context,
        "");

    return storage;
}

ASTPtr DatabaseSQLite::getCreateDatabaseQuery() const
{
    const auto & create_query = std::make_shared<ASTCreateQuery>();
    create_query->database = getDatabaseName();
    create_query->set(create_query->storage, database_engine_define);
    return create_query;
}

void DatabaseSQLite::shutdown()
{
}


}
