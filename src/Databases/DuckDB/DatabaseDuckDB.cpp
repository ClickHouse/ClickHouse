#include <Databases/DuckDB/DatabaseDuckDB.h>

#if USE_DUCKDB

#include <Common/logger_useful.h>
#include <Databases/DuckDB/fetchDuckDBTableStructure.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/Context.h>
#include <Storages/DuckDB/StorageDuckDB.h>
#include <Databases/DuckDB/DuckDBUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int DUCKDB_ENGINE_ERROR;
    extern const int UNKNOWN_TABLE;
}

DatabaseDuckDB::DatabaseDuckDB(
        ContextPtr context_,
        const ASTStorage * database_engine_define_,
        bool is_attach_,
        const String & database_path_)
    : IDatabase("DuckDB")
    , WithContext(context_->getGlobalContext())
    , database_engine_define(database_engine_define_->clone())
    , database_path(database_path_)
    , log(&Poco::Logger::get("DatabaseDuckDB"))
{
    duckdb_instance = openDuckDB(database_path_, context_, !is_attach_);
}


bool DatabaseDuckDB::empty() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return fetchTablesList().empty();
}


DatabaseTablesIteratorPtr DatabaseDuckDB::getTablesIterator(ContextPtr local_context, const IDatabase::FilterByNameFunction &) const
{
    std::lock_guard<std::mutex> lock(mutex);

    Tables tables;
    auto table_names = fetchTablesList();
    for (const auto & table_name : table_names)
        tables[table_name] = fetchTable(table_name, local_context, true);

    return std::make_unique<DatabaseTablesSnapshotIterator>(tables, database_name);
}


std::unordered_set<std::string> DatabaseDuckDB::fetchTablesList() const
{
    if (!duckdb_instance)
        duckdb_instance = openDuckDB(database_path, getContext(), /* throw_on_error */true);

    std::unordered_set<String> tables;

    std::string query = "SHOW TABLES;";

    duckdb::Connection con(*duckdb_instance);
    auto result = con.Query(query);

    if (result->HasError())
    {
        throw Exception(ErrorCodes::DUCKDB_ENGINE_ERROR,
                        "Cannot fetch DuckDB database tables. Error type: {}. Message: {}",
                        result->GetErrorType(), result->GetError());
    }

    auto chunk = result->Fetch();

    while (chunk)
    {
        for (size_t idx = 0; idx < chunk->size(); ++idx)
            tables.insert(chunk->GetValue(0, idx).GetValue<std::string>());

        chunk = result->Fetch();
    }

    return tables;
}


bool DatabaseDuckDB::checkDuckDBTable(const String & table_name) const
{
    if (!duckdb_instance)
        duckdb_instance = openDuckDB(database_path, getContext(), /* throw_on_error */true);

    const String query = fmt::format("SELECT table_name FROM duckdb_tables WHERE table_name={};", quoteStringDuckDB(table_name));

    duckdb::Connection con(*duckdb_instance);
    auto result = con.Query(query);

    if (result->HasError())
    {
        throw Exception(ErrorCodes::DUCKDB_ENGINE_ERROR,
                        "Cannot fetch DuckDB database tables. Error type: {}. Message: {}",
                        result->GetErrorType(), result->GetError());
    }

    auto chunk = result->Fetch();

    return chunk.get();
}


bool DatabaseDuckDB::isTableExist(const String & table_name, ContextPtr) const
{
    std::lock_guard<std::mutex> lock(mutex);
    return checkDuckDBTable(table_name);
}


StoragePtr DatabaseDuckDB::tryGetTable(const String & table_name, ContextPtr local_context) const
{
    std::lock_guard<std::mutex> lock(mutex);
    return fetchTable(table_name, local_context, false);
}


StoragePtr DatabaseDuckDB::fetchTable(const String & table_name, ContextPtr local_context, bool table_checked) const
{
    if (!duckdb_instance)
        duckdb_instance = openDuckDB(database_path, getContext(), /* throw_on_error */true);

    if (!table_checked && !checkDuckDBTable(table_name))
        return StoragePtr{};

    auto columns = fetchDuckDBTableStructure(*duckdb_instance, table_name);

    if (!columns)
        return StoragePtr{};

    auto storage = std::make_shared<StorageDuckDB>(
        StorageID(database_name, table_name),
        duckdb_instance,
        database_path,
        table_name,
        ColumnsDescription{*columns},
        ConstraintsDescription{},
        local_context);

    return storage;
}


ASTPtr DatabaseDuckDB::getCreateDatabaseQuery() const
{
    const auto & create_query = std::make_shared<ASTCreateQuery>();
    create_query->setDatabase(getDatabaseName());
    create_query->set(create_query->storage, database_engine_define);

    if (const auto comment_value = getDatabaseComment(); !comment_value.empty())
        create_query->set(create_query->comment, std::make_shared<ASTLiteral>(comment_value));

    return create_query;
}


ASTPtr DatabaseDuckDB::getCreateTableQueryImpl(const String & table_name, ContextPtr local_context, bool throw_on_error) const
{
    StoragePtr storage;
    {
        std::lock_guard<std::mutex> lock(mutex);
        storage = fetchTable(table_name, local_context, false);
    }
    if (!storage)
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "DuckDB table {}.{} does not exist",
                            getDatabaseName(), table_name);
        return nullptr;
    }
    auto table_storage_define = database_engine_define->clone();
    ASTStorage * ast_storage = table_storage_define->as<ASTStorage>();
    auto storage_engine_arguments = ast_storage->engine->arguments;
    auto table_id = storage->getStorageID();
    /// Add table_name to engine arguments
    storage_engine_arguments->children.insert(storage_engine_arguments->children.begin() + 1, std::make_shared<ASTLiteral>(table_id.table_name));

    unsigned max_parser_depth = static_cast<unsigned>(getContext()->getSettingsRef().max_parser_depth);
    auto create_table_query = DB::getCreateQueryFromStorage(storage, table_storage_define, true,
                                                            max_parser_depth,
                                                            throw_on_error);

    return create_table_query;
}

}

#endif
