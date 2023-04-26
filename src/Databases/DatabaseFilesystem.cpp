#include <Databases/DatabaseFilesystem.h>

#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
}

DatabaseFilesystem::DatabaseFilesystem(const String & name_, const String & path_, ContextPtr context_)
    : IDatabase(name_), WithContext(context_->getGlobalContext()), path(path_), log(&Poco::Logger::get("DatabaseFileSystem(" + name_ + ")"))
{
    path = fs::path(path).lexically_normal().string();
}

std::string DatabaseFilesystem::getTablePath(const std::string & table_name) const
{
    return fs::path(path) / table_name;
}

void DatabaseFilesystem::addTable(const std::string & table_name, StoragePtr table_storage) const
{
    std::lock_guard lock(mutex);
    loaded_tables.emplace(table_name, table_storage);
}

bool DatabaseFilesystem::isTableExist(const String & name, ContextPtr) const
{
    {
        std::lock_guard lock(mutex);
        if (loaded_tables.find(name) != loaded_tables.end())
            return true;
    }

    fs::path table_file_path(getTablePath(name));
    return fs::exists(table_file_path) && fs::is_regular_file(table_file_path);
}

StoragePtr DatabaseFilesystem::getTableImpl(const String & name, ContextPtr context_) const
{
    // Check if the table exists in the loaded tables map
    {
        std::lock_guard lock(mutex);
        auto it = loaded_tables.find(name);
        if (it != loaded_tables.end())
            return it->second;
    }

    auto table_path = getTablePath(name);

    // If the file exists, create a new table using TableFunctionFile and return it.
    auto args = makeASTFunction("file", std::make_shared<ASTLiteral>(table_path));

    auto table_function = TableFunctionFactory::instance().get(args, context_);
    if (!table_function)
        return nullptr;

    auto table_storage = table_function->execute(args, context_, name);
    if (table_storage)
        addTable(name, table_storage);

    return table_storage;
}

StoragePtr DatabaseFilesystem::getTable(const String & name, ContextPtr context_) const
{
    if (auto storage = getTableImpl(name, context_))
        return storage;
    throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist", backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(name));
}

StoragePtr DatabaseFilesystem::tryGetTable(const String & name, ContextPtr context_) const
{
    try
    {
        return getTable(name, context_);
    }
    catch (...)
    {
        return nullptr;
    }
}

ASTPtr DatabaseFilesystem::getCreateDatabaseQuery() const
{
    auto settings = getContext()->getSettingsRef();
    ParserCreateQuery parser;

    const String query = fmt::format("CREATE DATABASE {} ENGINE = Filesystem('{}')", backQuoteIfNeed(getDatabaseName()), path);
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, settings.max_parser_depth);

    if (const auto database_comment = getDatabaseComment(); !database_comment.empty())
    {
        auto & ast_create_query = ast->as<ASTCreateQuery &>();
        ast_create_query.set(ast_create_query.comment, std::make_shared<ASTLiteral>(database_comment));
    }

    return ast;
}

void DatabaseFilesystem::shutdown()
{
    Tables tables_snapshot;
    {
        std::lock_guard lock(mutex);
        tables_snapshot = loaded_tables;
    }

    for (const auto & kv : tables_snapshot)
    {
        auto table_id = kv.second->getStorageID();
        kv.second->flushAndShutdown();
    }

    std::lock_guard lock(mutex);
    loaded_tables.clear();
}

/**
 * Returns an empty vector because the database is read-only and no tables can be backed up
 */
std::vector<std::pair<ASTPtr, StoragePtr>> DatabaseFilesystem::getTablesForBackup(const FilterByNameFunction &, const ContextPtr &) const
{
    return {};
}

/**
 *
 * Returns an empty iterator because the database does not have its own tables
 * But only caches them for quick access
 */
DatabaseTablesIteratorPtr DatabaseFilesystem::getTablesIterator(ContextPtr, const FilterByNameFunction &) const
{
    return std::make_unique<DatabaseTablesSnapshotIterator>(Tables{}, getDatabaseName());
}

} // DB
