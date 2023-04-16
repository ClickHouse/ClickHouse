#include <Databases/DatabaseFileSystem.h>

#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Storages/IStorage.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

DatabaseFileSystem::DatabaseFileSystem(const String & name_, const String & path_, ContextPtr context_)
    : IDatabase(name_), WithContext(context_->getGlobalContext()), path(path_), log(&Poco::Logger::get("DatabaseFileSystem(" + name_ + ")"))
{
    if (path.empty())
        path = Poco::Path::current();
}

std::string DatabaseFileSystem::getTablePath(const std::string& table_name) const
{
    return Poco::Path(path, table_name).toString();
}

void DatabaseFileSystem::addTable(const std::string& table_name, StoragePtr table_storage) const
{
    std::lock_guard lock(mutex);
    loaded_tables.emplace(table_name, table_storage);
}

bool DatabaseFileSystem::isTableExist(const String & name, ContextPtr) const
{
    {
        std::lock_guard lock(mutex);
        if (loaded_tables.find(name) != loaded_tables.end())
            return true;
    }

    Poco::File table_file(getTablePath(name));
    return table_file.exists() && table_file.isFile();
}

StoragePtr DatabaseFileSystem::tryGetTable(const String & name, ContextPtr context_) const
{
    // Check if the table exists in the loaded tables map
    {
        std::lock_guard lock(mutex);
        auto it = loaded_tables.find(name);
        if (it != loaded_tables.end())
            return it->second;
    }

    auto table_path = getTablePath(name);

    try
    {
        // If the table doesn't exist in the tables map, check if the corresponding file exists
        Poco::File table_file(table_path);
        if (!table_file.exists())
            return nullptr;

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
    catch (...)
    {
        return nullptr;
    }
}

ASTPtr DatabaseFileSystem::getCreateDatabaseQuery() const
{
    auto settings = getContext()->getSettingsRef();
    ParserCreateQuery parser;

    String query = "CREATE DATABASE " + backQuoteIfNeed(getDatabaseName()) + " ENGINE = FileSystem(" + backQuoteIfNeed(path) + ")";
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, settings.max_parser_depth);

    if (const auto database_comment = getDatabaseComment(); !database_comment.empty())
    {
        auto & ast_create_query = ast->as<ASTCreateQuery &>();
        ast_create_query.set(ast_create_query.comment, std::make_shared<ASTLiteral>(database_comment));
    }

    return ast;
}

void DatabaseFileSystem::shutdown()
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
 * Returns an empty vector because the database is read-only and no tables can be backed up.
 */
std::vector<std::pair<ASTPtr, StoragePtr>> DatabaseFileSystem::getTablesForBackup(const FilterByNameFunction&, const ContextPtr&) const
{
    return {};
}

/**
 *
 * Returns an empty iterator because the database does not have its own tables
 * But only caches them for quick access.
 */
DatabaseTablesIteratorPtr DatabaseFileSystem::getTablesIterator(ContextPtr, const FilterByNameFunction&) const
{
    return std::make_unique<DatabaseTablesSnapshotIterator>(Tables{}, getDatabaseName());
}

} // DB
