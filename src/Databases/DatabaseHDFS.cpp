#include "config.h"

#if USE_HDFS

#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseHDFS.h>

#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Storages/ObjectStorage/HDFS/HDFSCommon.h>
#include <Storages/IStorage.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/re2.h>
#include <Core/Settings.h>

#include <Poco/URI.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int BAD_ARGUMENTS;
    extern const int FILE_DOESNT_EXIST;
    extern const int UNACCEPTABLE_URL;
    extern const int ACCESS_DENIED;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int HDFS_ERROR;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
}

static constexpr std::string_view HDFS_HOST_REGEXP = "^hdfs://[^/]*";


DatabaseHDFS::DatabaseHDFS(const String & name_, const String & source_url, ContextPtr context_)
    : IDatabase(name_)
    , WithContext(context_->getGlobalContext())
    , source(source_url)
    , log(getLogger("DatabaseHDFS(" + name_ + ")"))
{
    if (!source.empty())
    {
        if (!re2::RE2::FullMatch(source, std::string(HDFS_HOST_REGEXP)))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad HDFS host: {}. "
                            "It should have structure 'hdfs://<host_name>:<port>'", source);

        context_->getGlobalContext()->getRemoteHostFilter().checkURL(Poco::URI(source));
    }
}

void DatabaseHDFS::addTable(const std::string & table_name, StoragePtr table_storage) const
{
    std::lock_guard lock(mutex);
    auto [_, inserted] = loaded_tables.emplace(table_name, table_storage);
    if (!inserted)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Table with name `{}` already exists in database `{}` (engine {})",
            table_name, getDatabaseName(), getEngineName());
}

std::string DatabaseHDFS::getTablePath(const std::string & table_name) const
{
    if (table_name.starts_with("hdfs://"))
        return table_name;

    if (source.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad HDFS URL: {}. "
            "It should have the following structure 'hdfs://<host_name>:<port>/path'", table_name);

    return fs::path(source) / table_name;
}

bool DatabaseHDFS::checkUrl(const std::string & url, ContextPtr context_, bool throw_on_error) const
{
    try
    {
        checkHDFSURL(url);
        context_->getGlobalContext()->getRemoteHostFilter().checkURL(Poco::URI(url));
    }
    catch (...)
    {
        if (throw_on_error)
            throw;
        return false;
    }

    return true;
}

bool DatabaseHDFS::isTableExist(const String & name, ContextPtr context_) const
{
    std::lock_guard lock(mutex);
    if (loaded_tables.find(name) != loaded_tables.end())
        return true;

    return checkUrl(name, context_, false);
}

StoragePtr DatabaseHDFS::getTableImpl(const String & name, ContextPtr context_) const
{
    /// Check if the table exists in the loaded tables map.
    {
        std::lock_guard lock(mutex);
        auto it = loaded_tables.find(name);
        if (it != loaded_tables.end())
            return it->second;
    }

    auto url = getTablePath(name);

    checkUrl(url, context_, true);

    auto args = makeASTFunction("hdfs", std::make_shared<ASTLiteral>(url));

    auto table_function = TableFunctionFactory::instance().get(args, context_);
    if (!table_function)
        return nullptr;

    /// TableFunctionHDFS throws exceptions, if table cannot be created.
    auto table_storage = table_function->execute(args, context_, name);
    if (table_storage)
        addTable(name, table_storage);

    return table_storage;
}

StoragePtr DatabaseHDFS::getTable(const String & name, ContextPtr context_) const
{
    /// Rethrow all exceptions from TableFunctionHDFS to show correct error to user.
    if (auto storage = getTableImpl(name, context_))
        return storage;

    throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist",
                    backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(name));
}

StoragePtr DatabaseHDFS::tryGetTable(const String & name, ContextPtr context_) const
{
    try
    {
        return getTableImpl(name, context_);
    }
    catch (const Exception & e)
    {
        // Ignore exceptions thrown by TableFunctionHDFS, which indicate that there is no table
        if (e.code() == ErrorCodes::BAD_ARGUMENTS
            || e.code() == ErrorCodes::ACCESS_DENIED
            || e.code() == ErrorCodes::DATABASE_ACCESS_DENIED
            || e.code() == ErrorCodes::FILE_DOESNT_EXIST
            || e.code() == ErrorCodes::UNACCEPTABLE_URL
            || e.code() == ErrorCodes::HDFS_ERROR
            || e.code() == ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE)
        {
            return nullptr;
        }
        throw;
    }
    catch (const Poco::URISyntaxException &)
    {
        return nullptr;
    }
}

bool DatabaseHDFS::empty() const
{
    std::lock_guard lock(mutex);
    return loaded_tables.empty();
}

ASTPtr DatabaseHDFS::getCreateDatabaseQuery() const
{
    const auto & settings = getContext()->getSettingsRef();
    ParserCreateQuery parser;

    const String query = fmt::format("CREATE DATABASE {} ENGINE = HDFS('{}')", backQuoteIfNeed(getDatabaseName()), source);
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, settings.max_parser_depth, settings.max_parser_backtracks);

    if (const auto database_comment = getDatabaseComment(); !database_comment.empty())
    {
        auto & ast_create_query = ast->as<ASTCreateQuery &>();
        ast_create_query.set(ast_create_query.comment, std::make_shared<ASTLiteral>(database_comment));
    }

    return ast;
}

void DatabaseHDFS::shutdown()
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
std::vector<std::pair<ASTPtr, StoragePtr>> DatabaseHDFS::getTablesForBackup(const FilterByNameFunction &, const ContextPtr &) const
{
    return {};
}

/**
 *
 * Returns an empty iterator because the database does not have its own tables
 * But only caches them for quick access
 */
DatabaseTablesIteratorPtr DatabaseHDFS::getTablesIterator(ContextPtr, const FilterByNameFunction &, bool) const
{
    return std::make_unique<DatabaseTablesSnapshotIterator>(Tables{}, getDatabaseName());
}

void registerDatabaseHDFS(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        auto * engine_define = args.create_query.storage;
        const ASTFunction * engine = engine_define->engine;
        const String & engine_name = engine_define->engine->name;

        /// If source_url is empty, then table name must contain full url
        std::string source_url;

        if (engine->arguments && !engine->arguments->children.empty())
        {
            if (engine->arguments->children.size() != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "HDFS database requires at most 1 argument: source_url");

            const auto & arguments = engine->arguments->children;
            source_url = safeGetLiteralValue<String>(arguments[0], engine_name);
        }

        return std::make_shared<DatabaseHDFS>(args.database_name, source_url, args.context);
    };
    factory.registerDatabase("HDFS", create_fn);
}
} // DB

#endif
