#include "config.h"

#if USE_AWS_S3

#include <Databases/DatabaseS3.h>

#include <IO/S3/URI.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>
#include <TableFunctions/TableFunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int BAD_ARGUMENTS;
    extern const int FILE_DOESNT_EXIST;
    extern const int UNACCEPTABLE_URL;
    extern const int S3_ERROR;
}

DatabaseS3::DatabaseS3(const String & name_, const String & key_id, const String & secret_key, ContextPtr context_)
    : IDatabase(name_)
    , WithContext(context_->getGlobalContext())
    , access_key_id(key_id)
    , secret_access_key(secret_key)
    , log(&Poco::Logger::get("DatabaseS3(" + name_ + ")"))
{
}

void DatabaseS3::addTable(const std::string & table_name, StoragePtr table_storage) const
{
    std::lock_guard lock(mutex);
    auto [_, inserted] = loaded_tables.emplace(table_name, table_storage);
    if (!inserted)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Table with name `{}` already exists in database `{}` (engine {})",
            table_name,
            getDatabaseName(),
            getEngineName());
}

bool DatabaseS3::checkUrl(const std::string & url, ContextPtr context_, bool throw_on_error) const
{
    try
    {
        S3::URI uri(url);
        context_->getGlobalContext()->getRemoteHostFilter().checkURL(uri.uri);
    }
    catch (...)
    {
        if (throw_on_error)
            throw;
        return false;
    }
    return true;
}

bool DatabaseS3::isTableExist(const String & name, ContextPtr context_) const
{
    std::lock_guard lock(mutex);
    if (loaded_tables.find(name) != loaded_tables.end())
        return true;

    return checkUrl(name, context_, false);
}

StoragePtr DatabaseS3::getTableImpl(const String & url, ContextPtr context_) const
{
    // Check if the table exists in the loaded tables map
    {
        std::lock_guard lock(mutex);
        auto it = loaded_tables.find(url);
        if (it != loaded_tables.end())
            return it->second;
    }

    checkUrl(url, context_, true);

    // call TableFunctionS3
    auto args = makeASTFunction(
        "s3",
        std::make_shared<ASTLiteral>(url),
        std::make_shared<ASTLiteral>(access_key_id),
        std::make_shared<ASTLiteral>(secret_access_key));

    auto table_function = TableFunctionFactory::instance().get(args, context_);
    if (!table_function)
        return nullptr;

    // TableFunctionS3 throws exceptions, if table cannot be created
    auto table_storage = table_function->execute(args, context_, url);
    if (table_storage)
        addTable(url, table_storage);

    return table_storage;
}

StoragePtr DatabaseS3::getTable(const String & name, ContextPtr context_) const
{
    // rethrow all exceptions from TableFunctionS3 to show correct error to user
    if (auto storage = getTableImpl(name, context_))
        return storage;
    throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist", backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(name));
}

StoragePtr DatabaseS3::tryGetTable(const String & name, ContextPtr context_) const
{
    try
    {
        return getTableImpl(name, context_);
    }
    catch (const Exception & e)
    {
        // Ignore exceptions thrown by TableFunctionS3, which indicate that there is no table
        if (e.code() == ErrorCodes::BAD_ARGUMENTS)
            return nullptr;
        if (e.code() == ErrorCodes::S3_ERROR)
            return nullptr;
        if (e.code() == ErrorCodes::FILE_DOESNT_EXIST)
            return nullptr;
        if (e.code() == ErrorCodes::UNACCEPTABLE_URL)
            return nullptr;
        throw;
    }
    catch (const Poco::URISyntaxException &)
    {
        return nullptr;
    }
}

ASTPtr DatabaseS3::getCreateDatabaseQuery() const
{
    auto settings = getContext()->getSettingsRef();
    ParserCreateQuery parser;

    const String query = fmt::format("CREATE DATABASE {} ENGINE = S3('{}', '{}')",
                                     backQuoteIfNeed(getDatabaseName()),
                                     access_key_id,
                                     secret_access_key);
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, settings.max_parser_depth);

    if (const auto database_comment = getDatabaseComment(); !database_comment.empty())
    {
        auto & ast_create_query = ast->as<ASTCreateQuery &>();
        ast_create_query.set(ast_create_query.comment, std::make_shared<ASTLiteral>(database_comment));
    }

    return ast;
}

void DatabaseS3::shutdown()
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
std::vector<std::pair<ASTPtr, StoragePtr>> DatabaseS3::getTablesForBackup(const FilterByNameFunction &, const ContextPtr &) const
{
    return {};
}

/**
 *
 * Returns an empty iterator because the database does not have its own tables
 * But only caches them for quick access
 */
DatabaseTablesIteratorPtr DatabaseS3::getTablesIterator(ContextPtr, const FilterByNameFunction &) const
{
    return std::make_unique<DatabaseTablesSnapshotIterator>(Tables{}, getDatabaseName());
}

} // DB

#endif
