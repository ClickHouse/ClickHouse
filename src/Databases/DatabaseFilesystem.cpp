#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseFilesystem.h>

#include <Common/Logger.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
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
#include <Common/filesystemHelpers.h>
#include <Formats/FormatFactory.h>

#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsString rename_files_after_processing;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int PATH_ACCESS_DENIED;
    extern const int BAD_ARGUMENTS;
    extern const int FILE_DOESNT_EXIST;
}

DatabaseFilesystem::DatabaseFilesystem(const String & name_, const String & path_, ContextPtr context_)
    : IDatabase(name_), WithContext(context_->getGlobalContext()), path(path_), log(getLogger("DatabaseFileSystem(" + name_ + ")"))
{
    bool is_local = context_->getApplicationType() == Context::ApplicationType::LOCAL;
    fs::path user_files_path = is_local ? "" : fs::canonical(getContext()->getUserFilesPath());

    if (fs::path(path).is_relative())
    {
        path = user_files_path / path;
    }

    path = fs::absolute(path).lexically_normal();

    if (!is_local && !pathStartsWith(fs::path(path), user_files_path))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Path must be inside user-files path: {}", user_files_path.string());
    }

    if (!fs::exists(path))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path does not exist: {}", path);
}

std::string DatabaseFilesystem::getTablePath(const std::string & table_name) const
{
    fs::path table_path = fs::path(path) / table_name;
    return table_path.lexically_normal().string();
}

StoragePtr DatabaseFilesystem::addTable(const std::string & table_name, StoragePtr table_storage) const
{
    std::lock_guard lock(mutex);
    /// `emplace` keeps the existing entry if the key is already there, so `first->second` is the storage
    /// a concurrent call for the same name inserted first. Nothing that locks `mutex` again may be called
    /// here: it is the non-recursive base `IDatabase::mutex`, shared with `getDatabaseName`.
    return loaded_tables.emplace(table_name, table_storage).first->second;
}

bool DatabaseFilesystem::checkTableFilePath(const std::string & table_path, ContextPtr context_, bool throw_on_error) const
{
    /// If run in Local mode, no need for path checking.
    bool check_path = context_->getApplicationType() != Context::ApplicationType::LOCAL;
    const auto & user_files_path = context_->getUserFilesPath();

    /// Check access for file before checking its existence.
    if (check_path && !fileOrSymlinkPathStartsWith(table_path, user_files_path))
    {
        /// Access denied is thrown regardless of 'throw_on_error'
        throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "File is not inside {}", user_files_path);
    }

    if (!containsGlobs(table_path))
    {
        /// Check if the corresponding file exists.
        if (!fs::exists(table_path))
        {
            if (throw_on_error)
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File does not exist: {}", table_path);
            return false;
        }

        if (!fs::is_regular_file(table_path))
        {
            if (throw_on_error)
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File is directory, but expected a file: {}", table_path);
            return false;
        }
    }

    return true;
}

StoragePtr DatabaseFilesystem::tryGetTableFromCache(const std::string & name) const
{
    StoragePtr table = nullptr;
    {
        std::lock_guard lock(mutex);
        auto it = loaded_tables.find(name);
        if (it != loaded_tables.end())
            table = it->second;
    }

    /// Invalidate cache if file no longer exists.
    if (table && !fs::exists(getTablePath(name)))
    {
        std::lock_guard lock(mutex);
        loaded_tables.erase(name);
        return nullptr;
    }

    return table;
}

bool DatabaseFilesystem::isTableExist(const String & name, ContextPtr context_) const
{
    if (tryGetTableFromCache(name))
        return true;

    return checkTableFilePath(getTablePath(name), context_, /* throw_on_error */ false);
}

StoragePtr DatabaseFilesystem::getTableImpl(const String & name, ContextPtr context_, bool throw_on_error) const
{
    /// A renaming rule belongs to the one query that set it, while a cached table is shared with the
    /// later queries of every user. Such a table is therefore neither taken from the cache, where it
    /// would arrive without the rule, nor put into it, where it would rename for an unrelated query.
    const bool renames_after_processing
        = !context_->getSettingsRef()[Setting::rename_files_after_processing].value.empty();

    /// Check if table exists in loaded tables map.
    if (!renames_after_processing)
    {
        if (auto table = tryGetTableFromCache(name))
            return table;
    }

    auto table_path = getTablePath(name);
    if (!checkTableFilePath(table_path, context_, throw_on_error))
        return {};

    auto ast_function_ptr = makeASTFunction("file", make_intrusive<ASTLiteral>(table_path));

    auto table_function = TableFunctionFactory::instance().get(ast_function_ptr, context_);
    if (!table_function)
        return nullptr;

    /// Every reader of a file in one query shares the counter that decides when the rename happens, so
    /// such a table is memoised for that query, the way the `file` table function is.
    if (renames_after_processing && context_->hasQueryContext())
    {
        auto query_context = context_->getQueryContext();
        /// The memo builds the table with the context it is handed and keys on that context's changed
        /// settings, so the query's context is what makes the references of one query share a table.
        /// A rule a sub-query set locally is not among the query's settings, so resolving it there
        /// would build a table that renames by another rule, or does not rename at all.
        const bool rule_is_the_query_setting
            = query_context->getSettingsRef()[Setting::rename_files_after_processing].value
            == context_->getSettingsRef()[Setting::rename_files_after_processing].value;

        return query_context->executeTableFunction(
            ast_function_ptr, table_function, rule_is_the_query_setting ? ContextPtr(query_context) : context_);
    }

    /// TableFunctionFile throws exceptions, if table cannot be created.
    auto table_storage = table_function->execute(ast_function_ptr, context_, name);
    if (table_storage && !renames_after_processing)
        return addTable(name, table_storage);

    return table_storage;
}

StoragePtr DatabaseFilesystem::getTable(const String & name, ContextPtr context_) const
{
    /// getTableImpl can throw exceptions, do not catch them to show correct error to user.
    if (auto storage = getTableImpl(name, context_, true))
        return storage;

    throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist",
                    backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(name));
}

StoragePtr DatabaseFilesystem::tryGetTable(const String & name, ContextPtr context_) const
{
    return getTableImpl(name, context_, false);
}

bool DatabaseFilesystem::empty() const
{
    std::lock_guard lock(mutex);
    return loaded_tables.empty();
}

ASTPtr DatabaseFilesystem::getCreateDatabaseQueryImpl() const
{
    const auto & settings = getContext()->getSettingsRef();
    const String query = fmt::format("CREATE DATABASE {} ENGINE = Filesystem('{}')", backQuoteIfNeed(database_name), path);

    ParserCreateQuery parser;
    ASTPtr ast
        = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);

    if (!comment.empty())
    {
        auto & ast_create_query = ast->as<ASTCreateQuery &>();
        ast_create_query.set(ast_create_query.comment, make_intrusive<ASTLiteral>(comment));
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
DatabaseTablesIteratorPtr DatabaseFilesystem::getTablesIterator(ContextPtr, const FilterByNameFunction &, bool) const
{
    return std::make_unique<DatabaseTablesSnapshotIterator>(Tables{}, getDatabaseName());
}

void registerDatabaseFilesystem(DatabaseFactory & factory);
void registerDatabaseFilesystem(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        auto * engine_define = args.create_query.storage;
        const ASTFunction * engine = engine_define->engine;
        const String & engine_name = engine_define->engine->name;

        /// If init_path is empty, then the current path will be used
        std::string init_path;

        if (engine->arguments && !engine->arguments->children.empty())
        {
            if (engine->arguments->children.size() != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Filesystem database requires at most 1 argument: filesystem_path");

            const auto & arguments = engine->arguments->children;
            init_path = safeGetLiteralValue<String>(arguments[0], engine_name);
        }

        return std::make_shared<DatabaseFilesystem>(args.database_name, init_path, args.context);
    };
    factory.registerDatabase("Filesystem", create_fn, {
        .supports_arguments = true,
        .is_external = true,
        .source_access_type = AccessTypeObjects::Source::FILE,
    }, Documentation{
        .description = "A read-only database that exposes files in a directory on the local filesystem as tables, queryable by their path.",
        .syntax = "ENGINE = Filesystem([path])",
        .related = {"S3", "HDFS"}});
}
}
