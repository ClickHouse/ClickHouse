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
#include <Common/filesystemHelpers.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int BAD_ARGUMENTS;
    extern const int FILE_DOESNT_EXIST;
}

DatabaseFilesystem::DatabaseFilesystem(const String & name_, const String & path_, ContextPtr context_)
    : IDatabase(name_), WithContext(context_->getGlobalContext()), path(path_), log(&Poco::Logger::get("DatabaseFileSystem(" + name_ + ")"))
{
    fs::path user_files_path;
    const auto & application_type = context_->getApplicationType();

    if (application_type != Context::ApplicationType::LOCAL)
        user_files_path = fs::canonical(fs::path(getContext()->getUserFilesPath()));

    if (fs::path(path).is_relative())
        path = user_files_path / path;
    else if (application_type != Context::ApplicationType::LOCAL && !pathStartsWith(fs::path(path), user_files_path))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path must be inside user-files path ({})", user_files_path.string());

    path = fs::absolute(path).lexically_normal().string();

    if (!fs::exists(path))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path does not exist ({})", path);
}

std::string DatabaseFilesystem::getTablePath(const std::string & table_name) const
{
    fs::path table_path = fs::path(path) / table_name;
    return table_path.lexically_normal().string();
}

void DatabaseFilesystem::addTable(const std::string & table_name, StoragePtr table_storage) const
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

bool DatabaseFilesystem::checkTableFilePath(const std::string & table_path, ContextPtr context_, bool throw_on_error) const
{
    // If run in Local mode, no need for path checking.
    bool need_check_path = context_->getApplicationType() != Context::ApplicationType::LOCAL;
    std::string user_files_path = fs::canonical(fs::path(context_->getUserFilesPath())).string();

    // Check access for file before checking its existence
    if (need_check_path && !fileOrSymlinkPathStartsWith(table_path, user_files_path))
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "File is not inside {}", user_files_path);
        else
            return false;
    }

    // Check if the corresponding file exists
    if (!fs::exists(table_path) || !fs::is_regular_file(table_path))
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File does not exist ({})", table_path);
        else
            return false;
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

    // invalidate cache if file no longer exists
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

    fs::path table_file_path(getTablePath(name));

    return checkTableFilePath(table_file_path, context_, false);
}

StoragePtr DatabaseFilesystem::getTableImpl(const String & name, ContextPtr context_) const
{
    // Check if table exists in loaded tables map
    if (auto table = tryGetTableFromCache(name))
        return table;

    auto table_path = getTablePath(name);

    checkTableFilePath(table_path, context_, true);

    // If the file exists, create a new table using TableFunctionFile and return it.
    auto args = makeASTFunction("file", std::make_shared<ASTLiteral>(table_path));

    auto table_function = TableFunctionFactory::instance().get(args, context_);
    if (!table_function)
        return nullptr;

    // TableFunctionFile throws exceptions, if table cannot be created
    auto table_storage = table_function->execute(args, context_, name);
    if (table_storage)
        addTable(name, table_storage);

    return table_storage;
}

StoragePtr DatabaseFilesystem::getTable(const String & name, ContextPtr context_) const
{
    // rethrow all exceptions from TableFunctionFile to show correct error to user
    if (auto storage = getTableImpl(name, context_))
        return storage;
    throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist", backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(name));
}

StoragePtr DatabaseFilesystem::tryGetTable(const String & name, ContextPtr context_) const
{
    try
    {
        return getTableImpl(name, context_);
    }
    catch (const Exception & e)
    {
        // Ignore exceptions thrown by TableFunctionFile, which indicate that there is no table
        // see tests/02722_database_filesystem.sh for more details
        if (e.code() == ErrorCodes::BAD_ARGUMENTS)
            return nullptr;
        if (e.code() == ErrorCodes::DATABASE_ACCESS_DENIED)
            return nullptr;
        if (e.code() == ErrorCodes::FILE_DOESNT_EXIST)
            return nullptr;

        throw;
    }
}

bool DatabaseFilesystem::empty() const
{
    std::lock_guard lock(mutex);
    return loaded_tables.empty();
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
