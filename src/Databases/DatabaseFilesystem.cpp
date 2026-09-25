#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseFilesystem.h>

#include <Access/ContextAccess.h>
#include <Access/Common/AccessFlags.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <IO/Archives/ArchiveUtils.h>
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
    extern const SettingsBool allow_archive_path_syntax;
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

namespace
{

/// The archive and the path inside it a name resolves to through the archive path syntax
/// (`archive.tar.zst::data.native`). The first part is empty if the name is a plain path in this
/// context, and the second one is then the name itself.
std::pair<String, String> splitToArchiveParts(const String & table_path, const ContextPtr & context)
{
    if (!context->getSettingsRef()[Setting::allow_archive_path_syntax])
        return {{}, table_path};

    return splitToArchivePathAndPathInArchive(table_path);
}

/// The path to the archive a name resolves to through the archive path syntax, or an empty string
/// if the name is a plain path in this context.
String getPathToArchive(const String & table_path, const ContextPtr & context)
{
    return splitToArchiveParts(table_path, context).first;
}

/// The file whose presence decides whether a table name resolves to a table. A name can use the
/// archive path syntax (`archive.tar.zst::data.native`), which the `file` table function resolves
/// to a file stored inside the archive; the file that has to exist on the filesystem is then the
/// archive, not the whole name.
String getPathToProbe(const String & table_path, const ContextPtr & context)
{
    String path_to_archive = getPathToArchive(table_path, context);
    return path_to_archive.empty() ? table_path : path_to_archive;
}

}

DatabaseFilesystem::DatabaseFilesystem(
    const String & name_, const String & path_, ContextPtr context_, bool is_internal_metadata_replay)
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
    {
        /// Metadata loading stops at the first exception, so refusing the server's own startup replay here
        /// would make a directory removed since then enough to stop the server from starting. Tables resolve
        /// their file on access, so an unreachable path costs only the tables; the database still drops.
        if (!is_internal_metadata_replay)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path does not exist: {}", path);

        LOG_WARNING(log, "Path does not exist: {}. The database has no tables until it reappears", path);
    }
}

std::string DatabaseFilesystem::getTablePath(const std::string & table_name) const
{
    fs::path table_path = fs::path(path) / table_name;
    return table_path.lexically_normal().string();
}

StoragePtr DatabaseFilesystem::addTable(const std::string & cache_key, StoragePtr table_storage) const
{
    std::lock_guard lock(mutex);
    /// `emplace` keeps the existing entry if the key is already there, so `first->second` is the storage
    /// a concurrent call for the same name inserted first. Nothing that locks `mutex` again may be called
    /// here: it is the non-recursive base `IDatabase::mutex`, shared with `getDatabaseName`.
    return loaded_tables.emplace(cache_key, table_storage).first->second;
}

bool DatabaseFilesystem::checkTableFilePath(const std::string & table_path, ContextPtr context_, bool throw_on_error) const
{
    /// If run in Local mode, no need for path checking.
    bool check_path = context_->getApplicationType() != Context::ApplicationType::LOCAL;
    const auto & user_files_path = context_->getUserFilesPath();

    const auto [path_to_archive, path_in_archive] = splitToArchiveParts(table_path, context_);
    const String path_to_probe = path_to_archive.empty() ? table_path : path_to_archive;

    /// Check access for file before checking its existence.
    if (check_path && !fileOrSymlinkPathStartsWith(path_to_probe, user_files_path))
    {
        /// Access denied is thrown regardless of 'throw_on_error'
        throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "File is not inside {}", user_files_path);
    }

    if (!containsGlobs(path_to_probe))
    {
        /// Check if the corresponding file exists.
        if (!existsOrFileNameTooLong([&] { return fs::exists(path_to_probe); }))
        {
            if (throw_on_error)
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File does not exist: {}", path_to_probe);
            return false;
        }

        if (!existsOrFileNameTooLong([&] { return fs::is_regular_file(path_to_probe); }))
        {
            if (throw_on_error)
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File is directory, but expected a file: {}", path_to_probe);
            return false;
        }

        /// The name addresses a file stored inside the archive, so the table exists only if that file
        /// does. Answering with the presence of the archive alone would claim a table for a member
        /// that is not there, and its resolution would fail with a schema inference error instead of
        /// reporting the missing table, as this database does for a plain path.
        if (!path_to_archive.empty() && !archiveContainsFile(path_to_archive, path_in_archive))
        {
            if (throw_on_error)
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST,
                                "File does not exist inside the archive {}: {}", path_to_archive, path_in_archive);
            return false;
        }
    }

    return true;
}

std::string DatabaseFilesystem::getCacheKey(const std::string & table_name, const ContextPtr & context_) const
{
    /// `allow_archive_path_syntax` is part of the resolution of a name: with it enabled,
    /// `a.tar::x.csv` is the file `x.csv` inside the archive `a.tar`, and with it disabled the same
    /// name is a file called `a.tar::x.csv`. Both files can exist at once, and the existence probe
    /// succeeds in both modes, so the two interpretations must not share a cache entry, or a query
    /// would read the source the previous query resolved. A file name cannot contain a zero byte,
    /// so the marker cannot collide with the name of another table.
    if (getPathToArchive(getTablePath(table_name), context_).empty())
        return table_name;

    return table_name + String(1, '\0') + "archive";
}

StoragePtr DatabaseFilesystem::tryGetTableFromCache(const std::string & name, const ContextPtr & context_) const
{
    const std::string key = getCacheKey(name, context_);

    StoragePtr table = nullptr;
    {
        std::lock_guard lock(mutex);
        auto it = loaded_tables.find(key);
        if (it != loaded_tables.end())
            table = it->second;
    }

    /// Invalidate cache if file no longer exists.
    if (table && !existsOrFileNameTooLong([&] { return fs::exists(getPathToProbe(getTablePath(name), context_)); }))
    {
        std::lock_guard lock(mutex);
        loaded_tables.erase(key);
        return nullptr;
    }

    return table;
}

bool DatabaseFilesystem::isTableExist(const String & name, ContextPtr context_) const
{
    /// `EXISTS TABLE` requires only `SHOW TABLES`, so answering it without the read source grant turns
    /// this database into an oracle for `user_files`. Claim the table: resolving it reports the denial.
    if (!context_->getAccess()->isGrantedWithFilter(AccessType::READ, toStringSource(AccessTypeObjects::Source::FILE), /* filter */ ""))
        return true;

    if (tryGetTableFromCache(name, context_))
        return true;

    return checkTableFilePath(getTablePath(name), context_, /* throw_on_error */ false);
}

StoragePtr DatabaseFilesystem::getTableImpl(const String & name, ContextPtr context_, bool throw_on_error) const
{
    /// Resolving a table of this database requires the read source grant. It is checked here, above the
    /// cache, because the cache is keyed on the table name alone: an entry resolved by one user is
    /// handed to every later caller. `file` reports no URI, so the grant is checked with no filter.
    context_->getAccess()->checkAccessWithFilter(AccessType::READ, toStringSource(AccessTypeObjects::Source::FILE), /* filter */ "");

    /// A renaming rule belongs to the one query that set it, while a cached table is shared with the
    /// later queries of every user. Such a table is therefore neither taken from the cache, where it
    /// would arrive without the rule, nor put into it, where it would rename for an unrelated query.
    const bool renames_after_processing
        = !context_->getSettingsRef()[Setting::rename_files_after_processing].value.empty();

    /// Check if table exists in loaded tables map.
    if (!renames_after_processing)
    {
        if (auto table = tryGetTableFromCache(name, context_))
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
        return addTable(getCacheKey(name, context_), table_storage);

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

        /// The loader flag, not `internal`, is the discriminator: an internal query is not necessarily the
        /// server's own replay, because wrappers run user statements as internal ones.
        const bool is_internal_metadata_replay
            = args.is_metadata_replay && args.mode >= LoadingStrictnessLevel::ATTACH;

        return std::make_shared<DatabaseFilesystem>(args.database_name, init_path, args.context, is_internal_metadata_replay);
    };
    factory.registerDatabase("Filesystem", create_fn, {
        .supports_arguments = true,
        .is_external = true,
        .source_access_type = AccessTypeObjects::Source::FILE,
    }, Documentation{
        .description = R"DOCS_MD(
The `Filesystem` database engine exposes files in a local directory as read-only tables. A table name is resolved as a path relative to the database directory and is read using the [`file`](/reference/functions/table-functions/file) table function.

## Creating a database {#creating-a-database}

```sql
CREATE DATABASE files
ENGINE = Filesystem([path]);
```

`path` is the directory that contains the files. If it is omitted, ClickHouse uses the current directory in `clickhouse-local` and the `user_files` directory in ClickHouse server.

## Usage {#usage}

For example, with `data.csv` in the selected directory:

```sql
CREATE DATABASE files ENGINE = Filesystem('imports');

SELECT * FROM files.`data.csv`;
```

The table name can include a relative path beneath the database directory. The table schema and format are inferred in the same way as for the `file` table function.

The database owns no table definitions: tables are created when their files are first resolved and are only cached for subsequent access. `CREATE TABLE`, `INSERT`, and other writes through this database are not supported.

## Access control {#access-control}

On ClickHouse server, the database directory and every resolved file must be inside [`user_files_path`](/reference/settings/server-settings/settings#user_files_path); this restriction also applies after following symlinks. `clickhouse-local` is not restricted to `user_files_path`.

Creating this database requires `READ` and `WRITE` source grants on `FILE`, regardless of [`table_engines_require_grant`](/reference/settings/server-settings/settings/other#table_engines_require_grant). Grant them with, for example:

```sql
GRANT READ, WRITE ON FILE TO user_name;
```

See the [`SOURCES` privileges](/reference/statements/grant#sources) for version and compatibility details.

## See also {#see-also}

- [`file` table function](/reference/functions/table-functions/file)
- [S3 database engine](/reference/engines/database-engines/s3)
- [HDFS database engine](/reference/engines/database-engines/hdfs)
)DOCS_MD",
        .syntax = "ENGINE = Filesystem([path])",
        .related = {"S3", "HDFS"}});
}
}
