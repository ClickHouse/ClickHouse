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
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/Logger.h>
#include <Common/quoteString.h>
#include <Common/re2.h>
#include <Common/RemoteHostFilter.h>
#include <Core/Settings.h>

#include <Poco/URI.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
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
    }
}

StoragePtr DatabaseHDFS::addTable(const std::string & table_name, StoragePtr table_storage) const
{
    std::lock_guard lock(mutex);
    /// `emplace` keeps the existing entry if the key is already there, so `first->second` is the storage
    /// a concurrent call for the same name inserted first. Nothing that locks `mutex` again may be called
    /// here: it is the non-recursive base `IDatabase::mutex`, shared with `getDatabaseName`.
    return loaded_tables.emplace(table_name, table_storage).first->second;
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
    /// A name exists when it forms a URL this database may use, which needs no HDFS request. The cache
    /// must not answer it: that reports which names other callers resolved, past any filter tightening.
    if (source.empty() && !name.starts_with("hdfs://"))
        return false;

    return checkUrl(getTablePath(name), context_, false);
}

StoragePtr DatabaseHDFS::getTableImpl(const String & name, ContextPtr context_) const
{
    auto url = getTablePath(name);
    auto args = makeASTFunction("hdfs", make_intrusive<ASTLiteral>(url));

    auto table_function = TableFunctionFactory::instance().get(args, context_);
    if (!table_function)
        return nullptr;

    /// The cache is keyed on the name alone, so what authorizes a resolution is checked above it. The
    /// grant is the table function's to check: a filtered grant matches the URI it reports, not the path.
    table_function->checkSourceAccess(context_, /* is_insert_query */ false);

    checkUrl(url, context_, true);

    /// Check if the table exists in the loaded tables map.
    {
        std::lock_guard lock(mutex);
        auto it = loaded_tables.find(name);
        if (it != loaded_tables.end())
            return it->second;
    }

    /// TableFunctionHDFS throws exceptions, if table cannot be created.
    auto table_storage = table_function->execute(args, context_, name);
    if (table_storage)
        return addTable(name, table_storage);

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

ASTPtr DatabaseHDFS::getCreateDatabaseQueryImpl() const
{
    const auto & settings = getContext()->getSettingsRef();
    ParserCreateQuery parser;

    const String query = fmt::format("CREATE DATABASE {} ENGINE = HDFS('{}')", backQuoteIfNeed(database_name), source);
    ASTPtr ast
        = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);

    if (!comment.empty())
    {
        auto & ast_create_query = ast->as<ASTCreateQuery &>();
        ast_create_query.set(ast_create_query.comment, make_intrusive<ASTLiteral>(comment));
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

void registerDatabaseHDFS(DatabaseFactory & factory);
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

        /** The allowlist is checked here rather than in the constructor, and not for the server's own
          * metadata replay. Startup rebuilds every database by replaying its stored `ATTACH DATABASE`
          * statement and `loadMetadata` aborts on the first exception, so a check that throws there
          * takes the whole server down with it - and tightening `remote_url_allow_hosts` is exactly
          * what turns a stored host into a disallowed one. Every statement a user writes, `CREATE` and
          * `ATTACH` alike, is still checked up front, and the allowlist holds for every use of the
          * database regardless: `DatabaseHDFS::checkUrl` runs it for each table, which is where
          * `DatabaseS3` enforces it too.
          */
        if (!args.internal && !source_url.empty())
            args.context->getGlobalContext()->getRemoteHostFilter().checkURL(Poco::URI(source_url));

        return std::make_shared<DatabaseHDFS>(args.database_name, source_url, args.context);
    };
    factory.registerDatabase("HDFS", create_fn, {
        .supports_arguments = true,
        .is_external = true,
        .source_access_type = AccessTypeObjects::Source::HDFS,
    }, Documentation{
        .description = R"DOCS_MD(
The `HDFS` database engine exposes files in HDFS as read-only tables. A table name is resolved through the [`hdfs`](/reference/functions/table-functions/hdfs) table function.

## Creating a database {#creating-a-database}

```sql
CREATE DATABASE hdfs_data
ENGINE = HDFS([hdfs_host_and_root_path]);
```

`hdfs_host_and_root_path` optionally sets a base HDFS URL. When it is present, table names are paths relative to that URL. Without it, table names must be full `hdfs://` URLs.

## Usage {#usage}

```sql
CREATE DATABASE hdfs_data
ENGINE = HDFS('hdfs://namenode:9000/data');

SELECT * FROM hdfs_data.`events.parquet`;
```

The schema and format are inferred in the same way as for the `hdfs` table function. The database owns no table definitions and does not support table DDL or writes.

## Access control {#access-control}

HDFS URLs are checked against the server's remote-host filter. Creating this database requires `READ` and `WRITE` source grants on `HDFS`, regardless of [`table_engines_require_grant`](/reference/settings/server-settings/settings/other#table_engines_require_grant), for example:

```sql
GRANT READ, WRITE ON HDFS TO user_name;
```

See the [`SOURCES` privileges](/reference/statements/grant#sources) for version and compatibility details.

## See also {#see-also}

- [`hdfs` table function](/reference/functions/table-functions/hdfs)
- [Filesystem database engine](/reference/engines/database-engines/filesystem)
- [S3 database engine](/reference/engines/database-engines/s3)
)DOCS_MD",
        .syntax = "ENGINE = HDFS([hdfs_host_and_root_path])",
        .related = {"S3", "Filesystem"}});
}
} // DB

#endif
