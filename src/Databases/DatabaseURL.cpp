#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseURL.h>
#include <Databases/TableFunctionDatabaseTable.h>

#include <Access/ContextAccess.h>
#include <Access/Common/AccessFlags.h>
#include <Core/Settings.h>
#include <IO/Archives/ArchiveUtils.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageURL.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/StringUtils.h>
#include <Common/filesystemHelpers.h>
#include <Common/maskURIPassword.h>
#include <Common/quoteString.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_archive_path_syntax;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FILE_DOESNT_EXIST;
    extern const int UNKNOWN_TABLE;
}

namespace
{

/// Whether the user is allowed to read local files, i.e. whether the resolution of a `file://`
/// table would pass the read source access check of its `file` delegate. The decision is derived
/// directly from the grants, without constructing the delegate: already the parsing of the
/// arguments of the `file` table function observes the filesystem (`StorageFile::FileSource::parse`
/// enumerates the matching paths), which must not happen before the grant is confirmed. The `file`
/// delegate reports an empty URI for source-access filtering (see
/// `TableFunctionURL::parseArgumentsImpl`), so a filtered grant applies iff its regexp matches the
/// empty string — replicated here by filtering against the empty string as well.
bool isFileReadGranted(ContextPtr context)
{
    return context->getAccess()->isGrantedWithFilter(AccessType::READ, toStringSource(AccessTypeObjects::Source::FILE), /* filter */ "");
}

/// Resolve a table of a `URL` database into the delegate storage of the `url` table function.
///
/// `is_insert_query` selects the mode the delegate is constructed in, and the source access is
/// checked in the same mode (READ for a read, WRITE for a write). The mode is not an optimization:
/// the backends behave differently for reads and writes — `azureBlobStorage` creates the container
/// only in write mode — so a storage constructed in read mode must not be written to.
TableFunctionDelegate makeURLTableDelegate(const String & url, const String & name, ContextPtr context, bool is_insert_query)
{
    auto ast_function_ptr = makeASTFunction("url", make_intrusive<ASTLiteral>(url));

    /// The table is referenced in the query by an identifier (`db.table`), which cannot be
    /// rewritten into a `urlCluster(...)` table function call for sending to other replicas.
    /// Disable the parallel-replicas auto-conversion to cluster storages: otherwise the `url`
    /// table function would create `StorageURLCluster`, whose `updateQueryToSendIfNeeded`
    /// requires a table function in the query and throws a logical error for identifiers.
    ContextMutablePtr context_copy = Context::createCopy(context);
    context_copy->setSetting("parallel_replicas_for_cluster_engines", Field(false));

    auto table_function = TableFunctionFactory::instance().get(ast_function_ptr, context_copy);
    if (!table_function)
        return {};

    /// The `url` table function throws if a table cannot be created from the URL (for example, the
    /// resource is unreachable). Such errors are not swallowed as "table does not exist" even from
    /// `tryGetTable`, because they are more informative. The tables are intentionally not cached:
    /// the remote data (and the inferred schema) can change between queries.
    ///
    /// The table is referenced by an identifier and governed by the grants on this database and
    /// the source access, so the `CREATE TEMPORARY TABLE` privilege required for a table function
    /// call written in a query does not apply here.
    auto storage = table_function->execute(
        ast_function_ptr,
        context_copy,
        name,
        /* cached_columns_ */ {},
        /* use_global_context */ false,
        is_insert_query,
        /* check_create_temporary_table */ false);

    return {std::move(table_function), std::move(storage)};
}

}

DatabaseURL::DatabaseURL(const String & name_, const String & base_url_, ContextPtr context_)
    : IDatabase(name_), WithContext(context_->getGlobalContext()), base_url(base_url_)
{
    /// Not echoed back: password masking anchors on the `://` this value lacks, so it would log the password.
    if (!base_url.empty() && findURIAuthority(base_url) == String::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "The base URL of a URL database must contain a scheme (e.g. https://)");
}

String DatabaseURL::getTableURL(const String & name) const
{
    String resolved = StorageURL::resolveURLBase(name, base_url, "base URL of the URL database");

    if (findURIAuthority(resolved) == String::npos)
        return {};
    return resolved;
}

bool DatabaseURL::checkFileURLExists(const String & url, ContextPtr context_, bool throw_on_error) const
{
    if (classifyURLScheme(url) != URLSchemeTarget::File)
        return true;

    /// A table name can use the archive path syntax (`archive.tar.zst::data.native`), which the
    /// `file` delegate resolves to a file stored inside the archive. The file that has to exist on
    /// the filesystem is then the archive, not the whole name: probing the name itself would make
    /// every table inside an archive unresolvable.
    String local_path = getLocalPathFromFileURL(url);
    String path_in_archive;
    if (context_->getSettingsRef()[Setting::allow_archive_path_syntax])
    {
        if (auto [path_to_archive, path_inside] = splitToArchivePathAndPathInArchive(local_path); !path_to_archive.empty())
        {
            local_path = std::move(path_to_archive);
            path_in_archive = std::move(path_inside);
        }
    }

    fs::path fs_path(local_path);
    if (fs_path.is_relative())
        fs_path = fs::path(context_->getUserFilesPath()) / fs_path;
    const String path = fs::absolute(fs_path).lexically_normal().string();

    /// A path with globs matches a dynamic set of files, possibly empty, so it always "exists".
    if (containsGlobs(path))
        return true;

    /// Outside clickhouse-local, do not probe paths outside of user_files: instead of leaking
    /// whether such a file exists through the error message, claim the table and let the `file`
    /// engine report the access error.
    const bool is_local = getContext()->getApplicationType() == Context::ApplicationType::LOCAL;
    if (!is_local && !fileOrSymlinkPathStartsWith(path, context_->getUserFilesPath()))
        return true;

    /// The result of this probe is reported to the user (`EXISTS TABLE` returns it, and the
    /// resolution of a table reports `FILE_DOESNT_EXIST` instead of `ACCESS_DENIED`), so it must
    /// not be performed without the read source grant: `EXISTS TABLE` requires only `SHOW TABLES`,
    /// which would otherwise turn a `URL` database into an oracle for the contents of `user_files`.
    /// Claim the table, as above: a resolution of it fails with the access error, and `EXISTS`
    /// answers what it answers for a remote URL, which is not probed either.
    if (!isFileReadGranted(context_))
        return true;

    if (!existsOrFileNameTooLong([&] { return fs::exists(path); }))
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File does not exist: {}", path);
        return false;
    }

    if (!existsOrFileNameTooLong([&] { return fs::is_regular_file(path); }))
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File is directory, but expected a file: {}", path);
        return false;
    }

    /// The name addresses a file stored inside the archive, so the table exists only if that file
    /// does. Answering with the presence of the archive alone would claim a table for a member that
    /// is not there, and its resolution would fail with a schema inference error instead of
    /// reporting the missing table, as this database does for a plain path.
    if (!path_in_archive.empty() && !archiveContainsFile(path, path_in_archive))
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File does not exist inside the archive {}: {}", path, path_in_archive);
        return false;
    }

    return true;
}

StoragePtr DatabaseURL::getTableImpl(const String & name, ContextPtr context_, bool throw_on_error) const
{
    const String url = getTableURL(name);
    if (url.empty())
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist: the table name is not a URL",
                            backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(name));
        return {};
    }

    if (!checkFileURLExists(url, context_, throw_on_error))
        return {};

    /// The delegate of a `file://` table must not be constructed without the read source grant
    /// either: already the parsing of the arguments of the `file` table function observes the
    /// filesystem (see `isFileReadGranted`), and the errors of that parsing (e.g. that the path is
    /// outside of `user_files`) are reported before the source access check of the delegate runs.
    /// Fail here with the same access error that check would have produced after the parsing.
    if (classifyURLScheme(url) == URLSchemeTarget::File)
        context_->getAccess()->checkAccessWithFilter(AccessType::READ, toStringSource(AccessTypeObjects::Source::FILE), /* filter */ "");

    return makeTableFunctionDatabaseTable(
        StorageID(getDatabaseName(), name),
        [url, name](const ContextPtr & context, bool is_insert_query) { return makeURLTableDelegate(url, name, context, is_insert_query); },
        context_);
}

bool DatabaseURL::isTableExist(const String & name, ContextPtr context_) const
{
    /// For remote URLs this is a syntactic check only: verifying the existence of the remote
    /// resource would require a network request, which is too heavy (and has side effects)
    /// for this method. For `file://` URLs the existence of the file is checked.
    const String url = getTableURL(name);
    return !url.empty() && checkFileURLExists(url, context_, /* throw_on_error */ false);
}

StoragePtr DatabaseURL::getTable(const String & name, ContextPtr context_) const
{
    /// getTableImpl can throw exceptions, do not catch them to show correct error to user.
    if (auto storage = getTableImpl(name, context_, true))
        return storage;

    throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist",
                    backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(name));
}

StoragePtr DatabaseURL::tryGetTable(const String & name, ContextPtr context_) const
{
    return getTableImpl(name, context_, false);
}

ASTPtr DatabaseURL::getCreateDatabaseQueryImpl() const
{
    const auto & settings = getContext()->getSettingsRef();
    const String query = base_url.empty()
        ? fmt::format("CREATE DATABASE {} ENGINE = URL", backQuoteIfNeed(database_name))
        : fmt::format("CREATE DATABASE {} ENGINE = URL({})", backQuoteIfNeed(database_name), quoteString(base_url));

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

/**
 * Returns an empty vector because the database stores no tables of its own, so there is nothing to back up
 */
std::vector<std::pair<ASTPtr, StoragePtr>> DatabaseURL::getTablesForBackup(const FilterByNameFunction &, const ContextPtr &) const
{
    return {};
}

/**
 * Returns an empty iterator because the database does not have its own tables:
 * they are created on the fly from the URLs given as table names
 */
DatabaseTablesIteratorPtr DatabaseURL::getTablesIterator(ContextPtr, const FilterByNameFunction &, bool) const
{
    return std::make_unique<DatabaseTablesSnapshotIterator>(Tables{}, getDatabaseName());
}

void registerDatabaseURL(DatabaseFactory & factory);
void registerDatabaseURL(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        auto * engine_define = args.create_query.storage;
        const ASTFunction * engine = engine_define->engine;
        const String & engine_name = engine_define->engine->name;

        String base_url;

        if (engine->arguments && !engine->arguments->children.empty())
        {
            if (engine->arguments->children.size() != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "URL database requires at most 1 argument: base_url");

            const auto & arguments = engine->arguments->children;
            base_url = safeGetLiteralValue<String>(arguments[0], engine_name);
        }

        return std::make_shared<DatabaseURL>(args.database_name, base_url, args.context);
    };
    factory.registerDatabase("URL", create_fn, urlSecretArguments(0), {
        .supports_arguments = true,
        .is_external = true,
        .source_access_type = AccessTypeObjects::Source::URL,
    }, Documentation{
        .description = R"DOCS_MD(
The `URL` database engine treats table names as URLs and exposes the data at those URLs as tables. It uses the [`url`](/reference/functions/table-functions/url) table function, which dispatches each URL to the appropriate backend, such as `file`, `s3`, HDFS, Azure Blob Storage, or HTTP.

## Creating a database {#creating-a-database}

```sql
CREATE DATABASE web_data
ENGINE = URL([base_url]);
```

When `base_url` is specified, relative table names are resolved against it. Without a base URL, every table name must be a complete URL.

## Usage {#usage}

```sql
CREATE DATABASE web_data
ENGINE = URL('https://example.com/data/');

SELECT * FROM web_data.`daily.csv`;
```

The database owns no table definitions. Each table is resolved from its URL when it is used, so its schema is inferred from the current data. Table names can also use a full URL, for example `web_data.`s3://bucket/data/events.parquet``.

## Access control {#access-control}

Creating this database requires `READ` and `WRITE` source grants on `URL`, regardless of [`table_engines_require_grant`](/reference/settings/server-settings/settings/other#table_engines_require_grant). Reading a table also requires a `READ` source grant for the resolved backend; writing requires the matching `WRITE` grant. For example:

```sql
GRANT READ, WRITE ON URL TO user_name;
GRANT READ, WRITE ON S3 TO user_name;
```

For a database with a `file://` base URL, a user needs `READ ON FILE`. On ClickHouse server, local files are additionally restricted to [`user_files_path`](/reference/settings/server-settings/settings#user_files_path).

See the [`SOURCES` privileges](/reference/statements/grant#sources) for version and compatibility details.

## See also {#see-also}

- [`url` table function](/reference/functions/table-functions/url)
- [Filesystem database engine](/reference/engines/database-engines/filesystem)
- [S3 database engine](/reference/engines/database-engines/s3)
)DOCS_MD",
        .syntax = "ENGINE = URL([base_url])",
        .examples = {{
            "Reading files of the user_files directory through a database with a `file://` base URL",
            R"(
INSERT INTO FUNCTION file('web/daily.csv', 'CSVWithNames', 'day Date, visits UInt32') SETTINGS engine_file_truncate_on_insert = 1 VALUES ('2024-01-01', 100), ('2024-01-02', 150);
CREATE DATABASE web ENGINE = URL('file://web/');
SELECT * FROM web.`daily.csv`;
DROP DATABASE web;
            )",
            R"(
┌────────day─┬─visits─┐
│ 2024-01-01 │    100 │
│ 2024-01-02 │    150 │
└────────────┴────────┘
            )"
        }},
        .introduced_in = {26, 8},
        .related = {"Filesystem", "S3", "HDFS"}});
}
}
