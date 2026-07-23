#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseURL.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageURL.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/StringUtils.h>
#include <Common/filesystemHelpers.h>
#include <Common/quoteString.h>

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
    extern const int BAD_ARGUMENTS;
    extern const int FILE_DOESNT_EXIST;
    extern const int UNKNOWN_TABLE;
}

namespace
{

/// Check that the string starts with a valid RFC 3986 scheme followed by "://".
bool hasURLScheme(const String & url)
{
    auto scheme_end = url.find("://");
    if (scheme_end == String::npos || scheme_end == 0)
        return false;

    if (!std::isalpha(static_cast<unsigned char>(url[0])))
        return false;

    for (size_t i = 1; i < scheme_end; ++i)
    {
        char c = url[i];
        if (!std::isalnum(static_cast<unsigned char>(c)) && c != '+' && c != '-' && c != '.')
            return false;
    }

    return true;
}

}

DatabaseURL::DatabaseURL(const String & name_, const String & base_url_, ContextPtr context_)
    : IDatabase(name_), WithContext(context_->getGlobalContext()), base_url(base_url_)
{
    if (!base_url.empty() && !hasURLScheme(base_url))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "The base URL of a URL database must contain a scheme (e.g. https://), got: {}", base_url);
}

String DatabaseURL::getTableURL(const String & name) const
{
    String resolved = StorageURL::resolveURLBase(name, base_url, "base URL of the URL database");

    /// A plain name with a ':' in its first segment (e.g. `report:2026.csv`) is not a valid relative
    /// reference per RFC 3986 -- it parses as a URI with a scheme -- so `resolveURLBase` returns it
    /// unchanged. It is not a usable URL either, so resolve it against the base as a relative path.
    if (!base_url.empty() && !hasURLScheme(resolved) && resolved == name)
        resolved = StorageURL::resolveURLBase("./" + name, base_url, "base URL of the URL database");

    if (!hasURLScheme(resolved))
        return {};
    return resolved;
}

bool DatabaseURL::checkFileURLExists(const String & url, ContextPtr context_, bool throw_on_error) const
{
    if (classifyURLScheme(url) != URLSchemeTarget::File)
        return true;

    fs::path fs_path(getLocalPathFromFileURL(url));
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

    if (!fs::exists(path))
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File does not exist: {}", path);
        return false;
    }

    if (!fs::is_regular_file(path))
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File is directory, but expected a file: {}", path);
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

    auto ast_function_ptr = makeASTFunction("url", make_intrusive<ASTLiteral>(url));

    /// The table is referenced in the query by an identifier (`db.table`), which cannot be
    /// rewritten into a `urlCluster(...)` table function call for sending to other replicas.
    /// Disable the parallel-replicas auto-conversion to cluster storages: otherwise the `url`
    /// table function would create `StorageURLCluster`, whose `updateQueryToSendIfNeeded`
    /// requires a table function in the query and throws a logical error for identifiers.
    ContextMutablePtr context_copy = Context::createCopy(context_);
    context_copy->setSetting("parallel_replicas_for_cluster_engines", Field(false));

    auto table_function = TableFunctionFactory::instance().get(ast_function_ptr, context_copy);
    if (!table_function)
        return nullptr;

    /// The `url` table function throws if a table cannot be created from the URL (for example, the
    /// resource is unreachable). Such errors are not swallowed as "table does not exist" even from
    /// `tryGetTable`, because they are more informative. The tables are intentionally not cached:
    /// the remote data (and the inferred schema) can change between queries.
    return table_function->execute(ast_function_ptr, context_copy, name);
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
 * Returns an empty vector because the database is read-only and no tables can be backed up
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
    factory.registerDatabase("URL", create_fn, {
        .supports_arguments = true,
        .is_external = true,
        .source_access_type = AccessTypeObjects::Source::URL,
    }, Documentation{
        .description = "A read-only database that treats table names as URLs and exposes the data they point to as tables. "
                       "Relative names are resolved against the optional base URL. The URL scheme selects the backend: "
                       "`file://` reads local files, `s3://` (and `gs://`, `gcs://`, `oss://`) reads object storage, "
                       "`az://`/`azure://`/`abfss://` reads Azure Blob Storage, `hdfs://` reads HDFS, and `http://`/`https://` "
                       "and other schemes are read by the URL engine, so files, web and object storage URLs are handled uniformly. "
                       "clickhouse-local uses it (inside the Overlay database, with the `file://` base URL) as the default "
                       "database, so a plain table name resolves to a file in the current directory, while queries like "
                       "`SELECT * FROM 'https://example.com/data.csv'` read from the URL.",
        .syntax = "ENGINE = URL([base_url])",
        .examples = {{
            "Usage",
            "CREATE DATABASE web ENGINE = URL('https://example.com/data/');\n"
            "SELECT * FROM web.`daily.csv`; -- reads https://example.com/data/daily.csv",
            ""
        }},
        .introduced_in = {26, 7},
        .related = {"Filesystem", "S3", "HDFS"}});
}
}
