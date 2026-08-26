#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseURL.h>

#include <Access/ContextAccess.h>
#include <Access/Common/AccessFlags.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageProxy.h>
#include <Storages/StorageURL.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/StringUtils.h>
#include <Common/filesystemHelpers.h>
#include <Common/quoteString.h>

#include <filesystem>
#include <mutex>

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

/// A table of a `URL` database is a thin wrapper over the `url` table function, which dispatches
/// the URL to the matching backend (`file`, `s3`, `azureBlobStorage`, ...).
struct URLTableDelegate
{
    TableFunctionPtr table_function;
    StoragePtr storage;
};

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
URLTableDelegate makeURLTableDelegate(const String & url, const String & name, ContextPtr context, bool is_insert_query)
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

/// Resolving a table of a `URL` database requires the READ source grant: the table is resolved by
/// inferring its structure from the data, and the catalog then exposes that structure to the user
/// (`DESCRIBE`, `EXPLAIN`, the analyzer) without necessarily reading the data. Writing requires the
/// WRITE source grant on top of that: the delegate storage itself may be writable
/// (`INSERT INTO web.`s3://...``) and the interpreter checks only the INSERT privilege on the
/// logical table name, so the proxy intercepts every write entry point (INSERT, async insert, a
/// materialized view target, `TRUNCATE`) and re-resolves the delegate in write mode.
///
/// The proxy also carries the logical storage ID (`db`.`name`): the table function creates the
/// nested storage under the internal `_table_function` database, and privilege checks against the
/// storage ID (e.g. the SELECT check in `InterpreterSelectQuery`) must see the name the user's
/// grants refer to.
class StorageURLDatabaseTable final : public StorageProxy
{
public:
    StorageURLDatabaseTable(const StorageID & storage_id_, const String & url_, StoragePtr nested_, TableFunctionPtr table_function_)
        : StorageProxy(storage_id_)
        , url(url_)
        , nested(std::move(nested_))
        , table_function(std::move(table_function_))
    {
        const auto nested_metadata = nested->getInMemoryMetadataPtr(nullptr, false);
        setInMemoryMetadata(*nested_metadata);
    }

    StoragePtr getNested() const override { return nested; }
    String getName() const override { return nested->getName(); }

    /// Engine classification is used by policy checks (e.g. the `disable_insertion_and_mutation`
    /// guard in `InterpreterInsertQuery`, which exempts external engines), so report the class of
    /// the delegate instead of the `IStorage` default of a local engine. Deliberately not done in
    /// `StorageProxy`: the lazy proxies (`StorageTableProxy`, `StorageTableFunctionProxy`) would
    /// have to materialize and start up the nested storage just to answer a classification query.
    bool isDataLake() const override { return nested->isDataLake(); }
    bool isExternalDatabase() const override { return nested->isExternalDatabase(); }
    bool isObjectStorage() const override { return nested->isObjectStorage(); }
    bool isMessageQueue() const override { return nested->isMessageQueue(); }

    /// Forward the delegate's planner-visible capability contracts. `StorageProxy` forwards only a
    /// part of them (e.g. `supportsPrewhere`, `supportsSubcolumns`), so without these overrides the
    /// wrapper would silently fall back to the `IStorage` defaults and a table of a `URL` database
    /// would behave differently from the same data read directly through `file`/`s3` or through
    /// `ENGINE = URL(...)` (whose wrapper, `StorageURLSchemeDispatch`, carries the same overrides):
    ///
    /// * `supportedPrewhereColumns` defaults to `std::nullopt` (unrestricted) and
    ///   `canMoveConditionsToPrewhere` to `supportsPrewhere`, while the delegates keep `PREWHERE`
    ///   away from columns not materialized at `PREWHERE` time (default expressions, hive-partition
    ///   columns);
    /// * `supportsTrivialCountOptimization` defaults to `false`, while the delegates return `true`,
    ///   so `SELECT count()` would parse the external data instead of using the optimized count path.
    std::optional<NameSet> supportedPrewhereColumns() const override { return nested->supportedPrewhereColumns(); }
    bool canMoveConditionsToPrewhere() const override { return nested->canMoveConditionsToPrewhere(); }
    bool supportsTrivialCountOptimization(const StorageSnapshotPtr & storage_snapshot, ContextPtr query_context) const override
    {
        return nested->supportsTrivialCountOptimization(storage_snapshot, query_context);
    }

    /// Read through a snapshot of the nested storage (like `StorageTableFunctionProxy` does):
    /// the storage may downcast `storage_snapshot->storage` to its own type.
    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr &,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override
    {
        const auto nested_metadata = nested->getInMemoryMetadataPtr(context, false);
        auto nested_snapshot = nested->getStorageSnapshot(nested_metadata, context);
        nested->read(query_plan, column_names, nested_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
    }

    /// Checked on the initiator, before the query is executed or queued for asynchronous insertion:
    /// with `async_insert = 1` the sink below is created in a background flush, after the query has
    /// already reported success to the user (`wait_for_async_insert = 0`) and possibly with
    /// different privileges than the ones the user had when the query was issued.
    void checkInsertIsAllowed(ContextPtr context) const override
    {
        table_function->checkSourceAccess(context, /* is_insert_query */ true);
    }

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override
    {
        return getNestedForWrite(context)->write(query, metadata_snapshot, context, async_insert);
    }

    void truncate(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        TableExclusiveLockHolder & lock) override
    {
        getNestedForWrite(context)->truncate(query, metadata_snapshot, context, lock);
    }

    void shutdown(bool is_drop) override
    {
        nested->shutdown(is_drop);

        std::lock_guard lock(nested_for_write_mutex);
        if (nested_for_write)
            nested_for_write->shutdown(is_drop);
    }

private:
    /// The delegate of a write is a separate storage: the backends of the `url` table function
    /// construct different clients for reads and writes (`azureBlobStorage` creates the container
    /// only in write mode), and the resolved one is a read-mode storage. It is created on the first
    /// write and reused, because a single `INSERT` may write through several sinks.
    /// The write source grant is checked as part of the resolution (see `makeURLTableDelegate`).
    StoragePtr getNestedForWrite(ContextPtr context) const
    {
        std::lock_guard lock(nested_for_write_mutex);
        if (!nested_for_write)
            nested_for_write
                = makeURLTableDelegate(url, getStorageID().getTableName(), context, /* is_insert_query */ true).storage;

        if (!nested_for_write)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Cannot write to the table {}: the URL {} is not writable",
                            getStorageID().getNameForLogs(), url);

        return nested_for_write;
    }

    const String url;
    StoragePtr nested;
    TableFunctionPtr table_function;

    mutable std::mutex nested_for_write_mutex;
    mutable StoragePtr nested_for_write;
};

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

    /// The result of this probe is reported to the user (`EXISTS TABLE` returns it, and the
    /// resolution of a table reports `FILE_DOESNT_EXIST` instead of `ACCESS_DENIED`), so it must
    /// not be performed without the read source grant: `EXISTS TABLE` requires only `SHOW TABLES`,
    /// which would otherwise turn a `URL` database into an oracle for the contents of `user_files`.
    /// Claim the table, as above: a resolution of it fails with the access error, and `EXISTS`
    /// answers what it answers for a remote URL, which is not probed either.
    if (!isFileReadGranted(context_))
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

    /// The delegate of a `file://` table must not be constructed without the read source grant
    /// either: already the parsing of the arguments of the `file` table function observes the
    /// filesystem (see `isFileReadGranted`), and the errors of that parsing (e.g. that the path is
    /// outside of `user_files`) are reported before the source access check of the delegate runs.
    /// Fail here with the same access error that check would have produced after the parsing.
    if (classifyURLScheme(url) == URLSchemeTarget::File)
        context_->getAccess()->checkAccessWithFilter(AccessType::READ, toStringSource(AccessTypeObjects::Source::FILE), /* filter */ "");

    auto delegate = makeURLTableDelegate(url, name, context_, /* is_insert_query */ false);
    if (!delegate.storage)
        return nullptr;

    return std::make_shared<StorageURLDatabaseTable>(
        StorageID(getDatabaseName(), name), url, std::move(delegate.storage), std::move(delegate.table_function));
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
    factory.registerDatabase("URL", create_fn, {
        .supports_arguments = true,
        .is_external = true,
        .source_access_type = AccessTypeObjects::Source::URL,
    }, Documentation{
        .description = "A database that treats table names as URLs and exposes the data they point to as tables. "
                       "Relative names are resolved against the optional base URL. The URL scheme selects the backend: "
                       "`file://` reads local files, `s3://` (and `gs://`, `gcs://`, `oss://`) reads object storage, "
                       "`az://`/`azure://`/`abfss://` reads Azure Blob Storage, `hdfs://` reads HDFS, and `http://`/`https://` "
                       "and other schemes are read by the URL engine, so files, web and object storage URLs are handled uniformly. "
                       "The database stores no tables of its own; resolving a table requires read access to the corresponding "
                       "source (e.g. `GRANT READ ON URL`), because the structure of the table is inferred from the data, "
                       "and `INSERT` writes to the underlying source and additionally requires the "
                       "corresponding write source grant (e.g. `GRANT WRITE ON S3`). "
                       "clickhouse-local uses it (inside the Overlay database, with the `file://` base URL) as the default "
                       "database, so a plain table name resolves to a file in the current directory, while queries like "
                       "`SELECT * FROM 'https://example.com/data.csv'` read from the URL. "
                       "A base URL scopes the database to a location: after "
                       "`CREATE DATABASE web ENGINE = URL('https://example.com/data/')`, the query "
                       "``SELECT * FROM web.`daily.csv` `` reads `https://example.com/data/daily.csv`.",
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
