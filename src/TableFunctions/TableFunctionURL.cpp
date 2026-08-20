#include <TableFunctions/TableFunctionURL.h>

#include <TableFunctions/registerTableFunctions.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/Web/Configuration.h>
#include <Storages/StorageURLCluster.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromVector.h>
#include <Storages/HivePartitioningUtils.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_url_wildcard_from_index_pages;
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsBool parallel_replicas_for_cluster_engines;
    extern const SettingsString cluster_for_parallel_replicas;
    extern const SettingsParallelReplicasMode parallel_replicas_mode;
    extern const SettingsString url_base;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{
    void checkExperimentalURLWildcardFromIndexPages(const ContextPtr & context)
    {
        if (context->getSettingsRef()[Setting::allow_experimental_url_wildcard_from_index_pages])
            return;

        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Wildcard expansion for `url` from HTTP index pages is experimental. "
            "Set `allow_experimental_url_wildcard_from_index_pages = 1` to enable it");
    }

    ASTs makeWebObjectStorageEngineArgs(
        const String & source,
        const String & format,
        const String & structure,
        const String & compression_method,
        const HTTPHeaderEntries & headers)
    {
        ASTs engine_args;
        engine_args.emplace_back(make_intrusive<ASTLiteral>(source));
        engine_args.emplace_back(make_intrusive<ASTLiteral>(format));

        if (structure != "auto" || compression_method != "auto")
            engine_args.emplace_back(make_intrusive<ASTLiteral>(structure));
        if (compression_method != "auto")
            engine_args.emplace_back(make_intrusive<ASTLiteral>(compression_method));

        if (!headers.empty())
        {
            ASTs header_equals;
            header_equals.reserve(headers.size());
            for (const auto & [header_name, header_value] : headers)
            {
                ASTs equals_args;
                equals_args.emplace_back(make_intrusive<ASTLiteral>(header_name));
                equals_args.emplace_back(make_intrusive<ASTLiteral>(header_value));
                header_equals.emplace_back(makeASTOperator("equals", std::move(equals_args)));
            }

            auto headers_list = make_intrusive<ASTExpressionList>();
            headers_list->children = std::move(header_equals);

            auto headers_func = make_intrusive<ASTFunction>();
            headers_func->name = "headers";
            headers_func->arguments = headers_list;
            headers_func->children.push_back(headers_func->arguments);
            engine_args.emplace_back(std::move(headers_func));
        }

        return engine_args;
    }
}

VectorWithMemoryTracking<size_t> TableFunctionURL::skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr) const
{
    auto & table_function_node = query_node_table_function->as<TableFunctionNode &>();
    auto & table_function_arguments_nodes = table_function_node.getArguments().getNodes();
    size_t table_function_arguments_size = table_function_arguments_nodes.size();

    VectorWithMemoryTracking<size_t> result;

    for (size_t i = 0; i < table_function_arguments_size; ++i)
    {
        auto * function_node = table_function_arguments_nodes[i]->as<FunctionNode>();
        if (function_node && function_node->getFunctionName() == "headers")
            result.push_back(i);
    }

    return result;
}

void TableFunctionURL::parseArguments(const ASTPtr & ast, ContextPtr context)
{
    /// Clone ast function, because we can modify it's arguments like removing headers.
    ITableFunctionFileLike::parseArguments(ast->clone(), context);
}

void TableFunctionURL::parseArgumentsImpl(ASTs & args, const ContextPtr & context)
{
    if (auto named_collection = tryGetNamedCollectionWithOverrides(args, context))
    {
        StorageURL::processNamedCollectionResult(configuration, *named_collection);

        filename = configuration.url;
        structure = configuration.structure;
        compression_method = configuration.compression_method;

        format = configuration.format;

        StorageURL::evalArgsAndCollectHeaders(args, configuration.headers, context);
    }
    else
    {
        size_t count = StorageURL::evalArgsAndCollectHeaders(args, configuration.headers, context);
        /// ITableFunctionFileLike cannot parse headers argument, so remove it.
        ASTPtr headers_ast;
        if (count != args.size())
        {
            chassert(count + 1 == args.size());
            headers_ast = args.back();
            args.pop_back();
        }

        ITableFunctionFileLike::parseArgumentsImpl(args, context);

        if (headers_ast)
            args.push_back(headers_ast);
    }

    /// Resolve relative URLs against the url_base setting.
    const auto & url_base = context->getSettingsRef()[Setting::url_base].value;
    filename = StorageURL::resolveURLBase(filename, url_base);
    configuration.url = filename;

    /// Dispatch to another backend based on the URL scheme (file://, s3://, az://, hdfs://, ...).
    /// http(s) and unrecognized schemes keep the plain StorageURL behavior below.
    if (const auto target = classifyURLScheme(filename); target != URLSchemeTarget::URL)
    {
        /// `urlCluster` reaches this code path too (it strips the cluster name and delegates to
        /// `TableFunctionURL::parseArgumentsImpl`). Scheme dispatch builds a non-clustered delegate,
        /// so the read would silently run on the initiator and ignore the requested cluster. Reject
        /// such calls until the clustered delegates (`s3Cluster`, `fileCluster`, ...) are wired up.
        if (isClusterFunction())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The urlCluster table function does not support the '{}' scheme (URL '{}'); "
                "use the {}Cluster table function for this backend instead",
                storageEngineNameForURLScheme(target), filename, tableFunctionNameForURLScheme(target));

        if (!configuration.headers.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The url table function does not support headers(...) when dispatching to the {} engine (URL '{}')",
                storageEngineNameForURLScheme(target), filename);

        buildDelegate(target, context);
        return;
    }

    /// Re-derive format from the resolved URL if still auto, because the original
    /// filename may have been a relative reference (e.g. "?x=1") with no extension.
    /// `resolveURLBase` tolerates malformed inputs via string manipulation, so the resolved URL
    /// may contain characters that `Poco::URI` rejects. Fall back to "auto" instead of throwing.
    if (format == "auto")
    {
        try
        {
            format = FormatFactory::instance().tryGetFormatFromFileName(Poco::URI(filename).getPath()).value_or("auto");
        }
        catch (const Poco::Exception &) // NOLINT(bugprone-empty-catch)
        {
        }
    }
}

void TableFunctionURL::buildDelegate(URLSchemeTarget target, const ContextPtr & context)
{
    delegate_engine_name = storageEngineNameForURLScheme(target);

    auto args_list = make_intrusive<ASTExpressionList>();

    if (target == URLSchemeTarget::Azure)
    {
        auto parts = parseAzureURL(filename);
        args_list->children.push_back(make_intrusive<ASTLiteral>(parts.account_url));
        args_list->children.push_back(make_intrusive<ASTLiteral>(parts.container));
        args_list->children.push_back(make_intrusive<ASTLiteral>(parts.blob_path));

        /// `azureBlobStorage` positional order after the source triple is (format, compression, structure).
        const bool need_structure = structure != "auto";
        const bool need_compression = compression_method != "auto";
        const bool need_format = format != "auto" || need_compression || need_structure;
        if (need_format)
            args_list->children.push_back(make_intrusive<ASTLiteral>(format));
        if (need_compression || need_structure)
            args_list->children.push_back(make_intrusive<ASTLiteral>(need_compression ? compression_method : String("auto")));
        if (need_structure)
            args_list->children.push_back(make_intrusive<ASTLiteral>(structure));
    }
    else
    {
        const String source = (target == URLSchemeTarget::File) ? getLocalPathFromFileURL(filename) : filename;
        args_list->children.push_back(make_intrusive<ASTLiteral>(source));

        /// `file`, `s3` and `hdfs` share the (source, format, structure, compression) positional order.
        const bool need_compression = compression_method != "auto";
        const bool need_structure = structure != "auto" || need_compression;
        const bool need_format = format != "auto" || need_structure;
        if (need_format)
            args_list->children.push_back(make_intrusive<ASTLiteral>(format));
        if (need_structure)
            args_list->children.push_back(make_intrusive<ASTLiteral>(structure));
        if (need_compression)
            args_list->children.push_back(make_intrusive<ASTLiteral>(compression_method));
    }

    auto delegate_ast = make_intrusive<ASTFunction>();
    delegate_ast->name = tableFunctionNameForURLScheme(target);
    delegate_ast->arguments = args_list;
    delegate_ast->children.push_back(args_list);

    delegate = TableFunctionFactory::instance().get(delegate_ast, context);

    /// Use the delegate's own access URI for source-access filtering instead of approximating it
    /// from `filename`. This keeps filtered source grants consistent with calling the delegate
    /// directly: e.g. `azureBlobStorage` filters on `blob_path.path` (not the full `az://...` URL),
    /// `file` reports an empty URI, while `s3`/`hdfs` report the full URL as before.
    delegate_function_uri = delegate->getFunctionURI();

    if (!structure_hint.empty())
        delegate->setStructureHint(structure_hint);
}

StoragePtr TableFunctionURL::executeImpl(
    const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const
{
    if (delegate)
        return delegate->execute(ast_function, context, table_name, std::move(cached_columns), /*use_global_context=*/false, is_insert_query);

    return ITableFunctionFileLike::executeImpl(ast_function, context, table_name, std::move(cached_columns), is_insert_query);
}

bool TableFunctionURL::needStructureHint() const
{
    return delegate ? delegate->needStructureHint() : ITableFunctionFileLike::needStructureHint();
}

void TableFunctionURL::setStructureHint(const ColumnsDescription & structure_hint_)
{
    ITableFunctionFileLike::setStructureHint(structure_hint_);
    if (delegate)
        delegate->setStructureHint(structure_hint_);
}

bool TableFunctionURL::supportsReadingSubsetOfColumns(const ContextPtr & context)
{
    return delegate ? delegate->supportsReadingSubsetOfColumns(context) : ITableFunctionFileLike::supportsReadingSubsetOfColumns(context);
}

NameSet TableFunctionURL::getVirtualsToCheckBeforeUsingStructureHint() const
{
    return delegate ? delegate->getVirtualsToCheckBeforeUsingStructureHint() : ITableFunctionFileLike::getVirtualsToCheckBeforeUsingStructureHint();
}

bool TableFunctionURL::hasStaticStructure() const
{
    /// ITableFunctionFileLike::hasStaticStructure() is private; replicate its logic for the URL path.
    return delegate ? delegate->hasStaticStructure() : (structure != "auto");
}

void TableFunctionURL::setPartitionBy(const ASTPtr & partition_by_)
{
    if (delegate)
        delegate->setPartitionBy(partition_by_);
}

StoragePtr TableFunctionURL::getStorage(
    const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr context,
    const std::string & table_name, const String & compression_method_, bool is_insert_query) const
{
    const auto & settings = context->getSettingsRef();
    const auto is_secondary_query = context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY;
    const auto parallel_replicas_cluster_name = settings[Setting::cluster_for_parallel_replicas].toString();

    /// Listable `*` / `**` path wildcards are expanded by listing HTTP index pages through
    /// `StorageObjectStorage` (the branch below). `StorageURLCluster` still uses
    /// `DisclosedGlobIterator` / `parseRemoteDescription` and cannot list index pages, so it must not
    /// take over such queries via the parallel-replicas path — that would silently fall back to the
    /// old literal/template expansion and read different (or no) files than the non-cluster path.
    const bool use_web_wildcard = !is_insert_query && configuration.http_method.empty() && urlPathHasListableGlobs(source);

    const bool can_use_parallel_replicas = !parallel_replicas_cluster_name.empty()
        && settings[Setting::parallel_replicas_for_cluster_engines]
        && context->canUseTaskBasedParallelReplicas()
        && !context->isDistributed()
        && !is_secondary_query
        && !is_insert_query
        && !use_web_wildcard;

    if (can_use_parallel_replicas)
    {
        return std::make_shared<StorageURLCluster>(
            context,
            parallel_replicas_cluster_name,
            source,
            format_,
            compression_method_,
            StorageID(getDatabaseName(), table_name),
            getActualTableStructure(context, true),
            ConstraintsDescription{},
            configuration);
    }

    if (use_web_wildcard)
    {
        checkExperimentalURLWildcardFromIndexPages(context);
        auto object_storage_configuration = std::make_shared<StorageWebConfiguration>();

        auto engine_args = makeWebObjectStorageEngineArgs(source, format_, structure, compression_method_, configuration.headers);
        StorageObjectStorageConfiguration::initialize(*object_storage_configuration, engine_args, context, /* with_table_structure */ true);

        ObjectStoragePtr object_storage = object_storage_configuration->createObjectStorage(context, /* is_readonly */ true, std::nullopt);

        return std::make_shared<StorageObjectStorage>(
            object_storage_configuration,
            object_storage,
            context,
            StorageID(getDatabaseName(), table_name),
            columns,
            ConstraintsDescription{},
            String{},
            std::nullopt,
            LoadingStrictnessLevel::CREATE,
            /* catalog */ nullptr,
            /* if_not_exists */ false,
            /* is_datalake_query */ false,
            /* distributed_processing */ false,
            /* partition_by */ nullptr,
            /* order_by */ nullptr,
            /* is_table_function */ true,
            /* lazy_init */ false);
    }

    /// Note: distributed_processing is always false for the plain url() table function.
    /// Cluster table functions (urlCluster) handle distributed processing in their own getStorage() method.
    return std::make_shared<StorageURL>(
        source,
        StorageID(getDatabaseName(), table_name),
        format_,
        std::nullopt /*format settings*/,
        columns,
        ConstraintsDescription{},
        String{},
        context,
        compression_method_,
        configuration.headers,
        configuration.http_method,
        nullptr,
        /*distributed_processing=*/false);
}

ColumnsDescription TableFunctionURL::getActualTableStructure(ContextPtr context, bool is_insert_query) const
{
    if (delegate)
        return delegate->getActualTableStructure(context, is_insert_query);

    if (structure == "auto")
    {
        ColumnsDescription columns;
        String sample_path = filename;

        if (configuration.http_method.empty() && urlPathHasListableGlobs(filename))
        {
            checkExperimentalURLWildcardFromIndexPages(context);

            auto object_storage_configuration = std::make_shared<StorageWebConfiguration>();
            auto engine_args = makeWebObjectStorageEngineArgs(filename, format, structure, compression_method, configuration.headers);
            StorageObjectStorageConfiguration::initialize(*object_storage_configuration, engine_args, context, /* with_table_structure */ true);
            object_storage_configuration->check(context);

            auto object_storage = object_storage_configuration->createObjectStorage(context, /* is_readonly */ true, std::nullopt);
            if (format == "auto")
            {
                auto schema_and_format = StorageObjectStorage::resolveSchemaAndFormatFromData(
                    object_storage, object_storage_configuration, std::nullopt, sample_path, context);
                columns = std::move(schema_and_format.first);
            }
            else
            {
                columns = StorageObjectStorage::resolveSchemaFromData(
                    object_storage, object_storage_configuration, std::nullopt, sample_path, context);
            }
        }
        else if (format == "auto")
        {
            columns = StorageURL::getTableStructureAndFormatFromData(
                filename,
                chooseCompressionMethod(Poco::URI(filename).getPath(), compression_method),
                configuration.headers,
                std::nullopt,
                context).first;
        }
        else
        {
            columns = StorageURL::getTableStructureFromData(format,
                filename,
                chooseCompressionMethod(Poco::URI(filename).getPath(), compression_method),
                configuration.headers,
                std::nullopt,
                context);
        }

        HivePartitioningUtils::setupHivePartitioningForFileURLLikeStorage(
            columns,
            sample_path,
            /* inferred_schema */ true,
            /* format_settings */ std::nullopt,
            context);

        return columns;
    }

    return parseColumnsListFromString(structure, context);
}

std::optional<String> TableFunctionURL::tryGetFormatFromFirstArgument()
{
    return FormatFactory::instance().tryGetFormatFromFileName(Poco::URI(filename).getPath());
}

void registerTableFunctionURL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionURL>({.description = R"DOCS_MD(
import ExperimentalBadge from "/snippets/components/ExperimentalBadge/ExperimentalBadge.jsx";
import CloudNotSupportedBadge from "/snippets/components/CloudNotSupportedBadge/CloudNotSupportedBadge.jsx";

`url` function creates a table from the `URL` with given `format` and `structure`.

`url` function may be used in `SELECT` and `INSERT` queries on data in [URL](/reference/engines/table-engines/special/url) tables.

## Syntax {#syntax}

```sql
url(URL [,format] [,structure] [,headers])
```

## Parameters {#parameters}

| Parameter   | Description                                                                                                                                            |
|-------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| `URL`       | A single-quoted URL whose scheme selects the backend. An `http`/`https` (or unrecognized) URL is a server address accepting `GET` or `POST` requests (for `SELECT` or `INSERT` queries correspondingly); a recognized non-HTTP scheme (`file://`, `s3://`, `az://`, `hdfs://`, …) is delegated to the matching table function — see [Dispatching by URL scheme](#scheme-dispatch). Type: [String](/reference/data-types/string). |
| `format`    | [Format](/reference/formats/index) of the data. Type: [String](/reference/data-types/string).                                                  |
| `structure` | Table structure in `'UserID UInt64, Name String'` format. Determines column names and types. Type: [String](/reference/data-types/string).     |
| `headers`   | Headers in `'headers('key1'='value1', 'key2'='value2')'` format. You can set headers for HTTP call.                                                  |

## Returned value {#returned_value}

A table with the specified format and structure and with data from the defined `URL`.

## Examples {#examples}

Getting the first 3 lines of a table that contains columns of `String` and [UInt32](/reference/data-types/int-uint) type from HTTP-server which answers in [CSV](/reference/formats/CSV/CSV) format.

```sql
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32', headers('Accept'='text/csv; charset=utf-8')) LIMIT 3;
```

Inserting data from a `URL` into a table:

```sql
CREATE TABLE test_table (column1 String, column2 UInt32) ENGINE=Memory;
INSERT INTO FUNCTION url('http://127.0.0.1:8123/?query=INSERT+INTO+test_table+FORMAT+CSV', 'CSV', 'column1 String, column2 UInt32') VALUES ('http interface', 42);
SELECT * FROM test_table;
```

## Dispatching by URL scheme {#scheme-dispatch}

The `url` function acts as a unified wrapper on top of the other file- and object-storage table functions: it dispatches to the right backend based on the URL scheme. This lets you read from any supported location with a single uniform syntax.

| Scheme                                              | Dispatches to                                              |
|-----------------------------------------------------|------------------------------------------------------------|
| `http`, `https` (and any unrecognized scheme)       | the `URL` engine itself (HTTP `GET`/`POST`)                |
| `file`                                              | the [`file`](/reference/functions/table-functions/file) function                             |
| `s3`, `gs`, `gcs`, `oss`                            | the [`s3`](/reference/functions/table-functions/s3) function                                 |
| `az`, `azure`, `abfss`, `abfs`                      | the [`azureBlobStorage`](/reference/functions/table-functions/azureBlobStorage) function     |
| `hdfs`                                              | the [`hdfs`](/reference/functions/table-functions/hdfs) function                             |

Only the S3 schemes that the S3 URI mapper resolves to a concrete endpoint without extra configuration (`s3`, plus `gs`/`gcs`/`oss`) are dispatched. Other S3-compatible vendor schemes (`cos`, `obs`, `eos`, …) are region-specific and have no default endpoint mapping, so a `cos://…` URL is treated as an unrecognized scheme and reported as an error; use the [`s3`](/reference/functions/table-functions/s3) function directly (with `url_scheme_mappers` configured) for those backends.

For `file://`, a relative path (`file://data.csv`) is resolved inside the [user_files](/reference/settings/server-settings/settings#user_files_path) directory, and an absolute path (`file:///home/user/data.csv`) must point inside it as usual.

The `format`, `structure` and `compression_method` arguments and the [url_base](#resolving-relative-urls) setting work the same regardless of the dispatch target.

```sql
SELECT * FROM url('file://data.csv', CSV, 'a UInt32, b String');
SELECT * FROM url('s3://clickhouse-public-datasets/hits_compatible/hits.csv');
```

Scheme dispatch is not yet wired through [`urlCluster`](/reference/functions/table-functions/urlCluster): a non-`http(s)` scheme passed to `urlCluster` is rejected with an error. Use the corresponding cluster function (`s3Cluster`, `azureBlobStorageCluster`, `hdfsCluster`, …) for those backends instead.

## Globs in URL {#globs-in-url}

Patterns in `{ }` are used to generate a set of shards or to specify failover addresses. Supported pattern types and examples see in the description of the [remote](/reference/functions/table-functions/remote#globs-in-addresses) function.
Character `|` inside patterns is used to specify failover addresses. They are iterated in the same order as listed in the pattern. The number of generated addresses is limited by [glob_expansion_max_elements](/reference/settings/session-settings#glob_expansion_max_elements) setting.
For path glob syntax in the URL path (such as `*`, `{a,b}`, `{N..M}`, and `**`), see [Globs in path](/reference/functions/table-functions/file#globs-in-path). Note that `?` starts the query string in a URL and cannot be used as a wildcard in the path component.

## Wildcards with HTTP index pages {#wildcards-with-http-index-pages}

For `url` and the `URL` table engine, ClickHouse can expand wildcards by fetching HTTP index pages (HTML or plaintext) and extracting URLs from the response body. This enables patterns like `/**/` when the server exposes directory listings.

Notes:
- Relative URLs are resolved against the index page URL.
- `URL` templates are expanded before fetching index pages, including comma and numeric range shard expansion and `|` failover options outside the path component.
- `|` failover patterns inside the path component are not supported for HTTP index-page expansion.
- Wildcard matching is applied to the URL path component.
- If a listed URL already contains a query string or fragment, it takes precedence over the ones from the source URL. Otherwise, the query string and fragment from the source URL are used.
- An empty listing is allowed; HTTP errors (e.g. 404) for index pages raise exceptions.
- The maximum index page size is limited by [max_http_index_page_size](/reference/settings/server-settings/settings#max_http_index_page_size).
- The maximum number of directories read during recursive expansion is limited by [url_wildcard_max_directories_to_read](/reference/settings/session-settings#url_wildcard_max_directories_to_read).

Example:

```sql
SELECT count()
FROM url('https://ftp.gnu.org/gnu/wget/wget-1.21*.tar.gz', 'RawBLOB')
SETTINGS max_threads = 1, allow_experimental_url_wildcard_from_index_pages = 1;
```

## Virtual Columns {#virtual-columns}

- `_path` — Path to the `URL`. Type: `LowCardinality(String)`.
- `_file` — Resource name of the `URL`. Type: `LowCardinality(String)`.
- `_size` — Size of the resource in bytes. Type: `Nullable(UInt64)`. If the size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_headers` - HTTP response headers. Type: `Map(LowCardinality(String), LowCardinality(String))`.

## use_hive_partitioning setting {#hive-style-partitioning}

When setting `use_hive_partitioning` is set to 1, ClickHouse will detect Hive-style partitioning in the path (`/name=value/`) and will allow to use partition columns as virtual columns in the query. These virtual columns will have the same names as in the partitioned path.

**Example**

Use virtual column, created with Hive-style partitioning

```sql
SELECT * FROM url('http://data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

## Resolving relative URLs {#resolving-relative-urls}

The [url_base](/reference/settings/session-settings#url_base) setting allows passing a relative URL to the `url` function. When `url_base` is set and the function argument is a relative reference, it is resolved against the base URL per [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986).

The resolution rules are:

- **Path-relative** (e.g. `data.csv`): merged with the base URL path — everything after the last `/` of the base path is replaced. The trailing slash matters: `https://example.com/dir/` + `data.csv` gives `https://example.com/dir/data.csv`, but `https://example.com/dir` + `data.csv` gives `https://example.com/data.csv`. Dot segments (`./` and `../`) are normalized.
- **Host-relative** (e.g. `/test/data.csv`): resolved using the scheme and host of the base URL.
- **Scheme-relative** (e.g. `//other.com/test/data.csv`): resolved using the scheme of the base URL.
- **Query-only** (e.g. `?x=1`): appended to the full base path, replacing any existing query or fragment.
- **Fragment-only** (e.g. `#frag`): appended to the base URL, preserving the query, replacing any existing fragment.
- **Empty**: returns the base URL without fragment.
- **Absolute URL**: passed through unchanged; `url_base` is ignored.

**Example**

```sql
SET url_base = 'https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/';
SELECT * FROM url('tests/queries/0_stateless/data_csv/data.csv', CSV) LIMIT 3;
```

## Storage Settings {#storage-settings}

- [engine_url_skip_empty_files](/reference/settings/session-settings#engine_url_skip_empty_files) - allows to skip empty files while reading. Disabled by default.
- [enable_url_encoding](/reference/settings/session-settings#enable_url_encoding) - allows to enable/disable decoding/encoding path in uri. Enabled by default.
- [url_base](/reference/settings/session-settings#url_base) - base URL for resolving relative URLs passed to the `url` function.

## Permissions {#permissions}

`url` function requires `CREATE TEMPORARY TABLE` permission. As such - it'll not work for users with [readonly](/concepts/features/configuration/settings/permissions-for-queries#readonly) = 1 setting. At least readonly = 2 is required.

## Related {#related}

- [Virtual columns](/reference/engines/table-engines/index#table_engines-virtual_columns)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction});
}
}
