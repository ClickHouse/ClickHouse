#include <TableFunctions/TableFunctionURLCluster.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <Common/Exception.h>
#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>

#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageCluster.h>
#include <Storages/ObjectStorage/Web/Configuration.h>
#include <TableFunctions/registerTableFunctions.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    /// The web object storage behind the index pages follows redirects without the per-hop grant check of
    /// `StorageURL`, so a filtered `READ ON URL` cannot bound this read: the grant on the whole source is
    /// needed, exactly as in `TableFunctionURL`.
    void checkWholeSourceAccessForURLWildcard(const ContextPtr & context)
    {
        context->checkAccess(AccessType::READ, toStringSource(AccessTypeObjects::Source::URL));
    }
}


ColumnsDescription TableFunctionURLCluster::getActualTableStructure(ContextPtr context, bool is_insert_query) const
{
    return TableFunctionURL::getActualTableStructure(context, is_insert_query);
}

StoragePtr TableFunctionURLCluster::getStorage(
    const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr context,
    const std::string & table_name, const String & compression_method_, bool /*is_insert_query*/, bool is_table_definition) const
{
    /// The same split `TableFunctionURL` uses, so a definition stored by an earlier version keeps the
    /// exemption for a custom HTTP method or a failover group here as well instead of being reinterpreted
    /// as an archive read.
    const auto [url, archive_pattern] = getURLAndArchivePatternForSource(source);
    /// When archive syntax is disabled, `::` and everything after it deliberately remain part of the ordinary
    /// URL. In particular, glob characters there retain their normal URL semantics.
    const bool has_url_wildcards = configuration.http_method.empty() && urlPathHasListableGlobs(url);
    const bool needs_index_page_listing = configuration.http_method.empty() && urlPathNeedsIndexPageListing(url);

    /// The nodes must not receive the inferred values of the `NOT OVERRIDABLE` keys of a named collection.
    /// The plain `StorageURLCluster` branch below keeps that by passing no columns, but the object-storage
    /// implementation infers them from the data before it builds the query for the nodes, so the structure
    /// would be sent anyway. Reject the combination instead of failing on the node with an
    /// `Override not allowed for 'structure'` that does not name the cause.
    ///
    /// A definition stored by an earlier version may combine such a structure with a source this version
    /// would read through the object storage - before archive syntax existed, `a.zip::m.csv` was a literal
    /// URL. Such a definition keeps loading through the plain branch below, which passes no columns to the
    /// nodes and reads the source literally, so it keeps the meaning it was written with; only a fresh
    /// definition is rejected.
    const bool use_object_storage = has_url_wildcards || archive_pattern.has_value();

    if (use_object_storage && is_structure_not_overridable && !is_replayed_table_definition)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "A `NOT OVERRIDABLE` `structure` of a named collection is not supported for a wildcard or "
            "archive source, because the nodes would receive the inferred structure");

    /// The same holds for `format`: the query sent to the nodes carries the inferred format, which a
    /// `NOT OVERRIDABLE` key refuses there with an error that does not name the cause.
    if (use_object_storage && is_format_not_overridable && !is_replayed_table_definition)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "A `NOT OVERRIDABLE` `format` of a named collection is not supported for a wildcard or "
            "archive source, because the nodes would receive the inferred format");

    /// A replayed definition whose `format` is `NOT OVERRIDABLE` keeps the plain branch as well: the query
    /// sent to the nodes would carry the inferred format, which that key refuses there.
    const bool keeps_plain_branch = is_structure_not_overridable
        || (is_replayed_table_definition && is_format_not_overridable);

    if (use_object_storage && !keeps_plain_branch)
    {
        /// A glob in the URL path is read by listing the index page, which is the experimental read the
        /// setting gates. A literal path is expanded locally and needs no listing.
        if (needs_index_page_listing && !skip_experimental_gate)
            checkExperimentalURLWildcardFromIndexPages();

        /// The web object storage behind both the initiator and the workers follows redirects without the
        /// per-hop grant check of `StorageURL`, so a filtered `READ ON URL` cannot bound this read, exactly as
        /// for `url`: the whole-source grant is needed. A stored definition is exempt, as for `url`: it is read
        /// under the grants on the table.
        if (!is_replayed_table_definition)
            checkWholeSourceAccessForURLWildcard(context);

        /// `structure` has no corresponding `getStorage` argument. It is the value parsed by
        /// `ITableFunctionFileLike` together with the source, format, and compression arguments.
        auto object_storage_configuration
            = createWebObjectStorageConfiguration(source, format_, structure, compression_method_, context);
        auto object_storage = object_storage_configuration->createObjectStorage(context, /* is_readonly */ true, std::nullopt);

        if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
        {
            /// Workers always use distributed processing to request tasks from the initiator.
            /// `cluster_function_process_archive_on_multiple_nodes` changes whether those tasks
            /// contain individual files or whole archives, not whether workers request tasks.
            return std::make_shared<StorageObjectStorage>(
                object_storage_configuration,
                object_storage,
                context,
                StorageID(getDatabaseName(), table_name),
                columns,
                ConstraintsDescription{},
                /* comment */ String{},
                /* format_settings */ std::nullopt,
                /* mode */ LoadingStrictnessLevel::CREATE,
                /* catalog */ nullptr,
                /* if_not_exists */ false,
                /* is_datalake_query */ false,
                /* distributed_processing */ true,
                /* partition_by */ nullptr,
                /* order_by */ nullptr,
                /* is_table_function */ true,
                /* lazy_init */ true);
        }

        return std::make_shared<StorageObjectStorageCluster>(
            cluster_name,
            object_storage_configuration,
            object_storage,
            StorageID(getDatabaseName(), table_name),
            columns,
            ConstraintsDescription{},
            /* partition_by */ nullptr,
            context,
            /* is_table_function */ true);
    }

    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        //On worker node this uri won't contain globs
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
            /*distributed_processing=*/ true,
            globCaller(),
            /*is_replayed_definition=*/ false,
            /*check_redirect_access=*/ !is_table_definition);
    }

    checkCallerGrantAppliesOnWorkers(context, cluster_name);

    return std::make_shared<StorageURLCluster>(
        context,
        cluster_name,
        source,
        format_,
        compression_method_,
        StorageID(getDatabaseName(), table_name),
        is_structure_not_overridable ? ColumnsDescription{} : columns,
        ConstraintsDescription{},
        configuration);
}

void registerTableFunctionURLCluster(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionURLCluster>({.description = R"DOCS_MD(
Allows processing files from URL in parallel from many nodes in a specified cluster. On initiator it creates a connection to all nodes in the cluster, discloses asterisk in URL file path, and dispatches each file dynamically. On the worker node it asks the initiator about the next task to process and processes it. This is repeated until all tasks are finished.

## Syntax {#syntax}

```sql
urlCluster(cluster_name, URL [,format] [,structure] [,compression_method] [,headers])
```

## Arguments {#arguments}

| Argument       | Description                                                                                                                                            |
|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| `cluster_name` | Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.                                      |
| `URL`          | HTTP or HTTPS server address, which can accept `GET` requests. Type: [String](/reference/data-types/string).                               |
| `format`       | Optional. [Format](/reference/formats/index) of the data. Type: [String](/reference/data-types/string).                                                |
| `structure`    | Optional. Table structure in `'UserID UInt64, Name String'` format. Determines column names and types. Type: [String](/reference/data-types/string). |
| `compression_method` | Optional. Compression method. Supports the same values and automatic suffix detection as [`url`](/reference/functions/table-functions/url). |
| `headers`      | Optional HTTP headers in `headers('key'='value')` format. |

## Returned value {#returned-value}

A table with the specified format and structure and with data from the defined `URL`.

## Examples {#examples}

Getting the first 3 lines of a table that contains columns of `String` and [UInt32](/reference/data-types/int-uint) type from HTTP-server which answers in [CSV](/reference/formats/CSV/CSV) format.

1. Create a basic HTTP server using the standard Python 3 tools and start it:

```python
from http.server import BaseHTTPRequestHandler, HTTPServer

class CSVHTTPServer(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/csv')
        self.end_headers()

        self.wfile.write(bytes('Hello,1\nWorld,2\n', "utf-8"))

if __name__ == "__main__":
    server_address = ('127.0.0.1', 12345)
    HTTPServer(server_address, CSVHTTPServer).serve_forever()
```

```sql
SELECT * FROM urlCluster('cluster_simple','http://127.0.0.1:12345', CSV, 'column1 String, column2 UInt32')
```

## Virtual Columns {#virtual-columns}

- `_path` — Path to the `URL`. For an archive member, the value is `<namespace>/<archive path>::<member path>`, where the namespace of a Web source is its `host[:port]`, for example `example.com/data/a.zip::m.csv`. Type: `LowCardinality(String)`.
- `_file` — Resource name of the `URL`. For an archive member, the value is the member's path inside the archive. Type: `LowCardinality(String)`.
- `_size` — Size of the resource in bytes. For an archive member, the value is the member's uncompressed size, that is its bytes after the container's own compression is undone; for an entry stored with `DEFLATE` in a ZIP archive that is larger than the bytes the container holds. Type: `Nullable(UInt64)`. If the size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. For an archive member, this is the last modified time of the outer archive, not of the member. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_headers` - HTTP response headers. For an archive member, these are the headers of the metadata probe of the outer archive, not of the request that reads its bytes. Type: `Map(LowCardinality(String), LowCardinality(String))`.

## Working with archives {#working-with-archives}

Like `url`, `s3Cluster`, and `s3`, `urlCluster` can read files from remote ZIP, TAR, and 7Z archives by separating the archive URL and the path inside it with `::`:

```sql
SELECT *
FROM urlCluster(
    'cluster_simple',
    'https://example.com/dataset.zip :: data/*.csv'
);
```

The archive is distributed using the same `StorageObjectStorageCluster` implementation as `s3Cluster`. The [`cluster_function_process_archive_on_multiple_nodes`](/reference/settings/session-settings/cluster#cluster_function_process_archive_on_multiple_nodes) setting controls whether files from one archive can be processed on multiple cluster nodes. Both modes process each matching archive member once when every participating server supports `urlCluster` archive reads.

A server that predates archive support does not understand these tasks. With the setting enabled it receives a member path and reports the object as missing; with the setting disabled it receives the archive path and reads the container bytes as the table format, which can produce garbage rows. Upgrade every participating server before enabling archive reads through `urlCluster`, and keep the setting equal on the initiator and on the workers: a mismatch of [`cluster_function_process_archive_on_multiple_nodes`](/reference/settings/session-settings/cluster#cluster_function_process_archive_on_multiple_nodes) is reported as an error rather than silently read. [`allow_archive_path_syntax`](/reference/settings/session-settings/allow#allow_archive_path_syntax) has to match as well, because a worker parses the source again and a read task carries no record of how the initiator read it: a worker that has it off reads the task path as a plain URL, so with `cluster_function_process_archive_on_multiple_nodes = 1` it requests the member path as a URL and reports it as missing, and with `= 0` it reads the container bytes as the table format.

The [`allow_archive_path_syntax`](/reference/settings/session-settings/allow#allow_archive_path_syntax) setting is enabled by default. A `::` in a URL query string or fragment is treated as an archive separator only when the URL path itself looks like a supported archive path or contains `*`. For example, `api?x=::1` remains literal URL content, while `archive.zip?token=x::member.csv` reads `member.csv` from the archive without requiring spaces around `::`. A path such as `data.zip::v1` is inherently ambiguous and is interpreted as archive syntax while this setting is enabled; to use it as a literal URL path, disable `allow_archive_path_syntax` for the query.

Wildcards can be used both in the archive URL and in the path inside the archive. A literal archive URL is read directly; a glob character in the URL path - `*`, `**` or a `{...}` template - makes the read list the HTTP index page, so it requires [`allow_url_wildcard_from_index_pages`](/reference/settings/session-settings/allow#allow_url_wildcard_from_index_pages). The final combination of URL shards and the archive path is limited by [glob_expansion_max_elements](/reference/settings/session-settings/other#glob_expansion_max_elements).

A listed object key that itself contains `::` cannot be an archive source for `urlCluster`. A read task names the archive and the member as one `<archive>::<member>` string, so a key that carries the separator cannot be split back: the worker either requests a truncated URL or reports that the path cannot be told apart. Read such a source - a pre-signed URL whose query carries `::`, for example - with [`url`](/reference/functions/table-functions/url), which keeps the source whole.

## Globs in URL {#globs-in-url}

Patterns in `{ }` are used to generate a set of shards or to specify failover addresses. Supported pattern types and examples see in the description of the [remote](/reference/functions/table-functions/remote#globs-in-addresses) function.
Character `|` inside patterns is used to specify failover addresses. They are iterated in the same order as listed in the pattern. The number of generated addresses is limited by [glob_expansion_max_elements](/reference/settings/session-settings/other#glob_expansion_max_elements) setting.

With [`allow_url_wildcard_from_index_pages`](/reference/settings/session-settings/allow#allow_url_wildcard_from_index_pages) enabled, `urlCluster` expands `*` and `**` in URL paths by reading HTTP index pages and distributes the matched files across the cluster.

The addresses are generated one by one as tasks are handed to the nodes of the cluster, so `glob_expansion_max_elements` limits how many addresses a single query may read rather than how large the pattern is, as described for the [url](/reference/functions/table-functions/url#globs-in-url) function. A `_path` or `_file` predicate is applied to each address as it is generated, so the addresses it rejects are generated and counted against the limit as well; only the matching ones are dispatched.

## Permissions {#permissions}

A filtered grant names the object that is fetched. For an archive source that is the archive, not the member: `GRANT READ ON URL('http://host/a.zip')` covers every member of `a.zip`, while a grant written on `'http://host/a.zip :: member.csv'` does not match. A grant written before archive reads existed, on the literal `'http://host/a.zip::member.csv'` string no longer matches either, because the access check now names the archive; set `allow_archive_path_syntax = 0` to restore the old subject together with the literal meaning.

## Related {#related}

-   [HDFS engine](/reference/engines/table-engines/integrations/hdfs)
-   [URL table function](/reference/engines/table-engines/special/url)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}, urlSecretArguments(1));
}

}
