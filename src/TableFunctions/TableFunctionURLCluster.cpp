#include <TableFunctions/TableFunctionURLCluster.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <Common/Exception.h>
#include <Core/Settings.h>
#include <IO/Archives/ArchiveUtils.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageCluster.h>
#include <Storages/ObjectStorage/Web/Configuration.h>
#include <TableFunctions/registerTableFunctions.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace Setting
{
    extern const SettingsBool allow_archive_path_syntax;
}

namespace
{
struct WebObjectStorageUsage
{
    bool has_url_wildcards;
    bool has_archive_pattern;
};

WebObjectStorageUsage getWebObjectStorageUsage(
    const String & filename, const StorageURL::Configuration & configuration, ContextPtr context)
{
    String url = filename;
    std::optional<String> archive_pattern;
    /// When archive syntax is disabled, `::` and everything after it deliberately remain part of
    /// the ordinary URL. In particular, glob characters there retain their normal URL semantics.
    if (context->getSettingsRef()[Setting::allow_archive_path_syntax])
        std::tie(url, archive_pattern) = getURLAndArchivePattern(filename);

    if (archive_pattern && !configuration.http_method.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Archive path syntax is not supported with a custom HTTP method");

    return {
        .has_url_wildcards = configuration.http_method.empty() && urlPathHasListableGlobs(url),
        .has_archive_pattern = archive_pattern.has_value(),
    };
}
}

ColumnsDescription TableFunctionURLCluster::getActualTableStructure(ContextPtr context, bool is_insert_query) const
{
    return TableFunctionURL::getActualTableStructure(context, is_insert_query);
}

StoragePtr TableFunctionURLCluster::getStorage(
    const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr context,
    const std::string & table_name, const String & compression_method_, bool /*is_insert_query*/) const
{
    const auto usage = getWebObjectStorageUsage(source, configuration, context);
    if (usage.has_url_wildcards)
        checkExperimentalURLWildcardFromIndexPages(context);

    if (usage.has_url_wildcards || usage.has_archive_pattern)
    {
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
            /*distributed_processing=*/ true);
    }

    return std::make_shared<StorageURLCluster>(
        context,
        cluster_name,
        source,
        format_,
        compression_method_,
        StorageID(getDatabaseName(), table_name),
        getActualTableStructure(context, true),
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
| `format`       | [Format](/reference/formats/index) of the data. Type: [String](/reference/data-types/string).                                                |
| `structure`    | Table structure in `'UserID UInt64, Name String'` format. Determines column names and types. Type: [String](/reference/data-types/string). |
| `compression_method` | Compression method. Supports the same values and automatic suffix detection as [`url`](/reference/functions/table-functions/url). |
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

## Working with archives {#working-with-archives}

Like `url`, `s3Cluster`, and `s3`, `urlCluster` can read files from remote ZIP, TAR, and 7Z archives by separating the archive URL and the path inside it with `::`:

```sql
SELECT *
FROM urlCluster(
    'cluster_simple',
    'https://example.com/dataset.zip :: data/*.csv'
);
```

The archive is distributed using the same `StorageObjectStorageCluster` implementation as `s3Cluster`. The [cluster_function_process_archive_on_multiple_nodes](/operations/settings/settings#cluster_function_process_archive_on_multiple_nodes) setting controls whether files from one archive can be processed on multiple cluster nodes. Both modes process each matching archive member once when every participating server supports `urlCluster` archive reads; do not use this feature during a mixed-version upgrade with older servers.

The [`allow_archive_path_syntax`](/operations/settings/settings#allow_archive_path_syntax) setting is enabled by default. A `::` in a URL query string or fragment is treated as an archive separator only when the URL path itself looks like a supported archive path. For example, `api?x=::1` remains literal URL content, while `archive.zip?token=x::member.csv` reads `member.csv` from the archive without requiring spaces around `::`. A path such as `data.zip::v1` is inherently ambiguous and is interpreted as archive syntax while this setting is enabled; to use it as a literal URL path, disable `allow_archive_path_syntax` for the query.

Wildcards can be used both in the archive URL and in the path inside the archive. Brace and numeric templates in the archive URL are expanded locally. Comma alternatives are processed as independent archives, while `|` alternatives form a failover group:

```sql
SELECT *
FROM urlCluster(
    'cluster_simple',
    'https://example.com/dataset{0,1}{primary|mirror}.zip :: data/*.csv'
);
```

Expanding `*` or `**` in the archive URL requires [allow_experimental_url_wildcard_from_index_pages](/reference/settings/session-settings/allow-experimental#allow_experimental_url_wildcard_from_index_pages). Path-level `|` failover cannot be combined with this HTTP index-page expansion. The final combination of URL shards, archive paths, and failover options is limited by [glob_expansion_max_elements](/reference/settings/session-settings/other#glob_expansion_max_elements).

Archive reads support only `cluster_table_function_split_granularity = 'file'`. Bucket-level splitting operates on blocks of a plain object and cannot preserve an archive member identity, so `cluster_table_function_split_granularity = 'bucket'` is rejected for archives.

## Globs in URL {#globs-in-url}

Patterns in `{ }` are used to generate a set of shards or to specify failover addresses. Supported pattern types and examples see in the description of the [remote](/reference/functions/table-functions/remote#globs-in-addresses) function.
Character `|` inside patterns is used to specify failover addresses. They are iterated in the same order as listed in the pattern. The number of generated addresses is limited by [glob_expansion_max_elements](/reference/settings/session-settings/other#glob_expansion_max_elements) setting.

With [allow_experimental_url_wildcard_from_index_pages](/reference/settings/session-settings/allow-experimental#allow_experimental_url_wildcard_from_index_pages) enabled, `urlCluster` expands `*` and `**` in URL paths by reading HTTP index pages and distributes the matched files across the cluster.

## Related {#related}

-   [HDFS engine](/reference/engines/table-engines/integrations/hdfs)
-   [URL table function](/reference/engines/table-engines/special/url)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction});
}

}
