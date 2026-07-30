#include "config.h"

#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionObjectStorageCluster.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Storages/ObjectStorage/StorageObjectStorageCluster.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Common/CurrentThread.h>
#include <Interpreters/Context.h>


namespace DB
{

ContextPtr getQueryOrGlobalContext()
{
    if (auto query_context = CurrentThread::tryGetQueryContext(); query_context != nullptr)
        return query_context;
    return Context::getGlobalContextInstance();
}

template <typename Definition, typename Configuration, bool is_data_lake>
StoragePtr TableFunctionObjectStorageCluster<Definition, Configuration, is_data_lake>::executeImpl(
    const ASTPtr & /*function*/, ContextPtr context,
    const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const
{
    auto configuration = Base::getConfiguration(context);

    ColumnsDescription columns;
    if (configuration->structure != "auto")
        columns = parseColumnsListFromString(configuration->structure, context);
    else if (!Base::structure_hint.empty())
        columns = Base::structure_hint;
    else if (!cached_columns.empty())
        columns = cached_columns;

    auto object_storage = Base::getObjectStorage(context, !is_insert_query);
    StoragePtr storage;

    const auto & client_info = context->getClientInfo();

    if (client_info.query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        /// Like urlCluster/fileCluster, always request a distributed read on a secondary query and
        /// let the initiator decide: it serves the read task when it installed an iterator (the
        /// legitimate top-level and INSERT-SELECT dispatch) and rejects it otherwise (the nested
        /// cluster-function shapes), so every *Cluster function behaves the same way.
        /// On worker node this filename won't contains globs
        storage = std::make_shared<StorageObjectStorage>(
            configuration,
            object_storage,
            context,
            StorageID(Base::getDatabaseName(), table_name),
            columns,
            ConstraintsDescription{},
            /* comment */ String{},
            /* format_settings */ std::nullopt, /// No format_settings
            /* mode */ LoadingStrictnessLevel::CREATE,
            /* catalog*/nullptr,
            /* if_not_exists*/false,
            /* is_datalake_query*/ false,
            /* distributed_processing */ true,
            /* partition_by_ */Base::partition_by,
            /* order_by_ */nullptr,
            /* is_table_function */true,
            /* lazy_init */ true);
    }
    else
    {
        storage = std::make_shared<StorageObjectStorageCluster>(
            ITableFunctionCluster<Base>::cluster_name,
            configuration,
            object_storage,
            StorageID(Base::getDatabaseName(), table_name),
            columns,
            ConstraintsDescription{},
            Base::partition_by,
            context,
            /* is_table_function */true);
    }

    storage->startup();
    return storage;
}


void registerTableFunctionObjectStorageCluster(TableFunctionFactory & factory)
{
#if USE_AWS_S3
    factory.registerFunction<TableFunctionS3Cluster>(
        {.description = R"DOCS_MD(
This is an extension to the [s3](/reference/functions/table-functions/s3) table function.

Allows processing files from [Amazon S3](https://aws.amazon.com/s3/) and Google Cloud Storage [Google Cloud Storage](https://cloud.google.com/storage/) in parallel with many nodes in a specified cluster. On initiator it creates a connection to all nodes in the cluster, discloses asterisks in S3 file path, and dispatches each file dynamically. On the worker node it asks the initiator about the next task to process and processes it. This is repeated until all tasks are finished.

## Syntax {#syntax}

```sql
s3Cluster(cluster_name, url[, NOSIGN | access_key_id, secret_access_key,[session_token]][, format][, structure][, compression_method][, headers][, extra_credentials])
s3Cluster(cluster_name, named_collection[, option=value [,..]])
```

## Arguments {#arguments}

| Argument                              | Description                                                                                                                                                                                             |
|---------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `cluster_name`                        | Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.                                                                                         |
| `url`                                 | path to a file or a bunch of files. Supports following wildcards in readonly mode: `*`, `**`, `?`, `{'abc','def'}` and `{N..M}` where `N`, `M` — numbers, `abc`, `def` — strings. For more information see [Wildcards In Path](/reference/engines/table-engines/integrations/s3#wildcards-in-path). |
| `NOSIGN`                              | If this keyword is provided in place of credentials, all the requests will not be signed.                                                                                                             |
| `access_key_id` and `secret_access_key` | Keys that specify credentials to use with given endpoint. Optional.                                                                                                                                     |
| `session_token`                       | Session token to use with the given keys. Optional when passing keys.                                                                                                                                 |
| `format`                              | The [format](/reference/formats/index) of the file.                                                                                                                                                         |
| `structure`                           | Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                          |
| `compression_method`                  | Parameter is optional. Supported values: `none`, `gzip` or `gz`, `brotli` or `br`, `xz` or `LZMA`, `zstd` or `zst`. By default, it will autodetect compression method by file extension.                 |
| `headers`                             | Parameter is optional. Allows headers to be passed in the S3 request. Pass in the format `headers(key=value)` e.g. `headers('x-amz-request-payer' = 'requester')`. See [here](/reference/functions/table-functions/s3#accessing-requester-pays-buckets) for example of use. |
| `extra_credentials`                   | Optional. `roleARN` can be passed via this parameter. See [here](/products/cloud/guides/data-sources/accessing-s3-data-securely#access-your-s3-bucket-with-the-clickhouseaccess-role) for an example.                                          |

Arguments can also be passed using [named collections](/concepts/features/configuration/server-config/named-collections). In this case `url`, `access_key_id`, `secret_access_key`, `format`, `structure`, `compression_method` work in the same way, and some extra parameters are supported:

| Argument                       | Description                                                                                                                                                                                                                       |
|--------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `filename`                     | appended to the url if specified.                                                                                                                                                                                                 |
| `use_environment_credentials`  | enabled by default, allows passing extra parameters using environment variables `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `AWS_CONTAINER_CREDENTIALS_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN`, `AWS_EC2_METADATA_DISABLED`. |
| `no_sign_request`              | disabled by default.                                                                                                                                                                                                              |
| `expiration_window_seconds`    | default value is 120.                                                                                                                                                                                                             |

## Returned value {#returned_value}

A table with the specified structure for reading or writing data in the specified file.

## Examples {#examples}

Select the data from all the files in the `/root/data/clickhouse` and `/root/data/database/` folders, using all the nodes in the `cluster_simple` cluster:

```sql
SELECT * FROM s3Cluster(
    'cluster_simple',
    'http://minio1:9001/root/data/{clickhouse,database}/*',
    'minio',
    'ClickHouse_Minio_P@ssw0rd',
    'CSV',
    'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))'
) ORDER BY (name, value, polygon);
```

Count the total amount of rows in all files in the cluster `cluster_simple`:

<Tip>
If your listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.
</Tip>

For production use cases, it is recommended to use [named collections](/concepts/features/configuration/server-config/named-collections). Here is the example:
```sql

CREATE NAMED COLLECTION creds AS
        access_key_id = 'minio',
        secret_access_key = 'ClickHouse_Minio_P@ssw0rd';
SELECT count(*) FROM s3Cluster(
    'cluster_simple', creds, url='https://s3-object-url.csv',
    format='CSV', structure='name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))'
)
```

## Accessing private and public buckets {#accessing-private-and-public-buckets}

Users can use the same approaches as document for the s3 function [here](/reference/functions/table-functions/s3#accessing-public-buckets).

## Optimizing performance {#optimizing-performance}

For details on optimizing the performance of the s3 function see [our detailed guide](/integrations/connectors/data-ingestion/AWS/performance).

## Related {#related}

- [S3 engine](/reference/engines/table-engines/integrations/s3)
- [s3 table function](/reference/functions/table-functions/s3)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction},
        {.allow_readonly = false}
    );
#endif

#if USE_AZURE_BLOB_STORAGE
    factory.registerFunction<TableFunctionAzureBlobCluster>(
        {.description = R"DOCS_MD(
Allows processing files from [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) in parallel with many nodes in a specified cluster. On initiator it creates a connection to all nodes in the cluster, discloses asterisks in S3 file path, and dispatches each file dynamically. On the worker node it asks the initiator about the next task to process and processes it. This is repeated until all tasks are finished.
This table function is similar to the [s3Cluster function](/reference/functions/table-functions/s3Cluster).

## Syntax {#syntax}

```sql
azureBlobStorageCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])
```

## Arguments {#arguments}

| Argument            | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `cluster_name`      | Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.                                                                                                                                                                                                                                                                                                                                                                                                 |
| `connection_string` | storage_account_url` — connection_string includes account name & key ([Create connection string](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)) or you could also provide the storage account url here and account name & account key as separate parameters (see parameters account_name & account_key) | 
| `container_name`    | Container name                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
| `blobpath`          | file path. Supports following wildcards in readonly mode: `*`, `**`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc'`, `'def'` — strings.                                                                                                                                                                                                                                                                                                                                                          |
| `account_name`      | if storage_account_url is used, then account name can be specified here                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `account_key`       | if storage_account_url is used, then account key can be specified here                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `format`            | The [format](/reference/formats/index) of the file.                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `compression`       | Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. By default, it will autodetect compression by file extension. (same as setting to `auto`).                                                                                                                                                                                                                                                                                                                                               |
| `structure`         |  Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                                                                                                                                                    |

## Returned value {#returned_value}

A table with the specified structure for reading or writing data in the specified file.

## Examples {#examples}

Similar to the [AzureBlobStorage](/reference/engines/table-engines/integrations/azureBlobStorage) table engine, users can use Azurite emulator for local Azure Storage development. Further details [here](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub%2Cblob-storage). Below we assume Azurite is available at the hostname `azurite1`.

Select the count for the file `test_cluster_*.csv`, using all the nodes in the `cluster_simple` cluster:

```sql
SELECT count(*) FROM azureBlobStorageCluster(
        'cluster_simple', 'http://azurite1:10000/devstoreaccount1', 'testcontainer', 'test_cluster_count.csv', 'devstoreaccount1',
        'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV',
        'auto', 'key UInt64')
```

## Using Shared Access Signatures (SAS) {#using-shared-access-signatures-sas-sas-tokens}

See [azureBlobStorage](/reference/functions/table-functions/azureBlobStorage#using-shared-access-signatures-sas-sas-tokens) for examples.

## Related {#related}

- [AzureBlobStorage engine](/reference/engines/table-engines/integrations/azureBlobStorage)
- [azureBlobStorage table function](/reference/functions/table-functions/azureBlobStorage)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction},
        {.allow_readonly = false}
    );
#endif

#if USE_HDFS
    factory.registerFunction<TableFunctionHDFSCluster>(
        {.description = R"DOCS_MD(
Allows processing files from HDFS in parallel from many nodes in a specified cluster. On initiator it creates a connection to all nodes in the cluster, discloses asterisks in HDFS file path, and dispatches each file dynamically. On the worker node it asks the initiator about the next task to process and processes it. This is repeated until all tasks are finished.

## Syntax {#syntax}

```sql
hdfsCluster(cluster_name, URI, format, structure)
```

## Arguments {#arguments}

| Argument       | Description                                                                                                                                                                                                                                                                                      |
|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `cluster_name` | Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.                                                                                                                                                                                |
| `URI`          | URI to a file or a bunch of files. Supports following wildcards in readonly mode: `*`, `**`, `?`, `{'abc','def'}` and `{N..M}` where `N`, `M` — numbers, `abc`, `def` — strings. For more information see [Wildcards In Path](/reference/engines/table-engines/integrations/s3#wildcards-in-path). |
| `format`       | The [format](/reference/formats/index) of the file.                                                                                                                                                                                                                                                |
| `structure`    | Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                    |

## Returned value {#returned_value}

A table with the specified structure for reading data in the specified file.

## Examples {#examples}

1.  Suppose that we have a ClickHouse cluster named `cluster_simple`, and several files with following URIs on HDFS:

- 'hdfs://hdfs1:9000/some_dir/some_file_1'
- 'hdfs://hdfs1:9000/some_dir/some_file_2'
- 'hdfs://hdfs1:9000/some_dir/some_file_3'
- 'hdfs://hdfs1:9000/another_dir/some_file_1'
- 'hdfs://hdfs1:9000/another_dir/some_file_2'
- 'hdfs://hdfs1:9000/another_dir/some_file_3'

2.  Query the amount of rows in these files:

```sql
SELECT count(*)
FROM hdfsCluster('cluster_simple', 'hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3.  Query the amount of rows in all files of these two directories:

```sql
SELECT count(*)
FROM hdfsCluster('cluster_simple', 'hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

<Note>
If your listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.
</Note>

## Related {#related}

- [HDFS engine](/reference/engines/table-engines/integrations/hdfs)
- [HDFS table function](/reference/functions/table-functions/hdfs)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction},
        {.allow_readonly = false}
    );
#endif

    UNUSED(factory);
}


#if USE_AVRO
void registerTableFunctionIcebergCluster(TableFunctionFactory & factory);
void registerTableFunctionIcebergCluster(TableFunctionFactory & factory)
{
    UNUSED(factory);

    factory.registerFunction<TableFunctionIcebergLocalCluster>(
        {
            .description = R"(The table function can be used to read the Iceberg table stored on shared storage in parallel for many nodes in a specified cluster.)",
            .examples{{IcebergLocalClusterDefinition::name, "SELECT * FROM icebergLocalCluster(cluster, filename, format, [,compression])", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        {.allow_readonly = false}
    );

#if USE_AWS_S3
    factory.registerFunction<TableFunctionIcebergCluster>(
        {.description = R"DOCS_MD(
This is an extension to the [iceberg](/reference/functions/table-functions/iceberg) table function.

Allows processing files from Apache [Iceberg](https://iceberg.apache.org/) in parallel from many nodes in a specified cluster. On initiator it creates a connection to all nodes in the cluster and dispatches each file dynamically. On the worker node it asks the initiator about the next task to process and processes it. This is repeated until all tasks are finished.

## Syntax {#syntax}

```sql
icebergS3Cluster(cluster_name, url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method] [,extra_credentials])
icebergS3Cluster(cluster_name, named_collection[, option=value [,..]])

icebergAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzureCluster(cluster_name, named_collection[, option=value [,..]])

icebergHDFSCluster(cluster_name, path_to_table, [,format] [,compression_method])
icebergHDFSCluster(cluster_name, named_collection[, option=value [,..]])
```

## Arguments {#arguments}

- `cluster_name` — Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.
- Description of all other arguments coincides with description of arguments in equivalent [iceberg](/reference/functions/table-functions/iceberg) table function.
- An optional `extra_credentials` parameter can be used to pass a `role_arn` for role-based access in ClickHouse Cloud. See [Secure S3](/products/cloud/guides/data-sources/accessing-s3-data-securely) for configuration steps.

**Returned value**

A table with the specified structure for reading data from cluster in the specified Iceberg table.

**Examples**

```sql
SELECT * FROM icebergS3Cluster('cluster_simple', 'http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_etag` — The etag of the file. Type: `LowCardinality(String)`. If the etag is unknown, the value is `NULL`.

**See Also**

- [Iceberg engine](/reference/engines/table-engines/integrations/iceberg)
- [Iceberg table function](/reference/functions/table-functions/iceberg)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction},
        {.allow_readonly = false}
    );

    factory.registerFunction<TableFunctionIcebergS3Cluster>(
        {
            .description = R"(The table function can be used to read the Iceberg table stored on S3 object store in parallel for many nodes in a specified cluster.)",
            .examples{{IcebergS3ClusterDefinition::name, "SELECT * FROM icebergS3Cluster(cluster, url, [, NOSIGN | access_key_id, secret_access_key, [session_token]], format, [,compression])", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        {.allow_readonly = false}
    );
#endif

#if USE_AZURE_BLOB_STORAGE
    factory.registerFunction<TableFunctionIcebergAzureCluster>(
        {
            .description = R"(The table function can be used to read the Iceberg table stored on Azure object store in parallel for many nodes in a specified cluster.)",
            .examples{{IcebergAzureClusterDefinition::name, "SELECT * FROM icebergAzureCluster(cluster, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        {.allow_readonly = false}
    );
#endif

#if USE_HDFS
    factory.registerFunction<TableFunctionIcebergHDFSCluster>(
        {
            .description = R"(The table function can be used to read the Iceberg table stored on HDFS virtual filesystem in parallel for many nodes in a specified cluster.)",
            .examples{{IcebergHDFSClusterDefinition::name, "SELECT * FROM icebergHDFSCluster(cluster, uri, [format], [structure], [compression_method])", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        {.allow_readonly = false}
    );
#endif
}

void registerTableFunctionPaimonCluster(TableFunctionFactory & factory);
void registerTableFunctionPaimonCluster(TableFunctionFactory & factory)
{
    UNUSED(factory);

#if USE_AWS_S3
    factory.registerFunction<TableFunctionPaimonCluster>(
        {.description = R"DOCS_MD(
import ExperimentalBadge from "/snippets/components/ExperimentalBadge/ExperimentalBadge.jsx";

<ExperimentalBadge />

This is an extension to the [paimon](/reference/functions/table-functions/paimon) table function.

Allows processing files from Apache [Paimon](https://paimon.apache.org/) in parallel from many nodes in a specified cluster. On initiator it creates a connection to all nodes in the cluster and dispatches each file dynamically. On the worker node it asks the initiator about the next task to process and processes it. This is repeated until all tasks are finished.

## Syntax {#syntax}

```sql
paimonS3Cluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

paimonAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

paimonHDFSCluster(cluster_name, path_to_table, [,format] [,compression_method])
```

## Arguments {#arguments}

- `cluster_name` — Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.
- Description of all other arguments coincides with description of arguments in equivalent [paimon](/reference/functions/table-functions/paimon) table function.
- An optional `extra_credentials` parameter can be used to pass a `role_arn` for role-based access in ClickHouse Cloud. See [Secure S3](/products/cloud/guides/data-sources/accessing-s3-data-securely) for configuration steps.

**Returned value**

A table with the specified structure for reading data from cluster in the specified Paimon table.

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_etag` — The etag of the file. Type: `LowCardinality(String)`. If the etag is unknown, the value is `NULL`.

**See Also**

- [Paimon table function](/reference/functions/table-functions/paimon)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction},
        {.allow_readonly = false}
    );

    factory.registerFunction<TableFunctionPaimonS3Cluster>(
        {
            .description = R"(The table function can be used to read the Paimon table stored on S3 object store in parallel for many nodes in a specified cluster.)",
            .examples{{PaimonS3ClusterDefinition::name, "SELECT * FROM paimonS3Cluster(cluster, url, [, NOSIGN | access_key_id, secret_access_key, [session_token]], format, [,compression])", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        {.allow_readonly = false}
    );
#endif

#if USE_AZURE_BLOB_STORAGE
    factory.registerFunction<TableFunctionPaimonAzureCluster>(
        {
            .description = R"(The table function can be used to read the Paimon table stored on Azure object store in parallel for many nodes in a specified cluster.)",
            .examples{{PaimonAzureClusterDefinition::name, "SELECT * FROM paimonAzureCluster(cluster, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        {.allow_readonly = false}
    );
#endif

#if USE_HDFS
    factory.registerFunction<TableFunctionPaimonHDFSCluster>(
        {
            .description = R"(The table function can be used to read the Paimon table stored on HDFS virtual filesystem in parallel for many nodes in a specified cluster.)",
            .examples{{PaimonHDFSClusterDefinition::name, "SELECT * FROM paimonHDFSCluster(cluster, uri, [format], [structure], [compression_method])", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        {.allow_readonly = false}
    );
#endif
}
#endif


#if USE_PARQUET
void registerTableFunctionDeltaLakeCluster(TableFunctionFactory & factory);
void registerTableFunctionDeltaLakeCluster(TableFunctionFactory & factory)
{
    UNUSED(factory);
#if USE_AWS_S3 && USE_DELTA_KERNEL_RS
    factory.registerFunction<TableFunctionDeltaLakeCluster>(
        {.description = R"DOCS_MD(
This is an extension to the [deltaLake](/reference/functions/table-functions/deltalake) table function.

Allows processing files from [Delta Lake](https://github.com/delta-io/delta) tables in Amazon S3 in parallel from many nodes in a specified cluster. On initiator it creates a connection to all nodes in the cluster and dispatches each file dynamically. On the worker node it asks the initiator about the next task to process and processes it. This is repeated until all tasks are finished.

## Syntax {#syntax}

```sql
deltaLakeCluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
deltaLakeCluster(cluster_name, named_collection[, option=value [,..]])

deltaLakeS3Cluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
deltaLakeS3Cluster(cluster_name, named_collection[, option=value [,..]])

deltaLakeAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
deltaLakeAzureCluster(cluster_name, named_collection[, option=value [,..]])
```
`deltaLakeS3Cluster` is an alias to `deltaLakeCluster`, both are for S3. 

## Arguments {#arguments}

- `cluster_name` — Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.
- Description of all other arguments coincides with description of arguments in equivalent [deltaLake](/reference/functions/table-functions/deltalake) table function.
- An optional `extra_credentials` parameter can be used to pass a `role_arn` for role-based access in ClickHouse Cloud. See [Secure S3](/products/cloud/guides/data-sources/accessing-s3-data-securely) for configuration steps.

## Returned value {#returned_value}

A table with the specified structure for reading data from cluster in the specified Delta Lake table in S3.

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_etag` — The etag of the file. Type: `LowCardinality(String)`. If the etag is unknown, the value is `NULL`.

## Related {#related}

- [deltaLake engine](/reference/engines/table-engines/integrations/deltalake)
- [deltaLake table function](/reference/functions/table-functions/deltalake)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction},
        {.allow_readonly = false}
    );
    factory.registerFunction<TableFunctionDeltaLakeS3Cluster>(
        {
            .description = R"(The table function can be used to read the DeltaLake table stored on S3 object store in parallel for many nodes in a specified cluster.)",
            .examples{{DeltaLakeS3ClusterDefinition::name, "SELECT * FROM deltaLakeS3Cluster(cluster, url, access_key_id, secret_access_key)", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        {.allow_readonly = false}
    );
#endif

#if USE_AZURE_BLOB_STORAGE && USE_DELTA_KERNEL_RS
    factory.registerFunction<TableFunctionDeltaLakeAzureCluster>(
        {
            .description = R"(The table function can be used to read the DeltaLake table stored on Azure object store in parallel for many nodes in a specified cluster.)",
            .examples{{DeltaLakeAzureClusterDefinition::name, "SELECT * FROM deltaLakeAzureCluster(cluster, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        {.allow_readonly = false}
    );
#endif
}
#endif

#if USE_AWS_S3
void registerTableFunctionHudiCluster(TableFunctionFactory & factory);
void registerTableFunctionHudiCluster(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHudiCluster>(
        {.description = R"DOCS_MD(
This is an extension to the [hudi](/reference/functions/table-functions/hudi) table function.

Allows processing files from Apache [Hudi](https://hudi.apache.org/) tables in Amazon S3 in parallel with many nodes in a specified cluster. On initiator it creates a connection to all nodes in the cluster and dispatches each file dynamically. On the worker node it asks the initiator about the next task to process and processes it. This is repeated until all tasks are finished.

## Syntax {#syntax}

```sql
hudiCluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
```

## Arguments {#arguments}

| Argument                                     | Description                                                                                                                                                                                                                                                                                                                                                                           |
|----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `cluster_name`                               | Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.                                                                                                                                                                                                                                                                     |
| `url`                                        | Bucket url with the path to an existing Hudi table in S3.                                                                                                                                                                                                                                                                                                                             |
| `aws_access_key_id`, `aws_secret_access_key` | Long-term credentials for the [AWS](https://aws.amazon.com/) account user.  You can use these to authenticate your requests. These parameters are optional. If credentials are not specified, they are used from the ClickHouse configuration. For more information see [Using S3 for Data Storage](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-s3). |
| `format`                                     | The [format](/reference/formats/index) of the file.                                                                                                                                                                                                                                                                                                                                        |
| `structure`                                  | Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                         |
| `compression`                                | Parameter is optional. Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. By default, compression will be autodetected by the file extension.                                                                                                                                                                                                                   |
| `extra_credentials`                          | Parameter is optional. Used to pass a `role_arn` for role-based access in ClickHouse Cloud. See [Secure S3](/products/cloud/guides/data-sources/accessing-s3-data-securely) for configuration steps.                                                                                                                                                                                                                     |

## Returned value {#returned_value}

A table with the specified structure for reading data from cluster in the specified Hudi table in S3.

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_etag` — The etag of the file. Type: `LowCardinality(String)`. If the etag is unknown, the value is `NULL`.

## Related {#related}

- [Hudi engine](/reference/engines/table-engines/integrations/hudi)
- [Hudi table function](/reference/functions/table-functions/hudi)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction},
        {.allow_readonly = false}
    );
}
#endif

void registerDataLakeClusterTableFunctions(TableFunctionFactory & factory)
{
    UNUSED(factory);
#if USE_AVRO
    registerTableFunctionIcebergCluster(factory);
    registerTableFunctionPaimonCluster(factory);
#endif
#if USE_PARQUET
    registerTableFunctionDeltaLakeCluster(factory);
#endif
#if USE_AWS_S3
    registerTableFunctionHudiCluster(factory);
#endif
}

}
