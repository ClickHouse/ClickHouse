#include <string_view>
#include "config.h"

#include <Core/Settings.h>
#include <Core/SettingsEnums.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSetQuery.h>

#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionObjectStorage.h>
#include <TableFunctions/TableFunctionObjectStorageCluster.h>
#include <TableFunctions/registerTableFunctions.h>

#include <Interpreters/parseColumnsListForTableFunction.h>

#include <Storages/ObjectStorage/Utils.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/Local/Configuration.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageCluster.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/HivePartitioningUtils.h>


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsBool parallel_replicas_for_cluster_engines;
    extern const SettingsString cluster_for_parallel_replicas;
    extern const SettingsParallelReplicasMode parallel_replicas_mode;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace DataLakeStorageSetting
{
    extern const DataLakeStorageSettingsString disk;
}

template <typename Definition, typename Configuration, bool is_data_lake>
ObjectStoragePtr TableFunctionObjectStorage<Definition, Configuration, is_data_lake>::getObjectStorage(const ContextPtr & context, bool create_readonly) const
{
    if (!object_storage)
        object_storage = configuration->createObjectStorage(context, create_readonly, std::nullopt);
    return object_storage;
}

template <typename Definition, typename Configuration, bool is_data_lake>
StorageObjectStorageConfigurationPtr TableFunctionObjectStorage<Definition, Configuration, is_data_lake>::getConfiguration(ContextPtr context) const
{
    if (!configuration)
    {
        if constexpr (is_data_lake)
        {
            const auto disk_name = settings && (*settings)[DataLakeStorageSetting::disk].changed
                ? (*settings)[DataLakeStorageSetting::disk].value
                : "";
            if (!disk_name.empty())
            {
                auto disk = context->getDisk(disk_name);
                switch (disk->getObjectStorage()->getType())
                {
#if USE_AWS_S3 && USE_AVRO
                case ObjectStorageType::S3:
                    if (Definition::object_storage_type != "s3")
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk type doesn't match with table engine type storage");

                    if (std::string_view(Definition::name).starts_with("iceberg"))
                        configuration = std::make_shared<StorageS3IcebergConfiguration>(settings);
#if USE_PARQUET
                    else
                        configuration = std::make_shared<StorageS3DeltaLakeConfiguration>(settings);
#endif
                    break;
#endif
#if USE_AZURE_BLOB_STORAGE && USE_AVRO
                case ObjectStorageType::Azure:
                    if (Definition::name != "iceberg" &&
                        Definition::name != "icebergCluster" &&
                        Definition::name != "deltaLake" &&
                        Definition::name != "deltaLakeCluster" &&
                        Definition::object_storage_type != "azure")
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk type doesn't match with table engine type storage");

                    if (std::string_view(Definition::name).starts_with("iceberg"))
                        configuration = std::make_shared<StorageAzureIcebergConfiguration>(settings);
#if USE_PARQUET
                    else
                        configuration = std::make_shared<StorageAzureDeltaLakeConfiguration>(settings);
#endif
                    break;
#endif
#if USE_AVRO
                case ObjectStorageType::Local:
                    if (Definition::name != "iceberg" &&
                        Definition::name != "icebergCluster" &&
                        Definition::name != "deltaLake" &&
                        Definition::name != "deltaLakeCluster" &&
                        Definition::object_storage_type != "local")
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk type doesn't match with table engine type storage");
                    if (std::string_view(Definition::name).starts_with("iceberg"))
                        configuration = std::make_shared<StorageLocalIcebergConfiguration>(settings);
#if USE_PARQUET
                    else
                        configuration = std::make_shared<StorageLocalDeltaLakeConfiguration>(settings);
#endif
                    break;
#endif
                default:
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported disk type for iceberg {}", disk->getObjectStorage()->getType());
                }
            }
            else
                configuration = std::make_shared<Configuration>(settings);
        }
        else
            configuration = std::make_shared<Configuration>();
    }
    return configuration;
}

template <typename Definition, typename Configuration, bool is_data_lake>
VectorWithMemoryTracking<size_t> TableFunctionObjectStorage<Definition, Configuration, is_data_lake>::skipAnalysisForArguments(
    const QueryTreeNodePtr & query_node_table_function, ContextPtr) const
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

template <typename Definition, typename Configuration, bool is_data_lake>
std::shared_ptr<typename TableFunctionObjectStorage<Definition, Configuration, is_data_lake>::Settings>
TableFunctionObjectStorage<Definition, Configuration, is_data_lake>::createEmptySettings()
{
    if constexpr (is_data_lake)
        return std::make_shared<DataLakeStorageSettings>();
    else
        return std::make_shared<StorageObjectStorageSettings>();
}

template <typename Definition, typename Configuration, bool is_data_lake>
void TableFunctionObjectStorage<Definition, Configuration, is_data_lake>::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Clone ast function, because we can modify its arguments like removing headers.
    auto ast_copy = ast_function->clone();
    ASTs & args_func = ast_copy->children;
    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());

    settings = createEmptySettings();

    auto & args = args_func.at(0)->children;
    /// Support storage settings in table function,
    /// e.g. `s3(endpoint, ..., SETTINGS setting=value, ..., setting=value)`
    /// We do similarly for some other table functions
    /// whose storage implementation supports storage settings (for example, MySQL).
    for (auto it = args.begin(); it != args.end(); ++it)
    {
        ASTSetQuery * settings_ast = (*it)->as<ASTSetQuery>();
        if (settings_ast)
        {
            settings->loadFromQuery(*settings_ast);
            args.erase(it);
            break;
        }
    }
    parseArgumentsImpl(args, context);
}

template <typename Definition, typename Configuration, bool is_data_lake>
ColumnsDescription TableFunctionObjectStorage<
    Definition, Configuration, is_data_lake>::getActualTableStructure(ContextPtr context, bool is_insert_query) const
{
    if (configuration->structure == "auto")
    {
        auto storage = getObjectStorage(context, !is_insert_query);
        configuration->lazyInitializeIfNeeded(object_storage, context);

        std::string sample_path;
        ColumnsDescription columns;
        resolveSchemaAndFormat(
            columns,
            configuration->format,
            std::move(storage),
            configuration,
            /* format_settings */std::nullopt,
            sample_path,
            context);

        HivePartitioningUtils::setupHivePartitioningForObjectStorage(
            columns,
            configuration,
            sample_path,
            /* inferred_schema */ true,
            /* format_settings */ std::nullopt,
            context);

        return columns;
    }
    return parseColumnsListFromString(configuration->structure, context);
}

template <typename Definition, typename Configuration, bool is_data_lake>
StoragePtr TableFunctionObjectStorage<Definition, Configuration, is_data_lake>::executeImpl(
    const ASTPtr & /* ast_function */,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription cached_columns,
    bool is_insert_query) const
{
    chassert(configuration);
    ColumnsDescription columns;

    if (configuration->structure != "auto")
        columns = parseColumnsListFromString(configuration->structure, context);
    else if (!structure_hint.empty())
        columns = structure_hint;
    else if (!cached_columns.empty())
        columns = cached_columns;

    StoragePtr storage;
    const auto & query_settings = context->getSettingsRef();

    const auto parallel_replicas_cluster_name = query_settings[Setting::cluster_for_parallel_replicas].toString();
    /// Only use parallel replicas if the Cluster variant of this table function exists
    /// (e.g. `s3Cluster` for `s3`). Table functions without a Cluster variant (e.g. `paimonLocal`)
    /// cannot distribute work via task iterators, so distributing would just read all data on every replica.
    const auto can_use_parallel_replicas = !parallel_replicas_cluster_name.empty()
        && query_settings[Setting::parallel_replicas_for_cluster_engines]
        && context->canUseTaskBasedParallelReplicas()
        && !context->isDistributed()
        && TableFunctionFactory::instance().isTableFunctionName(String(name) + "Cluster");

    const auto is_secondary_query = context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY;

    if (can_use_parallel_replicas && !is_secondary_query && !is_insert_query)
    {
        storage = std::make_shared<StorageObjectStorageCluster>(
            parallel_replicas_cluster_name,
            configuration,
            getObjectStorage(context, !is_insert_query),
            StorageID(getDatabaseName(), table_name),
            columns,
            ConstraintsDescription{},
            partition_by,
            context,
            /* is_table_function */true);

        storage->startup();
        return storage;
    }

    std::string disk_name;
    if constexpr (is_data_lake)
    {
        disk_name = settings && (*settings)[DataLakeStorageSetting::disk].changed
            ? (*settings)[DataLakeStorageSetting::disk].value
            : "";
    }

    ObjectStoragePtr current_object_storage;
    if (configuration->isDataLakeConfiguration() && !disk_name.empty())
        current_object_storage = context->getDisk(disk_name)->getObjectStorage();
    else
        current_object_storage = getObjectStorage(context, !is_insert_query);

    /// Note: distributed_processing is always false for non-cluster table functions (s3, azure, etc.).
    /// Cluster table functions (s3Cluster, etc.) handle distributed processing in their own getStorage() method.
    storage = std::make_shared<StorageObjectStorage>(
        configuration,
        current_object_storage,
        context,
        StorageID(getDatabaseName(), table_name),
        columns,
        ConstraintsDescription{},
        /* comment */ String{},
        /* format_settings */ std::nullopt,
        /* mode */ LoadingStrictnessLevel::CREATE,
        /* catalog*/ nullptr,
        /* if_not_exists*/ false,
        /* is_datalake_query*/ false,
        /* distributed_processing */ false,
        /* partition_by */ partition_by,
        /* order_by */ nullptr,
        /* is_table_function */true);

    storage->startup();
    return storage;
}

void registerTableFunctionObjectStorage(TableFunctionFactory & factory)
{
    UNUSED(factory);
#if USE_AWS_S3
    factory.registerFunction<TableFunctionObjectStorage<S3Definition, StorageS3Configuration>>(
        {.description = R"DOCS_MD(
import ExperimentalBadge from "/snippets/components/ExperimentalBadge/ExperimentalBadge.jsx";
import CloudNotSupportedBadge from "/snippets/components/CloudNotSupportedBadge/CloudNotSupportedBadge.jsx";

Provides a table-like interface to select/insert files in [Amazon S3](https://aws.amazon.com/s3/) and [Google Cloud Storage](https://cloud.google.com/storage/). This table function is similar to the [hdfs function](/reference/functions/table-functions/hdfs), but provides S3-specific features.

If you have multiple replicas in your cluster, you can use the [s3Cluster function](/reference/functions/table-functions/s3Cluster) instead to parallelize inserts.

When using the `s3 table function` with [`INSERT INTO...SELECT`](/reference/statements/insert-into#inserting-the-results-of-select), data is read and inserted in a streaming fashion. Only a few blocks of data reside in memory while the blocks are continuously read from S3 and pushed into the destination table.

## Syntax {#syntax}

```sql
s3(url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,structure] [,compression_method],[,headers], [,extra_credentials], [,partition_strategy], [,partition_columns_in_data_file])
s3(named_collection[, option=value [,..]])
```

<Tip>
**GCS**

The S3 Table Function integrates with Google Cloud Storage by using the GCS XML API and HMAC keys.  See the [Google interoperability docs](https://cloud.google.com/storage/docs/interoperability) for more details about the endpoint and HMAC.

For GCS, substitute your HMAC key and HMAC secret where you see `access_key_id` and `secret_access_key`.
</Tip>

**Parameters**

`s3` table function supports the following plain parameters:

| Parameter                               | Description                                                                                                                                                                                                                                                                                                                                                                      |
|-----------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `url`                                   | Bucket url with path to file. Supports following wildcards in readonly mode: `*`, `**`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc'`, `'def'` — strings. For more information see [here](/reference/engines/table-engines/integrations/s3#wildcards-in-path).                                                                                                   |
| `NOSIGN`                                | If this keyword is provided in place of credentials, all the requests will not be signed.                                                                                                                                                                                                                                                                                        |
| `access_key_id` and `secret_access_key` | Keys that specify credentials to use with given endpoint. Optional.                                                                                                                                                                                                                                                                                                              |
| `session_token`                         | Session token to use with the given keys. Optional when passing keys.                                                                                                                                                                                                                                                                                                            |
| `format`                                | The [format](/reference/formats/index) of the file.                                                                                                                                                                                                                                                                                                                                |
| `structure`                             | Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                    |
| `compression_method`                    | Parameter is optional. Supported values: `none`, `gzip` or `gz`, `brotli` or `br`, `xz` or `LZMA`, `zstd` or `zst`. By default, it will autodetect compression method by file extension.                                                                                                                                                                                         |
| `headers`                               | Parameter is optional. Allows headers to be passed in the S3 request. Pass in the format `headers(key=value)` e.g. `headers('x-amz-request-payer' = 'requester')`.                                                                                                                                                                                                               |
| `partition_strategy`                    | Parameter is optional. Supported values: `wildcard` or `hive`. `wildcard` requires a `{_partition_id}` in the path, which is replaced with the partition key. `hive` does not allow wildcards, assumes the path is the table root, and generates Hive-style partitioned directories with Snowflake IDs as filenames and the file format as the extension. Defaults to the `file_like_engine_default_partition_strategy` setting (`wildcard` under `compatibility` settings older than `26.6`, `hive` otherwise). |
| `partition_columns_in_data_file`        | Parameter is optional. Only used with `hive` partition strategy. Tells ClickHouse whether to expect partition columns to be written in the data file. Defaults `false`.                                                                                                                                                                                                          |
| `extra_credentials`                     | Parameter is optional. Used to pass a `role_arn` for role-based access in ClickHouse Cloud. See [Secure S3](/products/cloud/guides/data-sources/accessing-s3-data-securely) for configuration steps. |
| `storage_class_name`                    | Parameter is optional. Supported values: `STANDARD`, `REDUCED_REDUNDANCY`, `STANDARD_IA`, `ONEZONE_IA`, `INTELLIGENT_TIERING`, `GLACIER_IR`, `EXPRESS_ONEZONE`. Only S3 storage classes that allow immediate retrieval are supported (archival classes such as `GLACIER` and `DEEP_ARCHIVE` are not). Allows to specify [AWS S3 Intelligent Tiering](https://aws.amazon.com/s3/storage-classes/intelligent-tiering/). Defaults to `STANDARD`. |

<Info>
**GCS**

The GCS url is in this format as the endpoint for the Google XML API is different than the JSON API:

```text
  https://storage.googleapis.com/<bucket>/<folder>/<filename(s)>
```

and not ~~https://storage.cloud.google.com~~.
</Info>

Arguments can also be passed using [named collections](/concepts/features/configuration/server-config/named-collections). In this case `url`, `access_key_id`, `secret_access_key`, `format`, `structure`, `compression_method` work in the same way, and some extra parameters are supported:

| Argument                      | Description                                                                                                                                                                       |
|-------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `filename`                    | appended to the url if specified.                                                                                                                                                 |
| `use_environment_credentials` | enabled by default, allows passing extra parameters using environment variables `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `AWS_CONTAINER_CREDENTIALS_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN`, `AWS_EC2_METADATA_DISABLED`. |
| `no_sign_request`             | disabled by default.                                                                                                                                                              |
| `expiration_window_seconds`   | default value is 120.                                                                                                                                                             |

## Returned value {#returned_value}

A table with the specified structure for reading or writing data in the specified file.

## Examples {#examples}

Selecting the first 5 rows from the table from S3 file `https://datasets-documentation.s3.eu-west-3.amazonaws.com/aapl_stock.csv`:

```sql
SELECT *
FROM s3(
   'https://datasets-documentation.s3.eu-west-3.amazonaws.com/aapl_stock.csv',
   'CSVWithNames'
)
LIMIT 5;
```

```response
┌───────Date─┬────Open─┬────High─┬─────Low─┬───Close─┬───Volume─┬─OpenInt─┐
│ 1984-09-07 │ 0.42388 │ 0.42902 │ 0.41874 │ 0.42388 │ 23220030 │       0 │
│ 1984-09-10 │ 0.42388 │ 0.42516 │ 0.41366 │ 0.42134 │ 18022532 │       0 │
│ 1984-09-11 │ 0.42516 │ 0.43668 │ 0.42516 │ 0.42902 │ 42498199 │       0 │
│ 1984-09-12 │ 0.42902 │ 0.43157 │ 0.41618 │ 0.41618 │ 37125801 │       0 │
│ 1984-09-13 │ 0.43927 │ 0.44052 │ 0.43927 │ 0.43927 │ 57822062 │       0 │
└────────────┴─────────┴─────────┴─────────┴─────────┴──────────┴─────────┘
```

<Note>
ClickHouse uses filename extensions to determine the format of the data. For example, we could have run the previous command without the `CSVWithNames`:

```sql
SELECT *
FROM s3(
   'https://datasets-documentation.s3.eu-west-3.amazonaws.com/aapl_stock.csv'
)
LIMIT 5;
```

ClickHouse also can determine the compression method of the file. For example, if the file was zipped up with a `.csv.gz` extension, ClickHouse would decompress the file automatically.
</Note>

<Note>
Parquet files with names like `*.parquet.snappy` or `*.parquet.zstd` can confuse ClickHouse and cause `TOO_LARGE_COMPRESSED_BLOCK` or `ZSTD_DECODER_FAILED` errors.
This is because ClickHouse would attempt to read the entire file as Snappy or ZSTD-encoded data when, in fact, Parquet applies compression at the row-group and column level.

Parquet metadata already specifies the per-column compression, and so the file extension is superfluous.
You can just use `compression_method = 'none'` in such cases:

```sql
SELECT *
FROM s3(
  'https://<my-bucket>.s3.<my-region>.amazonaws.com/path/to/my-data.parquet.snappy',
  compression_format = 'none'
);
```
</Note>

## Usage {#usage}

Suppose that we have several files with following URIs on S3:

- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/some_prefix/some_file_1.csv'
- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/some_prefix/some_file_2.csv'
- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/some_prefix/some_file_3.csv'
- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/some_prefix/some_file_4.csv'
- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/another_prefix/some_file_1.csv'
- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/another_prefix/some_file_2.csv'
- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/another_prefix/some_file_3.csv'
- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/another_prefix/some_file_4.csv'

Count the number of rows in files ending with numbers from 1 to 3:

```sql
SELECT count(*)
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/my-test-bucket-768/{some,another}_prefix/some_file_{1..3}.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
```

```text
┌─count()─┐
│      18 │
└─────────┘
```

Count the total amount of rows in all files in these two directories:

```sql
SELECT count(*)
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/my-test-bucket-768/{some,another}_prefix/*', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
```

```text
┌─count()─┐
│      24 │
└─────────┘
```

<Tip>
If your listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.
</Tip>

Count the total amount of rows in files named `file-000.csv`, `file-001.csv`, ... , `file-999.csv`:

```sql
SELECT count(*)
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/my-test-bucket-768/big_prefix/file-{000..999}.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32');
```

```text
┌─count()─┐
│      12 │
└─────────┘
```

Insert data into file `test-data.csv.gz`:

```sql
INSERT INTO FUNCTION s3('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
VALUES ('test-data', 1), ('test-data-2', 2);
```

Insert data into file `test-data.csv.gz` from existing table:

```sql
INSERT INTO FUNCTION s3('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
SELECT name, value FROM existing_table;
```

Glob ** can be used for recursive directory traversal. Consider the below example, it will fetch all files from `my-test-bucket-768` directory recursively:

```sql
SELECT * FROM s3('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/**', 'CSV', 'name String, value UInt32', 'gzip');
```

The below get data from all `test-data.csv.gz` files from any folder inside `my-test-bucket` directory recursively:

```sql
SELECT * FROM s3('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/**/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip');
```

Note. It is possible to specify custom URL mappers in the server configuration file. Example:
```sql
SELECT * FROM s3('s3://clickhouse-public-datasets/my-test-bucket-768/**/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip');
```
The URL `'s3://clickhouse-public-datasets/my-test-bucket-768/**/test-data.csv.gz'` would be replaced to `'http://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/**/test-data.csv.gz'`

Custom mapper can be added into `config.xml`:
```xml
<url_scheme_mappers>
   <s3>
      <to>https://{bucket}.s3.amazonaws.com</to>
   </s3>
   <gs>
      <to>https://{bucket}.storage.googleapis.com</to>
   </gs>
   <oss>
      <to>https://{bucket}.oss.aliyuncs.com</to>
   </oss>
</url_scheme_mappers>
```

For production use cases it is recommended to use [named collections](/concepts/features/configuration/server-config/named-collections). Here is the example:
```sql

CREATE NAMED COLLECTION creds AS
        access_key_id = '***',
        secret_access_key = '***';
SELECT count(*)
FROM s3(creds, url='https://s3-object-url.csv')
```

## Partitioned Write {#partitioned-write}

### Partition Strategy {#partition-strategy}

Supported for INSERT queries only.

`wildcard`: Replaces the `{_partition_id}` wildcard in the file path with the actual partition key. It is selected by default only when the `compatibility` setting is older than `26.6`. When `compatibility` is set to `26.6` or later, or when current defaults apply, the default is `hive`, so paths containing `{_partition_id}` must explicitly set `partition_strategy='wildcard'` (see the `file_like_engine_default_partition_strategy` setting).

`hive` implements hive style partitioning for reads & writes. It generates files using the following format: `<prefix>/<key1=val1/key2=val2...>/<snowflakeid>.<toLower(file_format)>`.

**Example of `hive` partition strategy**

```sql
INSERT INTO FUNCTION s3(s3_conn, filename='t_03363_function', format=Parquet, partition_strategy='hive') PARTITION BY (year, country) SELECT 2020 as year, 'Russia' as country, 1 as id;
```

```result
SELECT _path, * FROM s3(s3_conn, filename='t_03363_function/**.parquet');

   ┌─_path──────────────────────────────────────────────────────────────────────┬─id─┬─country─┬─year─┐
1. │ test/t_03363_function/year=2020/country=Russia/7351295896279887872.parquet │  1 │ Russia  │ 2020 │
   └────────────────────────────────────────────────────────────────────────────┴────┴─────────┴──────┘
```

**Examples of `wildcard` partition strategy**

1. Using partition ID in a key creates separate files:

```sql
INSERT INTO TABLE FUNCTION
    s3('http://bucket.amazonaws.com/my_bucket/file_{_partition_id}.csv', 'CSV', 'a String, b UInt32, c UInt32', partition_strategy='wildcard')
    PARTITION BY a VALUES ('x', 2, 3), ('x', 4, 5), ('y', 11, 12), ('y', 13, 14), ('z', 21, 22), ('z', 23, 24);
```
As a result, the data is written into three files: `file_x.csv`, `file_y.csv`, and `file_z.csv`.

2. Using partition ID in a bucket name creates files in different buckets:

```sql
INSERT INTO TABLE FUNCTION
    s3('http://bucket.amazonaws.com/my_bucket_{_partition_id}/file.csv', 'CSV', 'a UInt32, b UInt32, c UInt32', partition_strategy='wildcard')
    PARTITION BY a VALUES (1, 2, 3), (1, 4, 5), (10, 11, 12), (10, 13, 14), (20, 21, 22), (20, 23, 24);
```
As a result, the data is written into three files in different buckets: `my_bucket_1/file.csv`, `my_bucket_10/file.csv`, and `my_bucket_20/file.csv`.

## Accessing public buckets {#accessing-public-buckets}

ClickHouse tries to fetch credentials from many different types of sources.
Sometimes, it can produce problems when accessing some buckets that are public causing the client to return `403` error code.
This issue can be avoided by using `NOSIGN` keyword, forcing the client to ignore all the credentials, and not sign the requests.

```sql
SELECT *
FROM s3(
   'https://datasets-documentation.s3.eu-west-3.amazonaws.com/aapl_stock.csv',
   NOSIGN,
   'CSVWithNames'
)
LIMIT 5;
```

## Using S3 credentials (ClickHouse Cloud) {#using-s3-credentials-clickhouse-cloud}

For non-public buckets, users can pass an `aws_access_key_id` and `aws_secret_access_key` to the function. For example:

```sql
SELECT count() FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/mta/*.tsv', '<KEY>', '<SECRET>','TSVWithNames')
```

This is appropriate for one-off accesses or in cases where credentials can easily be rotated. However, this is not recommended as a long-term solution for repeated access or where credentials are sensitive. In this case, we recommend users rely on role-based access.

Role-based access for S3 in ClickHouse Cloud is documented [here](/products/cloud/guides/data-sources/accessing-s3-data-securely).

Once configured, a `roleARN` can be passed to the s3 function via an `extra_credentials` parameter. For example:

```sql
SELECT count() FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/mta/*.tsv','CSVWithNames',extra_credentials(role_arn = 'arn:aws:iam::111111111111:role/ClickHouseAccessRole-001'))
```

An optional `external_id` can also be supplied alongside `role_arn`. It is passed as the `ExternalId` parameter of the AWS STS `AssumeRole` call and lets the role's trust policy require a shared secret, which mitigates the [confused deputy problem](https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html). For example:

```sql
SELECT count() FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/mta/*.tsv','CSVWithNames',extra_credentials(role_arn = 'arn:aws:iam::111111111111:role/ClickHouseAccessRole-001', external_id = 'my-external-id'))
```

Further examples can be found [here](/products/cloud/guides/data-sources/accessing-s3-data-securely#access-your-s3-bucket-with-the-clickhouseaccess-role)

## Working with archives {#working-with-archives}

Suppose that we have several archive files with following URIs on S3:

- 'https://s3-us-west-1.amazonaws.com/umbrella-static/top-1m-2018-01-10.csv.zip'
- 'https://s3-us-west-1.amazonaws.com/umbrella-static/top-1m-2018-01-11.csv.zip'
- 'https://s3-us-west-1.amazonaws.com/umbrella-static/top-1m-2018-01-12.csv.zip'

Extracting data from these archives is possible using ::. Globs can be used both in the url part as well as in the part after :: (responsible for the name of a file inside the archive).

```sql
SELECT *
FROM s3(
   'https://s3-us-west-1.amazonaws.com/umbrella-static/top-1m-2018-01-1{0..2}.csv.zip :: *.csv'
);
```

<Note>
ClickHouse supports three archive formats:
ZIP
TAR
7Z
While ZIP and TAR archives can be accessed from any supported storage location, 7Z archives can only be read from the local filesystem where ClickHouse is installed.
</Note>

## Inserting Data {#inserting-data}

Note that rows can only be inserted into new files. There are no merge cycles or file split operations. Once a file is written, subsequent inserts will fail. See more details [here](/integrations/connectors/data-ingestion/AWS/integrating-s3-with-clickhouse#inserting-data).

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`. In case of archive, shows path in a format: `"{path_to_archive}::{path_to_file_inside_archive}"`
- `_file` — Name of the file. Type: `LowCardinality(String)`. In case of archive shows name of the file inside the archive.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`. In case of archive shows uncompressed file size of the file inside the archive.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.

## use_hive_partitioning setting {#hive-style-partitioning}

This is a hint for ClickHouse to parse hive style partitioned files upon reading time. It has no effect on writing. For symmetrical reads and writes, use the `partition_strategy` argument.

When setting `use_hive_partitioning` is set to 1, ClickHouse will detect Hive-style partitioning in the path (`/name=value/`) and will allow to use partition columns as virtual columns in the query. These virtual columns will have the same names as in the partitioned path.

**Example**

```sql
SELECT * FROM s3('s3://data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

## Accessing requester-pays buckets {#accessing-requester-pays-buckets}

To access a requester-pays bucket, a header `x-amz-request-payer = requester` must be passed in any requests. This is achieved by passing the parameter `headers('x-amz-request-payer' = 'requester')` to the s3 function. For example:

```sql
SELECT
    count() AS num_rows,
    uniqExact(_file) AS num_files
FROM s3('https://coiled-datasets-rp.s3.us-east-1.amazonaws.com/1trc/measurements-100*.parquet', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', headers('x-amz-request-payer' = 'requester'))

┌───num_rows─┬─num_files─┐
│ 1110000000 │       111 │
└────────────┴───────────┘

1 row in set. Elapsed: 3.089 sec. Processed 1.09 billion rows, 0.00 B (353.55 million rows/s., 0.00 B/s.)
Peak memory usage: 192.27 KiB.
```

## Storage Settings {#storage-settings}

- [s3_truncate_on_insert](/reference/settings/session-settings#s3_truncate_on_insert) - allows to truncate file before insert into it. Disabled by default.
- [s3_create_new_file_on_insert](/reference/settings/session-settings#s3_create_new_file_on_insert) - allows to create a new file on each insert if format has suffix. Disabled by default.
- [s3_skip_empty_files](/reference/settings/session-settings#s3_skip_empty_files) - allows to skip empty files while reading. Enabled by default.

## Nested Avro Schemas {#nested-avro-schemas}

When reading Avro files that contain **nested records** which diverge across files (for example, some files have an extra field inside a nested object), ClickHouse may return an error such as:

> The number of leaves in record doesn't match the number of elements in tuple...

This happens because ClickHouse expects all nested record structures to match the same schema.  
To handle this scenario, you can:

- Use `schema_inference_mode='union'` to merge different nested record schemas, or  
- Manually align your nested structures and enable  
  `use_structure_from_insertion_table_in_table_functions=1`.

<Info>
**Performance note**

`schema_inference_mode='union'` may take longer on very large S3 datasets because it must scan each file to infer the schema.
</Info>

**Example**
```sql
INSERT INTO data_stage
SELECT
    id,
    data
FROM s3('https://bucket-name/*.avro', 'Avro')
SETTINGS schema_inference_mode='union';

## Related {#related}

- [S3 engine](/reference/engines/table-engines/integrations/s3)
- [Integrating S3 with ClickHouse](/integrations/connectors/data-ingestion/AWS/integrating-s3-with-clickhouse)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction},
        {.allow_readonly = false}
    );

    factory.registerFunction<TableFunctionObjectStorage<GCSDefinition, StorageS3Configuration>>(
        {.description = R"DOCS_MD(
Provides a table-like interface to `SELECT` and `INSERT` data from [Google Cloud Storage](https://cloud.google.com/storage/). Requires the [`Storage Object User` IAM role](https://cloud.google.com/storage/docs/access-control/iam-roles).

This is an alias of the [s3 table function](/reference/functions/table-functions/s3).

If you have multiple replicas in your cluster, you can use the [s3Cluster function](/reference/functions/table-functions/s3Cluster) (which works with GCS) instead to parallelize inserts.

## Syntax {#syntax}

```sql
gcs(url [, NOSIGN | hmac_key, hmac_secret] [,format] [,structure] [,compression_method] [,partition_strategy])
gcs(named_collection[, option=value [,..]])
```

<Tip>
**GCS**

The GCS Table Function integrates with Google Cloud Storage by using the GCS XML API and HMAC keys. 
See the [Google interoperability docs](https://cloud.google.com/storage/docs/interoperability) for more details about the endpoint and HMAC.
</Tip>

## Arguments {#arguments}

| Argument                     | Description                                                                                                                                                                              |
|------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `url`                        | Bucket path to file. Supports following wildcards in readonly mode: `*`, `**`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc'`, `'def'` — strings.                       |
| `NOSIGN`                     | If this keyword is provided in place of credentials, all the requests will not be signed.                                                                                                |
| `hmac_key` and `hmac_secret` | Keys that specify credentials to use with given endpoint. Optional.                                                                                                                      |
| `format`                     | The [format](/reference/formats/index) of the file.                                                                                                                                        |
| `structure`                  | Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                            |
| `compression_method`         | Parameter is optional. Supported values: `none`, `gzip` or `gz`, `brotli` or `br`, `xz` or `LZMA`, `zstd` or `zst`. By default, it will autodetect compression method by file extension. |
| `partition_strategy`         | Optional. Supported values: `wildcard` or `hive`. `wildcard` requires `{_partition_id}` in the path. It is the default only when `compatibility` is older than `26.6`; otherwise, including when current defaults apply, the default is `hive`. |

<Info>
**GCS**

The GCS path is in this format as the endpoint for the Google XML API is different than the JSON API:

```text
  https://storage.googleapis.com/<bucket>/<folder>/<filename(s)>
```

and not ~~https://storage.cloud.google.com~~.
</Info>

Arguments can also be passed using [named collections](/concepts/features/configuration/server-config/named-collections). In this case `url`, `format`, `structure`, `compression_method`, `partition_strategy` work in the same way, and some extra parameters are supported:

| Parameter                     | Description                                                                                                                                                                                                                       |
|-------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `access_key_id`               | `hmac_key`, optional.                                                                                                                                                                                                             |
| `secret_access_key`           | `hmac_secret`, optional.                                                                                                                                                                                                          |
| `filename`                    | Appended to the url if specified.                                                                                                                                                                                                 |
| `use_environment_credentials` | Enabled by default, allows passing extra parameters using environment variables `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `AWS_CONTAINER_CREDENTIALS_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN`, `AWS_EC2_METADATA_DISABLED`. |
| `no_sign_request`             | Disabled by default.                                                                                                                                                                                                              |
| `expiration_window_seconds`   | Default value is 120.                                                                                                                                                                                                             |

## Returned value {#returned_value}

A table with the specified structure for reading or writing data in the specified file.

## Examples {#examples}

Selecting the first two rows from the GCS file `https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/data.csv.gz`. The compression method is detected automatically from the `.gz` file extension:

```sql
SELECT *
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/data.csv.gz', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2;
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

The same query as above, but with the `gzip` compression method specified explicitly instead of relying on autodetection:

```sql
SELECT *
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/data.csv.gz', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32', 'gzip')
LIMIT 2;
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

## Usage {#usage}

Suppose that we have several files with following URIs on GCS:

-   'https://storage.googleapis.com/my-test-bucket-768/some_prefix/some_file_1.csv'
-   'https://storage.googleapis.com/my-test-bucket-768/some_prefix/some_file_2.csv'
-   'https://storage.googleapis.com/my-test-bucket-768/some_prefix/some_file_3.csv'
-   'https://storage.googleapis.com/my-test-bucket-768/some_prefix/some_file_4.csv'
-   'https://storage.googleapis.com/my-test-bucket-768/another_prefix/some_file_1.csv'
-   'https://storage.googleapis.com/my-test-bucket-768/another_prefix/some_file_2.csv'
-   'https://storage.googleapis.com/my-test-bucket-768/another_prefix/some_file_3.csv'
-   'https://storage.googleapis.com/my-test-bucket-768/another_prefix/some_file_4.csv'

Count the amount of rows in files ending with numbers from 1 to 3:

```sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/{some,another}_prefix/some_file_{1..3}.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
```

```text
┌─count()─┐
│      18 │
└─────────┘
```

Count the total amount of rows in all files in these two directories:

```sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/{some,another}_prefix/*', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
```

```text
┌─count()─┐
│      24 │
└─────────┘
```

<Warning>
If your listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.
</Warning>

Count the total amount of rows in files named `file-000.csv`, `file-001.csv`, ... , `file-999.csv`:

```sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/big_prefix/file-{000..999}.csv', 'CSV', 'name String, value UInt32');
```

```text
┌─count()─┐
│      12 │
└─────────┘
```

Insert data into file `test-data.csv.gz`:

```sql
INSERT INTO FUNCTION gcs('https://storage.googleapis.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
VALUES ('test-data', 1), ('test-data-2', 2);
```

Insert data into file `test-data.csv.gz` from existing table:

```sql
INSERT INTO FUNCTION gcs('https://storage.googleapis.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
SELECT name, value FROM existing_table;
```

Glob ** can be used for recursive directory traversal. Consider the below example, it will fetch all files from `my-test-bucket-768` directory recursively:

```sql
SELECT * FROM gcs('https://storage.googleapis.com/my-test-bucket-768/**', 'CSV', 'name String, value UInt32', 'gzip');
```

The below get data from all `test-data.csv.gz` files from any folder inside `my-test-bucket` directory recursively:

```sql
SELECT * FROM gcs('https://storage.googleapis.com/my-test-bucket-768/**/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip');
```

For production use cases it is recommended to use [named collections](/concepts/features/configuration/server-config/named-collections). Here is the example:
```sql

CREATE NAMED COLLECTION creds AS
        access_key_id = '***',
        secret_access_key = '***';
SELECT count(*)
FROM gcs(creds, url='https://s3-object-url.csv')
```

## Partitioned Write {#partitioned-write}

If you specify `PARTITION BY` expression when inserting data into `GCS` table, a separate file is created for each partition value. Splitting the data into separate files helps to improve reading operations efficiency.

When `compatibility` is set to `26.6` or later, or when current defaults apply, `hive` is the default partition strategy. Because paths containing `{_partition_id}` require `wildcard`, the following examples set `partition_strategy='wildcard'` explicitly.

**Examples**

1. Using partition ID in a key creates separate files:

```sql
INSERT INTO TABLE FUNCTION
    gcs('http://bucket.amazonaws.com/my_bucket/file_{_partition_id}.csv', 'CSV', 'a String, b UInt32, c UInt32', partition_strategy='wildcard')
    PARTITION BY a VALUES ('x', 2, 3), ('x', 4, 5), ('y', 11, 12), ('y', 13, 14), ('z', 21, 22), ('z', 23, 24);
```
As a result, the data is written into three files: `file_x.csv`, `file_y.csv`, and `file_z.csv`.

2. Using partition ID in a bucket name creates files in different buckets:

```sql
INSERT INTO TABLE FUNCTION
    gcs('http://bucket.amazonaws.com/my_bucket_{_partition_id}/file.csv', 'CSV', 'a UInt32, b UInt32, c UInt32', partition_strategy='wildcard')
    PARTITION BY a VALUES (1, 2, 3), (1, 4, 5), (10, 11, 12), (10, 13, 14), (20, 21, 22), (20, 23, 24);
```
As a result, the data is written into three files in different buckets: `my_bucket_1/file.csv`, `my_bucket_10/file.csv`, and `my_bucket_20/file.csv`.

## Related {#related}
- [S3 table function](/reference/functions/table-functions/s3)
- [S3 engine](/reference/engines/table-engines/integrations/s3)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction},
        {.allow_readonly = false}
    );

    factory.registerFunction<TableFunctionObjectStorage<COSNDefinition, StorageS3Configuration>>(
        {
            .description=R"(The table function can be used to read the data stored on COSN.)",
            .examples{{COSNDefinition::name, "SELECT * FROM cosn(url, access_key_id, secret_access_key)", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        {.allow_readonly = false}
    );

    factory.registerFunction<TableFunctionObjectStorage<OSSDefinition, StorageS3Configuration>>(
        {
            .description=R"(The table function can be used to read the data stored on OSS.)",
            .examples{{OSSDefinition::name, "SELECT * FROM oss(url, access_key_id, secret_access_key)", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        {.allow_readonly = false}
    );
#endif

#if USE_AZURE_BLOB_STORAGE
    factory.registerFunction<TableFunctionObjectStorage<AzureDefinition, StorageAzureConfiguration>>(
        {.description = R"DOCS_MD(
import ExperimentalBadge from "/snippets/components/ExperimentalBadge/ExperimentalBadge.jsx";
import CloudNotSupportedBadge from "/snippets/components/CloudNotSupportedBadge/CloudNotSupportedBadge.jsx";

Provides a table-like interface to select/insert files in [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs). This table function is similar to the [s3 function](/reference/functions/table-functions/s3).

## Syntax {#syntax}

<Tabs>
<Tab title="Connection string">

Credentials are embedded in the connection string, so no separate `account_name`/`account_key` is needed:

```sql
azureBlobStorage(connection_string, container_name, blobpath [, format, compression, partition_strategy, structure])
```

</Tab>
<Tab title="Storage account URL">

Requires `account_name` and `account_key` as separate arguments:

```sql
azureBlobStorage(storage_account_url, container_name, blobpath, account_name, account_key [, format, compression, partition_strategy, structure])
```

</Tab>
<Tab title="Named collection">

See [Named Collections](#named-collections) below for the full list of supported keys:

```sql
azureBlobStorage(named_collection[, option=value [,..]])
```

</Tab>
</Tabs>

## Arguments {#arguments}

| Argument                         | Description                                                                                                                                                                                                                                                                                                                                               |
|----------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `connection_string`              | A connection string that includes embedded credentials (account name + account key or SAS token). When using this form, `account_name` and `account_key` should **not** be passed separately. See [Configure a connection string](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account). |
| `storage_account_url`            | The storage account endpoint URL, e.g. `https://myaccount.blob.core.windows.net/`. When using this form, you **must** also pass `account_name` and `account_key`.                                                                                                                                                                                         |
| `container_name`                 | Container name.                                                                                                                                                                                                                                                                                                                                           |
| `blobpath`                       | File path. Supports the following wildcards in read-only mode: `*`, `**`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc'`, `'def'` — strings.                                                                                                                                                                                            |
| `account_name`                   | Storage account name. **Required** when using `storage_account_url` without SAS; must **not** be passed when using `connection_string`.                                                                                                                                                                                                                               |
| `account_key`                    | Storage account key. **Required** when using `storage_account_url` without SAS; must **not** be passed when using `connection_string`.                                                                                                                                                                                                                                |
| `format`                         | The [format](/reference/formats/index) of the file.                                                                                                                                                                                                                                                                                                         |
| `compression`                    | Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. By default, it will autodetect compression by file extension (same as setting to `auto`).                                                                                                                                                                                       |
| `structure`                      | Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                             |
| `partition_strategy`             | Optional. Supported values: `WILDCARD` or `HIVE`. `WILDCARD` requires a `{_partition_id}` in the path, which is replaced with the partition key. `HIVE` does not allow wildcards, assumes the path is the table root, and generates Hive-style partitioned directories with Snowflake IDs as filenames and the file format as the extension. Defaults to the `file_like_engine_default_partition_strategy` setting (`WILDCARD` when `compatibility` is older than `26.6`; `HIVE` otherwise, including when current defaults apply). |
| `partition_columns_in_data_file` | Optional. Only used with `HIVE` partition strategy. Tells ClickHouse whether to expect partition columns to be written in the data file. Defaults `false`.                                                                                                                                                                                                 |
| `extra_credentials`              | Use `client_id` and `tenant_id` for authentication. If extra_credentials are provided, they are given priority over `account_name` and `account_key`.                                                                                                                                                                                                     |

## Named Collections {#named-collections}

Arguments can also be passed using [named collections](/concepts/features/configuration/server-config/named-collections). In this case the following keys are supported:

| Key                              | Required | Description                                                                                            |
|----------------------------------|----------|--------------------------------------------------------------------------------------------------------|
| `container`                      | Yes      | Container name. Corresponds to the positional argument `container_name`.                               |
| `blob_path`                      | Yes      | File path (with optional wildcards). Corresponds to the positional argument `blobpath`.                |
| `connection_string`              | No*      | Connection string with embedded credentials. *Either `connection_string` or `storage_account_url` must be provided. |
| `storage_account_url`            | No*      | Storage account endpoint URL. *Either `connection_string` or `storage_account_url` must be provided.   |
| `account_name`                   | No       | Required when using `storage_account_url`                                                            |
| `account_key`                    | No       | Required when using `storage_account_url`                                                            |
| `format`                         | No       | File format.                                                                                           |
| `compression`                    | No       | Compression type.                                                                                      |
| `structure`                      | No       | Table structure.                                                                                       |
| `client_id`                      | No       | Client ID for authentication.                                                                          |
| `tenant_id`                      | No       | Tenant ID for authentication.                                                                          |

<Note>
Named collection key names differ from positional function argument names: `container` (not `container_name`) and `blob_path` (not `blobpath`).
</Note>

**Example:**

```sql
CREATE NAMED COLLECTION azure_my_data AS
    storage_account_url = 'https://myaccount.blob.core.windows.net/',
    container = 'mycontainer',
    blob_path = 'data/*.parquet',
    account_name = 'myaccount',
    account_key = 'mykey...==',
    format = 'Parquet';

SELECT *
FROM azureBlobStorage(azure_my_data)
LIMIT 5;
```

You can also override named collection values at query time:

```sql
SELECT *
FROM azureBlobStorage(azure_my_data, blob_path = 'other_data/*.csv', format = 'CSVWithNames')
LIMIT 5;
```

## Returned value {#returned_value}

A table with the specified structure for reading or writing data in the specified file.

## Examples {#examples}

### Reading with `storage_account_url` form {#reading-with-storage-account-url}

```sql
SELECT *
FROM azureBlobStorage(
    'https://myaccount.blob.core.windows.net/',
    'mycontainer',
    'data/*.parquet',
    'myaccount',
    'mykey...==',
    'Parquet'
)
LIMIT 5;
```

### Reading with `connection_string` form {#reading-with-connection-string}

```sql
SELECT *
FROM azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'data/*.csv',
    'CSVWithNames'
)
LIMIT 5;
```

### Writing with partitions {#writing-with-partitions}

When `compatibility` is set to `26.6` or later, or when current defaults apply, `HIVE` is the default partition strategy. Paths containing `{_partition_id}` therefore require an explicit `WILDCARD` strategy.

```sql
INSERT INTO TABLE FUNCTION azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'test_{_partition_id}.csv',
    'CSV',
    'auto',
    'wildcard',
    'column1 UInt32, column2 UInt32, column3 UInt32'
) PARTITION BY column3
VALUES (1, 2, 3), (3, 2, 1), (78, 43, 3);
```

Then read back a specific partition:

```sql
SELECT *
FROM azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'test_1.csv',
    'CSV',
    'auto',
    'column1 UInt32, column2 UInt32, column3 UInt32'
);
```

```response
┌─column1─┬─column2─┬─column3─┐
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.

## Partitioned Write {#partitioned-write}

### Partition Strategy {#partition-strategy}

Supported for INSERT queries only.

`WILDCARD`: Replaces the `{_partition_id}` wildcard in the file path with the actual partition key. It is selected by default only when the `compatibility` setting is older than `26.6`. When `compatibility` is set to `26.6` or later, or when current defaults apply, the default is `HIVE`, so paths containing `{_partition_id}` must explicitly select `WILDCARD` (see the `file_like_engine_default_partition_strategy` setting).

`HIVE` implements hive style partitioning for reads & writes. It generates files using the following format: `<prefix>/<key1=val1/key2=val2...>/<snowflakeid>.<toLower(file_format)>`.

**Example of `HIVE` partition strategy**

```sql
INSERT INTO TABLE FUNCTION azureBlobStorage(
    azure_conf2,
    storage_account_url = 'https://myaccount.blob.core.windows.net/',
    container = 'cont',
    blob_path = 'azure_table_root',
    format = 'CSVWithNames',
    compression = 'auto',
    structure = 'year UInt16, country String, id Int32',
    partition_strategy = 'hive'
) PARTITION BY (year, country)
VALUES (2020, 'Russia', 1), (2021, 'Brazil', 2);
```

```result
SELECT _path, * FROM azureBlobStorage(
    azure_conf2,
    storage_account_url = 'https://myaccount.blob.core.windows.net/',
    container = 'cont',
    blob_path = 'azure_table_root/**.csvwithnames'
)

   ┌─_path───────────────────────────────────────────────────────────────────────────┬─id─┬─year─┬─country─┐
1. │ cont/azure_table_root/year=2021/country=Brazil/7351307847391293440.csvwithnames │  2 │ 2021 │ Brazil  │
2. │ cont/azure_table_root/year=2020/country=Russia/7351307847378710528.csvwithnames │  1 │ 2020 │ Russia  │
   └─────────────────────────────────────────────────────────────────────────────────┴────┴──────┴─────────┘
```

## use_hive_partitioning setting {#hive-style-partitioning}

This is a hint for ClickHouse to parse hive style partitioned files upon reading time. It has no effect on writing. For symmetrical reads and writes, use the `partition_strategy` argument.

When setting `use_hive_partitioning` is set to 1, ClickHouse will detect Hive-style partitioning in the path (`/name=value/`) and will allow to use partition columns as virtual columns in the query. These virtual columns will have the same names as in the partitioned path.

**Example**

Use virtual column, created with Hive-style partitioning

```sql
SELECT * FROM azureBlobStorage(config, storage_account_url='...', container='...', blob_path='http://data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

## Using Shared Access Signatures (SAS) {#using-shared-access-signatures-sas-sas-tokens}

A Shared Access Signature (SAS) is a URI that grants restricted access to an Azure Storage container or file. Use it to provide time-limited access to storage account resources without sharing your storage account key. More details [here](https://learn.microsoft.com/en-us/rest/api/storageservices/delegate-access-with-shared-access-signature).

The `azureBlobStorage` function supports Shared Access Signatures (SAS).

A [Blob SAS token](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers) contains all the information needed to authenticate the request, including the target blob, permissions, and validity period. To construct a blob URL, append the SAS token to the blob service endpoint. For example, if the endpoint is `https://clickhousedocstest.blob.core.windows.net/`, the request becomes:

```sql
SELECT count()
FROM azureBlobStorage('BlobEndpoint=https://clickhousedocstest.blob.core.windows.net/;SharedAccessSignature=sp=r&st=2025-01-29T14:58:11Z&se=2025-01-29T22:58:11Z&spr=https&sv=2022-11-02&sr=c&sig=Ac2U0xl4tm%2Fp7m55IilWl1yHwk%2FJG0Uk6rMVuOiD0eE%3D', 'exampledatasets', 'example.csv')

┌─count()─┐
│      10 │
└─────────┘

1 row in set. Elapsed: 0.425 sec.
```

Alternatively, users can use the generated [Blob SAS URL](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers):

```sql
SELECT count()
FROM azureBlobStorage('https://clickhousedocstest.blob.core.windows.net/?sp=r&st=2025-01-29T14:58:11Z&se=2025-01-29T22:58:11Z&spr=https&sv=2022-11-02&sr=c&sig=Ac2U0xl4tm%2Fp7m55IilWl1yHwk%2FJG0Uk6rMVuOiD0eE%3D', 'exampledatasets', 'example.csv')

┌─count()─┐
│      10 │
└─────────┘

1 row in set. Elapsed: 0.153 sec.
```

## Related {#related}
- [AzureBlobStorage Table Engine](/reference/engines/table-engines/integrations/azureBlobStorage)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction},
        {.allow_readonly = false}
    );
#endif
#if USE_HDFS
    factory.registerFunction<TableFunctionObjectStorage<HDFSDefinition, StorageHDFSConfiguration>>(
        {.description = R"DOCS_MD(
import ExperimentalBadge from "/snippets/components/ExperimentalBadge/ExperimentalBadge.jsx";
import CloudNotSupportedBadge from "/snippets/components/CloudNotSupportedBadge/CloudNotSupportedBadge.jsx";

Creates a table from files in HDFS. This table function is similar to the [url](/reference/functions/table-functions/url) and [file](/reference/functions/table-functions/file) table functions.

## Syntax {#syntax}

```sql
hdfs(URI, format, structure)
```

## Arguments {#arguments}

| Argument  | Description                                                                                                                                                              |
|-----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `URI`     | The relative URI to the file in HDFS. Path to file support following globs in readonly mode: `*`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc', 'def'` — strings. |
| `format`  | The [format](/reference/formats/index) of the file.                                                                                                                          |
| `structure`| Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.                                                                           |

## Returned value {#returned_value}

A table with the specified structure for reading or writing data in the specified file.

**example**

Table from `hdfs://hdfs1:9000/test` and selection of the first two rows from it:

```sql
SELECT *
FROM hdfs('hdfs://hdfs1:9000/test', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

## Globs in path {#globs_in_path}

Paths may use globbing. Files must match the whole path pattern, not only the suffix or prefix.

- `*` — Represents arbitrarily many characters except `/` but including the empty string.
- `**` — Represents all files inside a folder recursively.
- `?` — Represents an arbitrary single character.
- `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`. The strings can contain the `/` symbol.
- `{N..M}` — Represents any number `>= N` and `<= M`.

Constructions with `{}` are similar to the [remote](/reference/functions/table-functions/remote) and [file](/reference/functions/table-functions/file) table functions.

**Example**

1.  Suppose that we have several files with following URIs on HDFS:

- 'hdfs://hdfs1:9000/some_dir/some_file_1'
- 'hdfs://hdfs1:9000/some_dir/some_file_2'
- 'hdfs://hdfs1:9000/some_dir/some_file_3'
- 'hdfs://hdfs1:9000/another_dir/some_file_1'
- 'hdfs://hdfs1:9000/another_dir/some_file_2'
- 'hdfs://hdfs1:9000/another_dir/some_file_3'

2.  Query the amount of rows in these files:

{/* */}

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3.  Query the amount of rows in all files of these two directories:

{/* */}

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

<Note>
If your listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.
</Note>

**Example**

Query the data from files named `file000`, `file001`, ... , `file999`:

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32')
```

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.

## use_hive_partitioning setting {#hive-style-partitioning}

When setting `use_hive_partitioning` is set to 1, ClickHouse will detect Hive-style partitioning in the path (`/name=value/`) and will allow to use partition columns as virtual columns in the query. These virtual columns will have the same names as in the partitioned path.

**Example**

Use virtual column, created with Hive-style partitioning

```sql
SELECT * FROM HDFS('hdfs://hdfs1:9000/data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

## Storage Settings {#storage-settings}

- [hdfs_truncate_on_insert](/reference/settings/session-settings#hdfs_truncate_on_insert) - allows to truncate file before insert into it. Disabled by default.
- [hdfs_create_new_file_on_insert](/reference/settings/session-settings#hdfs_create_new_file_on_insert) - allows to create a new file on each insert if format has suffix. Disabled by default.
- [hdfs_skip_empty_files](/reference/settings/session-settings#hdfs_skip_empty_files) - allows to skip empty files while reading. Disabled by default.

## Related {#related}

- [Virtual columns](/reference/engines/table-engines/index#table_engines-virtual_columns)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction},
        {.allow_readonly = false}
    );
#endif
}

#if USE_AZURE_BLOB_STORAGE
template class TableFunctionObjectStorage<AzureDefinition, StorageAzureConfiguration>;
template class TableFunctionObjectStorage<AzureClusterDefinition, StorageAzureConfiguration>;
#endif

#if USE_AWS_S3
template class TableFunctionObjectStorage<S3Definition, StorageS3Configuration>;
template class TableFunctionObjectStorage<S3ClusterDefinition, StorageS3Configuration>;
template class TableFunctionObjectStorage<GCSDefinition, StorageS3Configuration>;
template class TableFunctionObjectStorage<COSNDefinition, StorageS3Configuration>;
template class TableFunctionObjectStorage<OSSDefinition, StorageS3Configuration>;
#endif

#if USE_HDFS
template class TableFunctionObjectStorage<HDFSDefinition, StorageHDFSConfiguration>;
template class TableFunctionObjectStorage<HDFSClusterDefinition, StorageHDFSConfiguration>;
#endif

#if USE_AVRO
template class TableFunctionObjectStorage<IcebergLocalClusterDefinition, StorageLocalIcebergConfiguration, true>;
#endif

#if USE_AVRO && USE_AWS_S3
template class TableFunctionObjectStorage<IcebergS3ClusterDefinition, StorageS3IcebergConfiguration, true>;
template class TableFunctionObjectStorage<IcebergClusterDefinition, StorageS3IcebergConfiguration, true>;
#endif

#if USE_AVRO && USE_AZURE_BLOB_STORAGE
template class TableFunctionObjectStorage<IcebergAzureClusterDefinition, StorageAzureIcebergConfiguration, true>;
#endif

#if USE_AVRO && USE_HDFS
template class TableFunctionObjectStorage<IcebergHDFSClusterDefinition, StorageHDFSIcebergConfiguration, true>;
#endif

#if USE_AVRO && USE_AWS_S3
template class TableFunctionObjectStorage<PaimonS3ClusterDefinition, StorageS3PaimonConfiguration, true>;
template class TableFunctionObjectStorage<PaimonClusterDefinition, StorageS3PaimonConfiguration, true>;
#endif

#if USE_AVRO && USE_AZURE_BLOB_STORAGE
template class TableFunctionObjectStorage<PaimonAzureClusterDefinition, StorageAzurePaimonConfiguration, true>;
#endif

#if USE_AVRO && USE_HDFS
template class TableFunctionObjectStorage<PaimonHDFSClusterDefinition, StorageHDFSPaimonConfiguration, true>;
#endif

#if USE_PARQUET && USE_AWS_S3 && USE_DELTA_KERNEL_RS
template class TableFunctionObjectStorage<DeltaLakeClusterDefinition, StorageS3DeltaLakeConfiguration, true>;
template class TableFunctionObjectStorage<DeltaLakeS3ClusterDefinition, StorageS3DeltaLakeConfiguration, true>;
#endif

#if USE_PARQUET && USE_AZURE_BLOB_STORAGE && USE_DELTA_KERNEL_RS
template class TableFunctionObjectStorage<DeltaLakeAzureClusterDefinition, StorageAzureDeltaLakeConfiguration, true>;
#endif

#if USE_AWS_S3
template class TableFunctionObjectStorage<HudiClusterDefinition, StorageS3HudiConfiguration, true>;
#endif

#if USE_AVRO
void registerTableFunctionIceberg(TableFunctionFactory & factory);
void registerTableFunctionIceberg(TableFunctionFactory & factory)
{
#if USE_AWS_S3
    factory.registerFunction<TableFunctionIceberg>(
         {.description = R"DOCS_MD(
Provides a read-only table-like interface to Apache [Iceberg](https://iceberg.apache.org/) tables in Amazon S3, Azure, HDFS or locally stored.

## Syntax {#syntax}

```sql
icebergS3(url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method] [,extra_credentials])
icebergS3(named_collection[, option=value [,..]])

icebergAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzure(named_collection[, option=value [,..]])

icebergHDFS(path_to_table, [,format] [,compression_method])
icebergHDFS(named_collection[, option=value [,..]])

icebergLocal(path_to_table, [,format] [,compression_method])
icebergLocal(named_collection[, option=value [,..]])
```

## Arguments {#arguments}

Description of the arguments coincides with description of arguments in table functions `s3`, `azureBlobStorage`, `HDFS` and `file` correspondingly.
`format` stands for the format of data files in the Iceberg table.

For `icebergS3`, an optional `extra_credentials` parameter can be used to pass a `role_arn` for role-based access in ClickHouse Cloud. See [Secure S3](/products/cloud/guides/data-sources/accessing-s3-data-securely) for configuration steps.

### Returned value {#returned-value}

A table with the specified structure for reading data in the specified Iceberg table.

### Example {#example}

```sql
SELECT * FROM icebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

<Warning>
ClickHouse currently supports reading v1 and v2 of the Iceberg format via the `icebergS3`, `icebergAzure`, `icebergHDFS` and `icebergLocal` table functions and `IcebergS3`, `icebergAzure`, `IcebergHDFS` and `IcebergLocal` table engines.
</Warning>

## Defining a named collection {#defining-a-named-collection}

Here is an example of configuring a named collection for storing the URL and credentials:

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
            <format>auto</format>
            <structure>auto</structure>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM icebergS3(iceberg_conf, filename = 'test_table')
DESCRIBE icebergS3(iceberg_conf, filename = 'test_table')
```

## Using a data catalog {#iceberg-writes-catalogs}

Iceberg tables can also be used with various data catalogs, such as the [REST Catalog](https://iceberg.apache.org/rest-catalog-spec/), [AWS Glue Data Catalog](https://docs.aws.amazon.com/prescriptive-guidance/latest/serverless-etl-aws-glue/aws-glue-data-catalog.html) and [Unity Catalog](https://www.unitycatalog.io/).

<Warning>
When using a catalog, most users will want to use the `DataLakeCatalog` database engine, which connects ClickHouse to your catalog to discover your tables. You can use this database engine instead of manually creating individual tables with `IcebergS3` table engine.
</Warning>

To use them, create a table with the `IcebergS3` engine and provide the necessary settings.

For example, using REST Catalog with MinIO storage:
```sql
CREATE TABLE `database_name.table_name`
ENGINE = IcebergS3(
  'http://minio:9000/warehouse-rest/table_name/',
  'minio_access_key',
  'minio_secret_key'
)
```

Or, using AWS Glue Data Catalog with S3:
```sql
CREATE TABLE `my_database.my_table`  
ENGINE = IcebergS3(
  's3://my-data-bucket/warehouse/my_database/my_table/',
  'aws_access_key',
  'aws_secret_key'
)
```

## Schema Evolution {#schema-evolution}

At the moment, with the help of CH, you can read iceberg tables, the schema of which has changed over time. We currently support reading tables where columns have been added and removed, and their order has changed. You can also change a column where a value is required to one where NULL is allowed. Additionally, we support permitted type casting for simple types, namely:  

* int -> long
* float -> double
* decimal(P, S) -> decimal(P', S) where P' > P.

Currently, it is not possible to change nested structures or the types of elements within arrays and maps.

## Partition Pruning {#partition-pruning}

ClickHouse supports partition pruning during SELECT queries for Iceberg tables, which helps optimize query performance by skipping irrelevant data files. To enable partition pruning, set `use_iceberg_partition_pruning = 1`. For more information about iceberg partition pruning address https://iceberg.apache.org/spec/#partitioning

## Time Travel {#time-travel}

ClickHouse supports time travel for Iceberg tables, allowing you to query historical data with a specific timestamp or snapshot ID.

## Processing of tables with deleted rows {#deleted-rows}

Currently, only Iceberg tables with [position deletes](https://iceberg.apache.org/spec/#position-delete-files) are supported. 

The following deletion methods are **not supported**:
- [Equality deletes](https://iceberg.apache.org/spec/#equality-delete-files)
- [Deletion vectors](https://iceberg.apache.org/spec/#deletion-vectors) (introduced in v3)

### Basic usage {#basic-usage}

 ```sql
 SELECT * FROM example_table ORDER BY 1 
 SETTINGS iceberg_timestamp_ms = 1714636800000
 ```

 ```sql
 SELECT * FROM example_table ORDER BY 1 
 SETTINGS iceberg_snapshot_id = 3547395809148285433
 ```

Note: You cannot specify both `iceberg_timestamp_ms` and `iceberg_snapshot_id` parameters in the same query.

### Important considerations {#important-considerations}

* **Snapshots** are typically created when:
* New data is written to the table
* Some kind of data compaction is performed

* **Schema changes typically don't create snapshots** - This leads to important behaviors when using time travel with tables that have undergone schema evolution.

### Example scenarios {#example-scenarios}

All scenarios are written in Spark because CH doesn't support writing to Iceberg tables yet.

#### Scenario 1: Schema Changes Without New Snapshots {#scenario-1}

Consider this sequence of operations:

 ```sql
 -- Create a table with two columns
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

- - Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES 
    (1, 'Mars')

  ts1 = now() // A piece of pseudo code

- - Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)
 
  ts2 = now()

- - Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES (2, 'Venus', 100)

   ts3 = now()

- - Query the table at each timestamp
  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts1;

+------------+------------+
|order_number|product_code|
+------------+------------+
|           1|        Mars|
+------------+------------+
  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts2;

+------------+------------+
|order_number|product_code|
+------------+------------+
|           1|        Mars|
+------------+------------+

  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts3;

+------------+------------+-----+
|order_number|product_code|price|
+------------+------------+-----+
|           1|        Mars| NULL|
|           2|       Venus|100.0|
+------------+------------+-----+
```

Query results at different timestamps:

* At ts1 & ts2: Only the original two columns appear
* At ts3: All three columns appear, with NULL for the price of the first row

#### Scenario 2:  Historical vs. Current Schema Differences {#scenario-2}

A time travel query at a current moment might show a different schema than the current table:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_2 (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

-- Insert initial data into the table
  INSERT INTO spark_catalog.db.time_travel_example_2 VALUES (2, 'Venus');

-- Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example_2 ADD COLUMN (price double);

  ts = now();

-- Query the table at a current moment but using timestamp syntax

  SELECT * FROM spark_catalog.db.time_travel_example_2 TIMESTAMP AS OF ts;

    +------------+------------+
    |order_number|product_code|
    +------------+------------+
    |           2|       Venus|
    +------------+------------+

-- Query the table at a current moment
  SELECT * FROM spark_catalog.db.time_travel_example_2;
    +------------+------------+-----+
    |order_number|product_code|price|
    +------------+------------+-----+
    |           2|       Venus| NULL|
    +------------+------------+-----+
```

This happens because `ALTER TABLE` doesn't create a new snapshot but for the current table Spark takes value of `schema_id` from the latest metadata file, not a snapshot.

#### Scenario 3:  Historical vs. Current Schema Differences {#scenario-3}

The second one is that while doing time travel you can't get state of table before any data was written to it:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_3 (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2');

  ts = now();

-- Query the table at a specific timestamp
  SELECT * FROM spark_catalog.db.time_travel_example_3 TIMESTAMP AS OF ts; -- Finises with error: Cannot find a snapshot older than ts.
```

In Clickhouse the behavior is consistent with Spark. You can mentally replace Spark Select queries with Clickhouse Select queries and it will work the same way.

## Metadata File Resolution {#metadata-file-resolution}

When using the `iceberg` table function in ClickHouse, the system needs to locate the correct metadata.json file that describes the Iceberg table structure. Here's how this resolution process works:

### Candidate Search (in Priority Order) {#candidate-search}

1. **Direct Path Specification**:
*If you set `iceberg_metadata_file_path`, the system will use this exact path by combining it with the Iceberg table directory path.
* When this setting is provided, all other resolution settings are ignored.

2. **Table UUID Matching**:
*If `iceberg_metadata_table_uuid` is specified, the system will:
    *Look only at `.metadata.json` files in the `metadata` directory
    *Filter for files containing a `table-uuid` field matching your specified UUID (case-insensitive)

3. **Default Search**:
*If neither of the above settings are provided, all `.metadata.json` files in the `metadata` directory become candidates

### Selecting the Most Recent File {#most-recent-file}

After identifying candidate files using the above rules, the system determines which one is the most recent:

* If `iceberg_recent_metadata_file_by_last_updated_ms_field` is enabled:
* The file with the largest `last-updated-ms` value is selected

* Otherwise:
* The file with the highest version number is selected
* (Version appears as `V` in filenames formatted as `V.metadata.json` or `V-uuid.metadata.json`)

**Note**: All mentioned settings are table function settings (not global or query-level settings) and must be specified as shown below:

```sql
SELECT * FROM iceberg('s3://bucket/path/to/iceberg_table', 
    SETTINGS iceberg_metadata_table_uuid = 'a90eed4c-f74b-4e5b-b630-096fb9d09021');
```

**Note**: While Iceberg Catalogs typically handle metadata resolution, the `iceberg` table function in ClickHouse directly interprets files stored in S3 as Iceberg tables, which is why understanding these resolution rules is important.

## Metadata cache {#metadata-cache}

`Iceberg` table engine and table function support metadata cache storing the information of manifest files, manifest list and metadata json. The cache is stored in memory. This feature is controlled by setting `use_iceberg_metadata_files_cache`, which is enabled by default.

## Aliases {#aliases}

Table function `iceberg` is an alias to `icebergS3` now.

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_etag` — The etag of the file. Type: `LowCardinality(String)`. If the etag is unknown, the value is `NULL`.

## Writes into iceberg table {#writes-into-iceberg-table}

Starting from version 25.7, ClickHouse supports modifications of user’s Iceberg tables.

Currently, this is an experimental feature, so you first need to enable it:

```sql
SET allow_insert_into_iceberg = 1;
```

### Creating table {#create-iceberg-table}

To create your own empty Iceberg table, use the same commands as for reading, but specify the schema explicitly.
Writes supports all data formats from iceberg specification, such as Parquet, Avro, ORC.

### Example {#example-iceberg-writes-create}

```sql
CREATE TABLE iceberg_writes_example
(
    x Nullable(String),
    y Nullable(Int32)
)
ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/')
```

Note: To create a version hint file, enable the `iceberg_use_version_hint` setting.
If you want to compress the metadata.json file, specify the codec name in the `iceberg_metadata_compression_method` setting.

### INSERT {#writes-inserts}

After creating a new table, you can insert data using the usual ClickHouse syntax.

### Example {#example-iceberg-writes-insert}

```sql
INSERT INTO iceberg_writes_example VALUES ('Pavel', 777), ('Ivanov', 993);

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Pavel
y: 777

Row 2:
──────
x: Ivanov
y: 993
```

### DELETE {#iceberg-writes-delete}

Deleting extra rows in the merge-on-read format is also supported in ClickHouse.
This query will create a new snapshot with position delete files.

### Example {#example-iceberg-writes-delete}

```sql
ALTER TABLE iceberg_writes_example DELETE WHERE x != 'Ivanov';

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

### Schema evolution {#iceberg-writes-schema-evolution}

ClickHouse allows you to add, drop, modify, or rename columns with simple types (non-tuple, non-array, non-map).

### Example {#example-iceberg-writes-evolution}

```sql
ALTER TABLE iceberg_writes_example MODIFY COLUMN y Nullable(Int64);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

ALTER TABLE iceberg_writes_example ADD COLUMN z Nullable(Int32);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64),                                 ↴│
   │↳    `z` Nullable(Int32)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
z: ᴺᵁᴸᴸ

ALTER TABLE iceberg_writes_example DROP COLUMN z;
SHOW CREATE TABLE iceberg_writes_example;
   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993

ALTER TABLE iceberg_writes_example RENAME COLUMN y TO value;
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `value` Nullable(Int64)                              ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
value: 993
```

### Compaction {#iceberg-writes-compaction}

ClickHouse supports compaction iceberg table. Currently, it can merge position delete files into data files while updating metadata. Previous snapshot IDs and timestamps remain unchanged, so the time-travel feature can still be used with the same values.

How to use it:

```sql
SET allow_experimental_iceberg_compaction = 1

OPTIMIZE TABLE iceberg_writes_example;

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

### Expire Snapshots {#iceberg-expire-snapshots}

Iceberg tables accumulate snapshots with each INSERT, DELETE, or UPDATE operation. Over time, this can lead to a large number of snapshots and associated data files. The `expire_snapshots` command removes old snapshots and cleans up data files that are no longer referenced by any retained snapshot.

**Syntax:**

```sql
ALTER TABLE iceberg_table EXECUTE expire_snapshots(
    ['timestamp']
    [, expire_before = 'timestamp']
    [, retention_period = '3d']
    [, retain_last = 100]
    [, snapshot_ids = [1, 2, 3, 4]]
    [, dry_run = 1]
);
```

By default, which snapshots to keep is determined by the [retention policy](#iceberg-snapshot-retention-policy) (table properties `min-snapshots-to-keep`, `max-snapshot-age-ms`, and per-ref overrides). When `snapshot_ids` is specified, the retention policy is bypassed and only the listed snapshots are considered for expiration.

**Arguments:**

- `'timestamp'` (positional) or `expire_before = 'timestamp'` — a datetime string (e.g., `'2024-06-01 00:00:00'`) interpreted in the **server's timezone**. Acts as a safety fuse: snapshots whose `timestamp-ms` is at or after this value are protected from expiration, even if the retention policy would otherwise expire them. Can be combined with `snapshot_ids`, in which case listed snapshots at or newer than the timestamp are not expired.
- `retention_period = '<duration>'` — overrides the table-level `history.expire.max-snapshot-age-ms` for this invocation only. Snapshots older than this duration (measured from now) become candidates for expiration. The value is a duration string consisting of one or more `{number}{unit}` pairs concatenated together. Supported units: `y` (365 days), `w` (7 days), `d` (24 hours), `h` (60 minutes), `m` (60 seconds), `s` (1 second), `ms` (1 millisecond). Units can be combined, e.g. `'3d'`, `'12h'`, `'1d12h30m'`, `'500ms'`.
- `retain_last = N` — overrides the table-level `history.expire.min-snapshots-to-keep` for this invocation only. At least `N` snapshots are always retained regardless of age.
- `snapshot_ids = [id1, id2, ...]` — expires exactly the listed snapshot IDs (except snapshots referenced by current snapshot, branches, or tags). This mode bypasses the retention policy entirely and cannot be combined with `retention_period` or `retain_last`.
- `dry_run = 1` — computes what would be expired and returns metrics without writing new metadata or deleting files.

<Note>
`retention_period` and `retain_last` override only the **table-level** retention defaults. Per-ref (branch/tag) retention overrides configured in the Iceberg table properties (e.g., `refs.<branch>.min-snapshots-to-keep`) are never overridden — they always take effect as specified in the table metadata.
</Note>

**Example:**

```sql
SET allow_insert_into_iceberg = 1;

-- Create some snapshots by inserting data
INSERT INTO iceberg_table VALUES (1);
INSERT INTO iceberg_table VALUES (2);
INSERT INTO iceberg_table VALUES (3);

-- Expire using retention policy only
ALTER TABLE iceberg_table EXECUTE expire_snapshots();

-- Expire with a safety fuse: protect snapshots newer than the timestamp (positional syntax)
ALTER TABLE iceberg_table EXECUTE expire_snapshots('2025-01-01 00:00:00');

-- Same using the named argument form
ALTER TABLE iceberg_table EXECUTE expire_snapshots(expire_before = '2025-01-01 00:00:00');

-- Override retention parameters for one execution
ALTER TABLE iceberg_table EXECUTE expire_snapshots(retention_period = '3d', retain_last = 10);

-- Expire explicit snapshots
ALTER TABLE iceberg_table EXECUTE expire_snapshots(snapshot_ids = [101, 102, 103]);

-- Dry-run preview (no metadata updates, no file deletes)
ALTER TABLE iceberg_table EXECUTE expire_snapshots(retention_period = '1d', dry_run = 1);
```

**Output:**

The command returns a table with two columns (`metric_name String`, `metric_value Int64`) containing one row per metric. The metric names follow the [Iceberg spec](https://iceberg.apache.org/docs/latest/spark-procedures/#output):

| metric_name | Description |
|---|---|
| `deleted_data_files_count` | Number of data files deleted |
| `deleted_position_delete_files_count` | Number of position delete files deleted |
| `deleted_equality_delete_files_count` | Number of equality delete files deleted |
| `deleted_manifest_files_count` | Number of manifest files deleted |
| `deleted_manifest_lists_count` | Number of manifest list files deleted |
| `deleted_statistics_files_count` | Number of statistics files deleted (always 0 currently) |
| `dry_run` | `1` for dry-run mode, `0` for normal execution |

The command performs the following steps:

1. Evaluates the retention policy (see below) to determine which snapshots must be preserved
2. If a timestamp argument was provided, additionally protects all snapshots at or newer than that timestamp
3. Expires snapshots that are neither retained by the policy nor protected by the timestamp fuse
4. Computes which files are exclusively associated with expired snapshots
5. In normal mode: generates new metadata without the expired snapshots
6. In normal mode: physically deletes unreachable manifest lists, manifest files, and data files
7. In `dry_run = 1` mode: skips steps 5 and 6 and only returns the calculated metrics

#### Snapshot Retention Policy {#iceberg-snapshot-retention-policy}

The `expire_snapshots` command respects the [Iceberg snapshot retention policy](https://iceberg.apache.org/spec/#snapshot-retention-policy). Retention is configured via Iceberg table properties and per-reference overrides:

| Property | Scope | Default | Description |
|---|---|---|---|
| `history.expire.min-snapshots-to-keep` | Table | `iceberg_expire_default_min_snapshots_to_keep` (default `1`) | Minimum number of snapshots to keep in each branch's ancestor chain |
| `history.expire.max-snapshot-age-ms` | Table | `iceberg_expire_default_max_snapshot_age_ms` (default `432000000`, 5 days) | Maximum age (in ms) of snapshots to retain in a branch |
| `history.expire.max-ref-age-ms` | Table | `iceberg_expire_default_max_ref_age_ms` (default `∞`) | Maximum age (in ms) for a snapshot reference (branch or tag) before the reference itself is removed |

Each snapshot reference (`refs` in the Iceberg metadata) can override these with per-ref fields: `min-snapshots-to-keep`, `max-snapshot-age-ms`, and `max-ref-age-ms`.

**Retention evaluation:**

- **For each branch** (including `main`): the ancestor chain is walked starting from the branch head. Snapshots are retained while either of these conditions is true:
  - The snapshot is one of the first `min-snapshots-to-keep` in the chain
  - The snapshot's age is within `max-snapshot-age-ms` (i.e., `now - timestamp-ms <= max-snapshot-age-ms`)
- **For tags**: the tagged snapshot is retained unless the tag has exceeded its `max-ref-age-ms`, in which case the tag reference is removed
- **Non-main references** whose age exceeds `max-ref-age-ms` are removed entirely (the `main` branch is never removed)
- **Dangling references** that point to non-existent snapshots are removed with a warning
- **The current snapshot is always preserved**, regardless of retention settings

**Required privileges:**

The `ALTER TABLE EXECUTE` privilege is required, which is a child of `ALTER TABLE` in the ClickHouse access control hierarchy. You can grant it specifically or via the parent:

```sql
-- Grant only EXECUTE permission
GRANT ALTER TABLE EXECUTE ON my_iceberg_table TO my_user;

-- Or grant all ALTER TABLE permissions (includes ALTER TABLE EXECUTE)
GRANT ALTER TABLE ON my_iceberg_table TO my_user;
```

<Note>
- Only Iceberg format version 2 tables are supported (v1 snapshots do not guarantee `manifest-list`, which is required to safely identify files for cleanup)
- The current snapshot is always preserved, even if it is older than the specified timestamp
- Requires the `allow_insert_into_iceberg` setting to be enabled
- Requires the `allow_experimental_expire_snapshots` setting to be enabled
- The catalog's own authorization (REST catalog auth, AWS Glue IAM, etc.) is enforced independently when ClickHouse updates the metadata
</Note>

### Remove Orphan Files {#iceberg-remove-orphan-files}

Orphan files are files on storage that are not referenced by any snapshot in the Iceberg table metadata. They accumulate from failed writes, partial cleanup after compaction, and interrupted operations, causing unbounded storage growth. The `remove_orphan_files` command identifies and removes these orphan files.

**Syntax:**

```sql
-- Positional form: single unnamed older_than argument
ALTER TABLE iceberg_table EXECUTE remove_orphan_files('timestamp')

-- Named form
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    older_than = 'timestamp',
    location = 'path',
    dry_run = 0|1
)

-- No arguments: use all defaults (older_than = 3 days ago)
ALTER TABLE iceberg_table EXECUTE remove_orphan_files()
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `older_than` | `String` (timestamp) | 3 days ago (configurable via `iceberg_orphan_files_older_than_seconds`) | Only consider files with a last-modified time older than this timestamp as orphan candidates. Safety guard against deleting files from in-progress writes. |
| `location` | `String` | Table location | Restrict the scan to a specific subdirectory under the table location (e.g., `'data/'` or `'metadata/'`). |
| `dry_run` | `UInt64` | `0` | When `1`, identify orphan files and return the result summary without actually deleting anything. |

**Examples:**

```sql
-- Remove orphan files older than a specific timestamp
ALTER TABLE iceberg_table EXECUTE remove_orphan_files('2026-03-01 00:00:00');

-- Dry run: preview which files would be deleted
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(dry_run = 1);

-- Scan only the data directory
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    older_than = '2026-03-01 00:00:00',
    location = 'data/'
);

-- Combine positional older_than with named arguments
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    '2026-03-01 00:00:00',
    dry_run = 1
);
```

**Output:**

The command returns a table with `metric_name` and `metric_value` columns showing the count of deleted (or would-be-deleted in dry_run mode) files by category. File categories are classified using best-effort heuristics based on file naming conventions; files that do not match any specific pattern default to `deleted_data_files_count`:

| metric_name | metric_value |
|---|---|
| deleted_data_files_count | 5 |
| deleted_position_delete_files_count | 2 |
| deleted_equality_delete_files_count | 0 |
| deleted_manifest_files_count | 3 |
| deleted_manifest_lists_count | 1 |
| deleted_metadata_files_count | 0 |
| deleted_statistics_files_count | 0 |
| skipped_missing_metadata_count | 0 |
| failed_deletions_count | 0 |

**Settings:**

| Setting | Type | Default | Description |
|---|---|---|---|
| `allow_iceberg_remove_orphan_files` | `Bool` | `false` | Gate setting to enable the feature (experimental). |
| `iceberg_orphan_files_older_than_seconds` | `UInt64` | `259200` (3 days) | Default `older_than` threshold in seconds when the argument is omitted. |

<Note>
- **Requires Iceberg format version 2 (or higher).** Version 1 tables are rejected because they lack `manifest-list` pointers in snapshots, which are needed to safely determine the reachable file set. Running the command on a v1 table returns a `BAD_ARGUMENTS` error.
- Requires both `allow_insert_into_iceberg` and `allow_iceberg_remove_orphan_files` settings to be enabled
- It is recommended to run `expire_snapshots` before `remove_orphan_files` so that files uniquely referenced by expired snapshots are cleaned up first
- Use `dry_run = 1` to preview orphan files before deletion
- The `older_than` threshold protects against deleting files from in-progress writes — the default 3-day threshold provides a generous safety margin
</Note>

## See Also {#see-also}

* [Iceberg engine](/reference/engines/table-engines/integrations/iceberg)
* [Iceberg cluster table function](/reference/functions/table-functions/icebergCluster)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction},
        {.allow_readonly = false});
    factory.registerFunction<TableFunctionIcebergS3>(
         {.description = R"(The table function can be used to read the Iceberg table stored on S3 object store.)",
            .examples{{IcebergS3Definition::name, "SELECT * FROM icebergS3(url, access_key_id, secret_access_key)", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
        {.allow_readonly = false});

#endif
#if USE_AZURE_BLOB_STORAGE
    factory.registerFunction<TableFunctionIcebergAzure>(
         {.description = R"(The table function can be used to read the Iceberg table stored on Azure object store.)",
            .examples{{IcebergAzureDefinition::name, "SELECT * FROM icebergAzure(url, access_key_id, secret_access_key)", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         {.allow_readonly = false});
#endif
#if USE_HDFS
    factory.registerFunction<TableFunctionIcebergHDFS>(
         {.description = R"(The table function can be used to read the Iceberg table stored on HDFS virtual filesystem.)",
            .examples{{IcebergHDFSDefinition::name, "SELECT * FROM icebergHDFS(url)", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         {.allow_readonly = false});
#endif
    factory.registerFunction<TableFunctionIcebergLocal>(
         {.description = R"(The table function can be used to read the Iceberg table stored locally.)",
            .examples{{IcebergLocalDefinition::name, "SELECT * FROM icebergLocal(filename)", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         {.allow_readonly = false});
}
#endif


#if USE_AVRO
void registerTableFunctionPaimon(TableFunctionFactory & factory);
void registerTableFunctionPaimon(TableFunctionFactory & factory)
{
#if USE_AWS_S3
    factory.registerFunction<TableFunctionPaimon>(
         {.description = R"DOCS_MD(
import ExperimentalBadge from "/snippets/components/ExperimentalBadge/ExperimentalBadge.jsx";

<ExperimentalBadge />

Provides a read-only table-like interface to Apache [Paimon](https://paimon.apache.org/) tables in Amazon S3, Azure, HDFS or locally stored.

## Syntax {#syntax}

```sql
paimon(url [,access_key_id, secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

paimonS3(url [,access_key_id, secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

paimonAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

paimonHDFS(path_to_table, [,format] [,compression_method])

paimonLocal(path_to_table, [,format] [,compression_method])
```

## Arguments {#arguments}

Description of the arguments coincides with description of arguments in table functions `s3`, `azureBlobStorage`, `HDFS` and `file` correspondingly.
`format` stands for the format of data files in the Paimon table.

For `paimonS3`, an optional `extra_credentials` parameter can be used to pass a `role_arn` for role-based access in ClickHouse Cloud. See [Secure S3](/products/cloud/guides/data-sources/accessing-s3-data-securely) for configuration steps.

### Returned value {#returned-value}

A table with the specified structure for reading data in the specified Paimon table.

## Defining a named collection {#defining-a-named-collection}

Here is an example of configuring a named collection for storing the URL and credentials:

```xml
<clickhouse>
    <named_collections>
        <paimon_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
            <format>auto</format>
            <structure>auto</structure>
        </paimon_conf>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM paimonS3(paimon_conf, filename = 'test_table')
DESCRIBE paimonS3(paimon_conf, filename = 'test_table')
```

## Aliases {#aliases}

Table function `paimon` is an alias to `paimonS3` now.

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_etag` — The etag of the file. Type: `LowCardinality(String)`. If the etag is unknown, the value is `NULL`.

## Data Types supported {#data-types-supported}

| Paimon Data Type | Clickhouse Data Type 
|-------|--------|
|BOOLEAN     |Int8      |
|TINYINT     |Int8      |
|SMALLINT     |Int16      |
|INTEGER     |Int32      |
|BIGINT     |Int64      |
|FLOAT     |Float32      |
|DOUBLE     |Float64      |
|STRING,VARCHAR,BYTES,VARBINARY     |String      |
|DATE     |Date      |
|TIME(p),TIME     |Time('UTC')      |
|TIMESTAMP(p) WITH LOCAL TIME ZONE     |DateTime64      |
|TIMESTAMP(p)     |DateTime64('UTC')      |
|CHAR     |FixedString(1)      |
|BINARY(n)     |FixedString(n)      |
|DECIMAL(P,S)     |Decimal(P,S)      |
|ARRAY     |Array      |
|MAP     |Map    |

## Partition supported {#partition-supported}
Data types supported in Paimon partition keys:
* `CHAR`
* `VARCHAR`
* `BOOLEAN`
* `DECIMAL`
* `TINYINT`
* `SMALLINT`
* `INTEGER`
* `DATE`
* `TIME`
* `TIMESTAMP`
* `TIMESTAMP WITH LOCAL TIME ZONE`
* `BIGINT`
* `FLOAT`
* `DOUBLE`

## See Also {#see-also}

* [Paimon cluster table function](/reference/functions/table-functions/paimonCluster)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction},
         {.allow_readonly = false});
    factory.registerFunction<TableFunctionPaimonS3>(
         {.description = R"(The table function can be used to read the Paimon table stored on S3 object store.)",
            .examples{{"paimonS3", "SELECT * FROM paimonS3(url, access_key_id, secret_access_key)", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         {.allow_readonly = false});

#endif
#if USE_AZURE_BLOB_STORAGE
    factory.registerFunction<TableFunctionPaimonAzure>(
         {.description = R"(The table function can be used to read the Paimon table stored on Azure object store.)",
            .examples{{"paimonAzure", "SELECT * FROM paimonAzure(url, access_key_id, secret_access_key)", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         {.allow_readonly = false});
#endif
#if USE_HDFS
    factory.registerFunction<TableFunctionPaimonHDFS>(
         {.description = R"(The table function can be used to read the Paimon table stored on HDFS virtual filesystem.)",
            .examples{{"paimonHDFS", "SELECT * FROM paimonHDFS(url)", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         {.allow_readonly = false});
#endif
    factory.registerFunction<TableFunctionPaimonLocal>(
         {.description = R"(The table function can be used to read the Paimon table stored locally.)",
            .examples{{"paimonLocal", "SELECT * FROM paimonLocal(filename)", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         {.allow_readonly = false});
}
#endif

#if USE_PARQUET && USE_DELTA_KERNEL_RS
void registerTableFunctionDeltaLake(TableFunctionFactory & factory);
void registerTableFunctionDeltaLake(TableFunctionFactory & factory)
{
#if USE_AWS_S3
    factory.registerFunction<TableFunctionDeltaLake>(
         {.description = R"DOCS_MD(
Provides a table-like interface to [Delta Lake](https://github.com/delta-io/delta) tables in Amazon S3, Azure Blob Storage, or a locally mounted file system, supporting both reads and writes (from v25.10)

## Syntax {#syntax}

`deltaLake` is an alias of `deltaLakeS3` which is supported for compatibility.

```sql
deltaLake(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

deltaLakeS3(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

deltaLakeAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

deltaLakeLocal(path, [,format])
```

## Arguments {#arguments}

The arguments for this table function are the same as for the `s3`, `azureBlobStorage`, `HDFS` and `file` table functions respectively.
The `format` argument stands for the format of data files in the Delta lake table.

An optional `extra_credentials` parameter can be used to pass a `role_arn` for role-based access in ClickHouse Cloud. See [Secure S3](/products/cloud/guides/data-sources/accessing-s3-data-securely) for configuration steps.

## Returned value {#returned_value}

Returns a table with the specified structure for reading or writing data from/to the specified Delta Lake table.

## Examples {#examples}

### Reading data {#reading-data}

Consider a table in S3 storage at `https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/`.
To read data from the table in ClickHouse, run:

```sql title="Query"
SELECT
    URL,
    UserAgent
FROM deltaLake('https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/')
WHERE URL IS NOT NULL
LIMIT 2
```

```response title="Response"
┌─URL───────────────────────────────────────────────────────────────────┬─UserAgent─┐
│ http://auto.ria.ua/search/index.kz/jobinmoscow/detail/55089/hasimages │         1 │
│ http://auto.ria.ua/search/index.kz/jobinmoscow.ru/gosushi             │         1 │
└───────────────────────────────────────────────────────────────────────┴───────────┘
```

### Inserting data {#inserting-data}

Consider a table in S3 storage at `s3://ch-docs-s3-bucket/people_10k/`.
Delta Lake writes are a Beta feature disabled by default. Enable them with the following (`allow_delta_lake_writes` is available from version 26.7; on earlier versions use `allow_experimental_delta_lake_writes`):

```sql title="Query"
SET allow_delta_lake_writes=1
```

Then write:

```sql title="Query"
INSERT INTO TABLE FUNCTION deltaLake('s3://ch-docs-s3-bucket/people_10k/', '<access_key>', '<secret>') VALUES (10001, 'John', 'Smith', 'Male', 30)
```

```response title="Response"
Query id: 09069b47-89fa-4660-9e42-3d8b1dde9b17

Ok.

1 row in set. Elapsed: 3.426 sec.
```

You can confirm the insert worked by reading the table again:

```sql title="Query"
SELECT *
FROM deltaLake('s3://ch-docs-s3-bucket/people_10k/', '<access_key>', '<secret>')
WHERE (firstname = 'John') AND (lastname = 'Smith')
```

```response title="Response"
Query id: 65032944-bed6-4d45-86b3-a71205a2b659

   ┌────id─┬─firstname─┬─lastname─┬─gender─┬─age─┐
1. │ 10001 │ John      │ Smith    │ Male   │  30 │
   └───────┴───────────┴──────────┴────────┴─────┘
```

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_etag` — The etag of the file. Type: `LowCardinality(String)`. If the etag is unknown, the value is `NULL`.

## Related {#related}

- [DeltaLake engine](/reference/engines/table-engines/integrations/deltalake)
- [DeltaLake cluster table function](/reference/functions/table-functions/deltalakeCluster)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction},
         {.allow_readonly = false});

    factory.registerFunction<TableFunctionDeltaLakeS3>(
         {.description = R"(The table function can be used to read the DeltaLake table stored on S3.)",
            .examples{{DeltaLakeS3Definition::name, "SELECT * FROM deltaLakeS3(url, access_key_id, secret_access_key)", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         {.allow_readonly = false});
#endif

#if USE_AZURE_BLOB_STORAGE
    factory.registerFunction<TableFunctionDeltaLakeAzure>(
         {.description = R"(The table function can be used to read the DeltaLake table stored on Azure object store.)",
            .examples{{DeltaLakeAzureDefinition::name, "SELECT * FROM deltaLakeAzure(connection_string|storage_account_url, container_name, blobpath, \"\n"
 "                \"[account_name, account_key, format, compression, structure])", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         {.allow_readonly = false});
#endif
    // Register the new local Delta Lake table function
    factory.registerFunction<TableFunctionDeltaLakeLocal>(
         {.description = R"(The table function can be used to read the DeltaLake table stored locally.)",
            .examples{{DeltaLakeLocalDefinition::name, "SELECT * FROM deltaLakeLocal(path)", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         {.allow_readonly = false});
}
#endif

#if USE_AWS_S3
void registerTableFunctionHudi(TableFunctionFactory & factory);
void registerTableFunctionHudi(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHudi>(
         {.description = R"DOCS_MD(
Provides a read-only table-like interface to Apache [Hudi](https://hudi.apache.org/) tables in Amazon S3.

## Syntax {#syntax}

```sql
hudi(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
```

## Arguments {#arguments}

| Argument                                     | Description                                                                                                                                                                                                                                                                                                                                                                            |
|----------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `url`                                        | Bucket url with the path to an existing Hudi table in S3.                                                                                                                                                                                                                                                                                                                              |
| `aws_access_key_id`, `aws_secret_access_key` | Long-term credentials for the [AWS](https://aws.amazon.com/) account user.  You can use these to authenticate your requests. These parameters are optional. If credentials are not specified, they are used from the ClickHouse configuration. For more information see [Using S3 for Data Storage](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-s3).  |
| `format`                                     | The [format](/reference/formats/index) of the file.                                                                                                                                                                                                                                                                                                                                         |
| `structure`                                  | Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                          |
| `compression`                                | Parameter is optional. Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. By default, compression will be autodetected by the file extension.                                                                                                                                                                                                                    |
| `extra_credentials`                          | Parameter is optional. Used to pass a `role_arn` for role-based access in ClickHouse Cloud. See [Secure S3](/products/cloud/guides/data-sources/accessing-s3-data-securely) for configuration steps.                                                                                                                                                                                                                    |

## Returned value {#returned_value}

A table with the specified structure for reading data in the specified Hudi table in S3.

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_etag` — The etag of the file. Type: `LowCardinality(String)`. If the etag is unknown, the value is `NULL`.

## Related {#related}

- [Hudi engine](/reference/engines/table-engines/integrations/hudi)
- [Hudi cluster table function](/reference/functions/table-functions/hudiCluster)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction},
         {.allow_readonly = false});
}
#endif

void registerDataLakeTableFunctions(TableFunctionFactory & factory)
{
    UNUSED(factory);
#if USE_AVRO
    registerTableFunctionIceberg(factory);
#endif

#if USE_AVRO
    registerTableFunctionPaimon(factory);
#endif

#if USE_PARQUET && USE_DELTA_KERNEL_RS
    registerTableFunctionDeltaLake(factory);
#endif
#if USE_AWS_S3
    registerTableFunctionHudi(factory);
#endif
}
}
