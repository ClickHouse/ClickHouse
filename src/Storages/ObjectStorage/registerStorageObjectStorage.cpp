#include <Core/FormatFactorySettings.h>
#include <Core/Settings.h>
#include <Databases/DataLake/ICatalog.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSettings.h>
#include <Storages/ObjectStorage/StorageObjectStorageDefinitions.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/StorageFactory.h>
#include <Poco/Logger.h>
#include <Disks/DiskType.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

namespace Setting
{
    extern const SettingsBool write_full_path_in_iceberg_metadata;
    extern const SettingsBool allow_experimental_paimon_storage_engine;
}

namespace DataLakeStorageSetting
{
    extern const DataLakeStorageSettingsString disk;
}

namespace
{

// LocalObjectStorage is only supported for Iceberg Datalake operations where Avro format is required. For regular file access, use FileStorage instead.
#if USE_AWS_S3 || USE_AZURE_BLOB_STORAGE || USE_HDFS || USE_AVRO

std::shared_ptr<StorageObjectStorage>
createStorageObjectStorage(const StorageFactory::Arguments & args, StorageObjectStorageConfigurationPtr configuration)
{
    const auto context = args.getLocalContext();
    StorageObjectStorageConfiguration::initialize(*configuration, args.engine_args, context, false, &args.table_id);

    // Use format settings from global server context + settings from
    // the SETTINGS clause of the create query. Settings from current
    // session and user are ignored.
    std::optional<FormatSettings> format_settings;
    if (args.storage_def->settings)
    {
        Settings settings = context->getSettingsCopy();

        // Apply changes from SETTINGS clause, with validation.
        settings.applyChanges(args.storage_def->settings->changes);

        format_settings = getFormatSettings(context, settings);
    }
    else
    {
        format_settings = getFormatSettings(context);
    }

    ASTPtr partition_by;
    if (args.storage_def->partition_by)
        partition_by = args.storage_def->partition_by->clone();

    ASTPtr order_by;
    if (args.storage_def->order_by)
        order_by = args.storage_def->order_by->clone();

    ContextMutablePtr context_copy = Context::createCopy(args.getContext());
    Settings settings_copy = args.getLocalContext()->getSettingsCopy();
    context_copy->setSettings(settings_copy);
    return std::make_shared<StorageObjectStorage>(
        configuration,
        // We only want to perform write actions (e.g. create a container in Azure) when the table is being created,
        // and we want to avoid it when we load the table after a server restart.
        configuration->createObjectStorage(context, /* is_readonly */ args.mode != LoadingStrictnessLevel::CREATE, std::nullopt),
        context_copy, /// Use global context.
        args.table_id,
        args.columns,
        args.constraints,
        args.comment,
        format_settings,
        args.mode,
        configuration->getCatalog(context, args.table_id),
        args.query.if_not_exists,
        /* is_datalake_query*/ false,
        /* distributed_processing */ false,
        partition_by,
        order_by);
}

#endif
}

#if USE_AZURE_BLOB_STORAGE
static void registerStorageAzure(StorageFactory & factory)
{
    factory.registerStorage(AzureDefinition::storage_engine_name, [](const StorageFactory::Arguments & args)
    {
        auto configuration = std::make_shared<StorageAzureConfiguration>();
        return createStorageObjectStorage(args, configuration);
    },
    {
        .supports_settings = true,
        .supports_sort_order = true, // for partition by
        .supports_schema_inference = true,
        .source_access_type = AccessTypeObjects::Source::AZURE,
        .has_builtin_setting_fn = StorageObjectStorageSettings::hasBuiltin,
    },
    Documentation{
        .description = R"DOCS_MD(
This engine provides an integration with [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) ecosystem.

## Create table {#create-table}

```sql
CREATE TABLE azure_blob_storage_table (name String, value UInt32)
    ENGINE = AzureBlobStorage(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, partition_strategy, partition_columns_in_data_file, extra_credentials(client_id=, tenant_id=)])
    [PARTITION BY expr]
    [SETTINGS ...]
```

### Engine parameters {#engine-parameters}

- `endpoint` — AzureBlobStorage endpoint URL with container & prefix. Optionally can contain account_name if the authentication method used needs it. (`http://azurite1:{port}/[account_name]{container_name}/{data_prefix}`) or these parameters can be provided separately using storage_account_url, account_name & container. For specifying prefix, endpoint should be used.
- `endpoint_contains_account_name` - This flag is used to specify if endpoint contains account_name as it is only needed for certain authentication methods. (Default : true)
- `connection_string|storage_account_url` — connection_string includes account name & key ([Create connection string](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)) or you could also provide the storage account url here and account name & account key as separate parameters (see parameters account_name & account_key)
- `container_name` - Container name
- `blobpath` - file path. Supports following wildcards in readonly mode: `*`, `**`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc'`, `'def'` — strings.
- `account_name` - if storage_account_url is used, then account name can be specified here
- `account_key` - if storage_account_url is used, then account key can be specified here
- `format` — The [format](/interfaces/formats.md) of the file.
- `compression` — Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. By default, it will autodetect compression by file extension. (same as setting to `auto`).
- `partition_strategy` – Options: `WILDCARD` or `HIVE`. `WILDCARD` requires a `{_partition_id}` in the path, which is replaced with the partition key. `HIVE` does not allow wildcards, assumes the path is the table root, and generates Hive-style partitioned directories with Snowflake IDs as filenames and the file format as the extension. When `PARTITION BY` is used without an explicit `partition_strategy`, the default is taken from the `file_like_engine_default_partition_strategy` setting, which defaults to `HIVE`.
- `partition_columns_in_data_file` - Only used with `HIVE` partition strategy. Tells ClickHouse whether to expect partition columns to be written in the data file. Defaults `false`.
- `extra_credentials` - Use `client_id` and `tenant_id` for authentication. If extra_credentials are provided, they are given priority over `account_name` and `account_key`.

**Example**

Users can use the Azurite emulator for local Azure Storage development. Further details [here](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub%2Cblob-storage). If using a local instance of Azurite, users may need to substitute `http://localhost:10000` for `http://azurite1:10000` in the commands below, where we assume Azurite is available at host `azurite1`.

```sql
CREATE TABLE test_table (key UInt64, data String)
    ENGINE = AzureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', 'test_table', 'CSV');

INSERT INTO test_table VALUES (1, 'a'), (2, 'b'), (3, 'c');

SELECT * FROM test_table;
```

```text
┌─key──┬─data──┐
│  1   │   a   │
│  2   │   b   │
│  3   │   c   │
└──────┴───────┘
```

## Virtual columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.

## Authentication {#authentication}

Currently there are 3 ways to authenticate:
- `Managed Identity` - Can be used by providing an `endpoint`, `connection_string` or `storage_account_url`.
- `SAS Token` - Can be used by providing an `endpoint`, `connection_string` or `storage_account_url`. It is identified by presence of '?' in the url. See [azureBlobStorage](/sql-reference/table-functions/azureBlobStorage#using-shared-access-signatures-sas-sas-tokens) for examples.
- `Workload Identity` - Can be used by providing an `endpoint` or `storage_account_url`. If `use_workload_identity` parameter is set in config, ([workload identity](https://github.com/Azure/azure-sdk-for-cpp/tree/main/sdk/identity/azure-identity#authenticate-azure-hosted-applications)) is used for authentication.

### Data cache {#data-cache}

`Azure` table engine supports data caching on local disk.
See filesystem cache configuration options and usage in this [section](/operations/storing-data.md/#using-local-cache).
Caching is made depending on the path and ETag of the storage object, so clickhouse will not read a stale cache version.

To enable caching use a setting `filesystem_cache_name = '<name>'` and `enable_filesystem_cache = 1`.

```sql
SELECT *
FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', 'test_table', 'CSV')
SETTINGS filesystem_cache_name = 'cache_for_azure', enable_filesystem_cache = 1;
```

1. add the following section to clickhouse configuration file:

```xml
<clickhouse>
    <filesystem_caches>
        <cache_for_azure>
            <path>path to cache directory</path>
            <max_size>10Gi</max_size>
        </cache_for_azure>
    </filesystem_caches>
</clickhouse>
```

2. reuse cache configuration (and therefore cache storage) from clickhouse `storage_configuration` section, [described here](/operations/storing-data.md/#using-local-cache)

### PARTITION BY {#partition-by}

`PARTITION BY` — Optional. In most cases you don't need a partition key, and if it is needed you generally don't need a partition key more granular than by month. Partitioning does not speed up queries (in contrast to the ORDER BY expression). You should never use too granular partitioning. Don't partition your data by client identifiers or names (instead, make client identifier or name the first column in the ORDER BY expression).

For partitioning by month, use the `toYYYYMM(date_column)` expression, where `date_column` is a column with a date of the type [Date](/sql-reference/data-types/date.md). The partition names here have the `"YYYYMM"` format.

#### Partition strategy {#partition-strategy}

`WILDCARD`: Replaces the `{_partition_id}` wildcard in the file path with the actual partition key. Reading is not supported.

`HIVE` (the default) implements hive style partitioning for reads & writes. Reading is implemented using a recursive glob pattern. Writing generates files using the following format: `<prefix>/<key1=val1/key2=val2...>/<snowflakeid>.<toLower(file_format)>`.

Note: When using `HIVE` partition strategy, the `use_hive_partitioning` setting has no effect.

Example of `HIVE` partition strategy:

```sql
arthur :) create table azure_table (year UInt16, country String, counter UInt8) ENGINE=AzureBlobStorage(account_name='devstoreaccount1', account_key='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', storage_account_url = 'http://localhost:30000/devstoreaccount1', container='cont', blob_path='hive_partitioned', format='Parquet', compression='auto', partition_strategy='hive') PARTITION BY (year, country);

arthur :) insert into azure_table values (2020, 'Russia', 1), (2021, 'Brazil', 2);

arthur :) select _path, * from azure_table;

┌─_path──────────────────────────────────────────────────────────────────────┬─year─┬─country─┬─counter─┐
│ cont/hive_partitioned/year=2020/country=Russia/7351305360873664512.parquet │ 2020 │ Russia  │       1 │
│ cont/hive_partitioned/year=2021/country=Brazil/7351305360894636032.parquet │ 2021 │ Brazil  │       2 │
└────────────────────────────────────────────────────────────────────────────┴──────┴─────────┴─────────┘
```

## See also {#see-also}

[Azure Blob Storage Table Function](/sql-reference/table-functions/azureBlobStorage)
)DOCS_MD",
        .syntax = "ENGINE = AzureBlobStorage(connection_string | storage_account_url, container_name, blobpath, "
            "[account_name, account_key,] format [, compression])",
        .related = {"S3", "HDFS"}});
}
#endif

#if USE_AWS_S3
static void registerStorageS3Impl(const String & name, StorageFactory & factory)
{
    /// The full embedded documentation below describes the Amazon `S3` engine. This same implementation
    /// is also registered under the S3-compatible `COSN`, `OSS`, and `GCS` engine names; give those a
    /// concise provider-specific description instead of the S3-specific page.
    String description;
    if (name == S3Definition::storage_engine_name)
    {
        description = R"DOCS_MD(
This engine provides integration with the [Amazon S3](https://aws.amazon.com/s3/) ecosystem. This engine is similar to the [HDFS](/engines/table-engines/integrations/hdfs) engine, but provides S3-specific features.

## Example {#example}

```sql
CREATE TABLE s3_engine_table (name String, value UInt32)
    ENGINE=S3('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'gzip')
    SETTINGS input_format_with_names_use_header = 0;

INSERT INTO s3_engine_table VALUES ('one', 1), ('two', 2), ('three', 3);

SELECT * FROM s3_engine_table LIMIT 2;
```

```text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## Create a table {#creating-a-table}

```sql
CREATE TABLE s3_engine_table (name String, value UInt32)
    ENGINE = S3(path [, NOSIGN | aws_access_key_id, aws_secret_access_key,] format, [compression], [partition_strategy], [partition_columns_in_data_file], [extra_credentials])
    [PARTITION BY expr]
    [SETTINGS ...]
```

### Engine parameters {#parameters}

- `path` — Bucket url with path to file. Supports following wildcards in readonly mode: `*`, `**`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc'`, `'def'` — strings. For more information see [below](#wildcards-in-path).
- `NOSIGN` - If this keyword is provided in place of credentials, all the requests will not be signed.
- `format` — The [format](/sql-reference/formats#formats-overview) of the file.
- `aws_access_key_id`, `aws_secret_access_key` - Long-term credentials for the [AWS](https://aws.amazon.com/) account user.  You can use these to authenticate your requests. Parameter is optional. If credentials are not specified, they are used from the configuration file. For more information see [Using S3 for Data Storage](../mergetree-family/mergetree.md#table_engine-mergetree-s3).
- `compression` — Compression type. Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. Parameter is optional. By default, it will auto-detect compression by file extension.
- `partition_strategy` – Options: `WILDCARD` or `HIVE`. `WILDCARD` requires a `{_partition_id}` in the path, which is replaced with the partition key. `HIVE` does not allow wildcards, assumes the path is the table root, and generates Hive-style partitioned directories with Snowflake IDs as filenames and the file format as the extension. When `PARTITION BY` is used without an explicit `partition_strategy`, the default is taken from the `file_like_engine_default_partition_strategy` setting, which defaults to `HIVE`.
- `partition_columns_in_data_file` - Only used with `HIVE` partition strategy. Tells ClickHouse whether to expect partition columns to be written in the data file. Defaults `false`.
- `storage_class_name` - Options: `STANDARD` or `INTELLIGENT_TIERING`, allow to specify [AWS S3 Intelligent Tiering](https://aws.amazon.com/s3/storage-classes/intelligent-tiering/).
- `extra_credentials` - Optional. Used to pass a `role_arn` for role-based access in ClickHouse Cloud. See [Secure S3](/cloud/data-sources/secure-s3) for configuration steps.

### Data cache {#data-cache}

`S3` table engine supports data caching on local disk.
See filesystem cache configuration options and usage in this [section](/operations/storing-data.md/#using-local-cache).
Caching is made depending on the path and ETag of the storage object, so clickhouse will not read a stale cache version.

To enable caching use a setting `filesystem_cache_name = '<name>'` and `enable_filesystem_cache = 1`.

```sql
SELECT *
FROM s3('http://minio:10000/clickhouse//test_3.csv', 'minioadmin', 'minioadminpassword', 'CSV')
SETTINGS filesystem_cache_name = 'cache_for_s3', enable_filesystem_cache = 1;
```

There are two ways to define cache in configuration file.

1. add the following section to clickhouse configuration file:

```xml
<clickhouse>
    <filesystem_caches>
        <cache_for_s3>
            <path>path to cache directory</path>
            <max_size>10Gi</max_size>
        </cache_for_s3>
    </filesystem_caches>
</clickhouse>
```

2. reuse cache configuration (and therefore cache storage) from clickhouse `storage_configuration` section, [described here](/operations/storing-data.md/#using-local-cache)

### PARTITION BY {#partition-by}

`PARTITION BY` — Optional. In most cases you don't need a partition key, and if it is needed you generally don't need a partition key more granular than by month. Partitioning does not speed up queries (in contrast to the ORDER BY expression). You should never use too granular partitioning. Don't partition your data by client identifiers or names (instead, make client identifier or name the first column in the ORDER BY expression).

For partitioning by month, use the `toYYYYMM(date_column)` expression, where `date_column` is a column with a date of the type [Date](/sql-reference/data-types/date.md). The partition names here have the `"YYYYMM"` format.

#### Partition strategy {#partition-strategy}

`WILDCARD`: Replaces the `{_partition_id}` wildcard in the file path with the actual partition key. Reading is not supported.

`HIVE` (the default) implements hive style partitioning for reads & writes. Reading is implemented using a recursive glob pattern, it is equivalent to `SELECT * FROM s3('table_root/**.parquet')`.
Writing generates files using the following format: `<prefix>/<key1=val1/key2=val2...>/<snowflakeid>.<toLower(file_format)>`.

Note: When using `HIVE` partition strategy, the `use_hive_partitioning` setting has no effect.

Example of `HIVE` partition strategy:

```sql
arthur :) CREATE TABLE t_03363_parquet (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_03363_parquet', format = Parquet, partition_strategy='hive')
PARTITION BY (year, country);

arthur :) INSERT INTO t_03363_parquet VALUES
    (2022, 'USA', 1),
    (2022, 'Canada', 2),
    (2023, 'USA', 3),
    (2023, 'Mexico', 4),
    (2024, 'France', 5),
    (2024, 'Germany', 6),
    (2024, 'Germany', 7),
    (1999, 'Brazil', 8),
    (2100, 'Japan', 9),
    (2024, 'CN', 10),
    (2025, '', 11);

arthur :) select _path, * from t_03363_parquet;

┌─_path──────────────────────────────────────────────────────────────────────┬─year─┬─country─┬─counter─┐
│ test/t_03363_parquet/year=2100/country=Japan/7329604473272971264.parquet   │ 2100 │ Japan   │       9 │
│ test/t_03363_parquet/year=2024/country=France/7329604473323302912.parquet  │ 2024 │ France  │       5 │
│ test/t_03363_parquet/year=2022/country=Canada/7329604473314914304.parquet  │ 2022 │ Canada  │       2 │
│ test/t_03363_parquet/year=1999/country=Brazil/7329604473289748480.parquet  │ 1999 │ Brazil  │       8 │
│ test/t_03363_parquet/year=2023/country=Mexico/7329604473293942784.parquet  │ 2023 │ Mexico  │       4 │
│ test/t_03363_parquet/year=2023/country=USA/7329604473319108608.parquet     │ 2023 │ USA     │       3 │
│ test/t_03363_parquet/year=2025/country=/7329604473327497216.parquet        │ 2025 │         │      11 │
│ test/t_03363_parquet/year=2024/country=CN/7329604473310720000.parquet      │ 2024 │ CN      │      10 │
│ test/t_03363_parquet/year=2022/country=USA/7329604473298137088.parquet     │ 2022 │ USA     │       1 │
│ test/t_03363_parquet/year=2024/country=Germany/7329604473306525696.parquet │ 2024 │ Germany │       6 │
│ test/t_03363_parquet/year=2024/country=Germany/7329604473306525696.parquet │ 2024 │ Germany │       7 │
└────────────────────────────────────────────────────────────────────────────┴──────┴─────────┴─────────┘
```

### Querying partitioned data {#querying-partitioned-data}

This example uses the [docker compose recipe](https://github.com/ClickHouse/examples/tree/5fdc6ff72f4e5137e23ea075c88d3f44b0202490/docker-compose-recipes/recipes/ch-and-minio-S3), which integrates ClickHouse and MinIO.  You should be able to reproduce the same queries using S3 by replacing the endpoint and authentication values.

Notice that the S3 endpoint in the `ENGINE` configuration uses the parameter token `{_partition_id}` as part of the S3 object (filename), and that the SELECT queries select against those resulting object names (e.g., `test_3.csv`).

:::note
As shown in the example, querying from S3 tables that are partitioned is
not directly supported at this time, but can be accomplished by querying the individual partitions
using the S3 table function.

The primary use-case for writing
partitioned data in S3 is to enable transferring that data into another
ClickHouse system (for example, moving from on-prem systems to ClickHouse
Cloud).  Because ClickHouse datasets are often very large, and network
reliability is sometimes imperfect it makes sense to transfer datasets
in subsets, hence partitioned writes.
:::

#### Create the table {#create-the-table}
```sql
CREATE TABLE p
(
    `column1` UInt32,
    `column2` UInt32,
    `column3` UInt32
)
ENGINE = S3(
-- highlight-next-line
           'http://minio:10000/clickhouse//test_{_partition_id}.csv',
           'minioadmin',
           'minioadminpassword',
           'CSV')
PARTITION BY column3
```

#### Insert data {#insert-data}
```sql
INSERT INTO p VALUES (1, 2, 3), (3, 2, 1), (78, 43, 45)
```

#### Select from partition 3 {#select-from-partition-3}

:::tip
This query uses the s3 table function
:::

```sql
SELECT *
FROM s3('http://minio:10000/clickhouse//test_3.csv', 'minioadmin', 'minioadminpassword', 'CSV')
```
```response
┌─c1─┬─c2─┬─c3─┐
│  1 │  2 │  3 │
└────┴────┴────┘
```

#### Select from partition 1 {#select-from-partition-1}
```sql
SELECT *
FROM s3('http://minio:10000/clickhouse//test_1.csv', 'minioadmin', 'minioadminpassword', 'CSV')
```
```response
┌─c1─┬─c2─┬─c3─┐
│  3 │  2 │  1 │
└────┴────┴────┘
```

#### Select from partition 45 {#select-from-partition-45}
```sql
SELECT *
FROM s3('http://minio:10000/clickhouse//test_45.csv', 'minioadmin', 'minioadminpassword', 'CSV')
```
```response
┌─c1─┬─c2─┬─c3─┐
│ 78 │ 43 │ 45 │
└────┴────┴────┘
```

#### Limitation {#limitation}

You may naturally try to `Select * from p`, but as noted above, this query will fail; use the preceding query.

```sql
SELECT * FROM p
```
```response
Received exception from server (version 23.4.1):
Code: 48. DB::Exception: Received from localhost:9000. DB::Exception: Reading from a partitioned S3 storage is not implemented yet. (NOT_IMPLEMENTED)
```

## Insert data {#inserting-data}

Note that rows can only be inserted into new files. There are no merge cycles or file split operations. Once a file is written, subsequent inserts will fail. To avoid this you can use `s3_truncate_on_insert` and `s3_create_new_file_on_insert` settings. See more details [here](/integrations/s3#inserting-data).

## Virtual columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_etag` — ETag of the file. Type: `LowCardinality(String)`. If the etag is unknown, the value is `NULL`.
- `_tags` — Tags of the file. Type: `Map(String, String)`. If no tag exist, the value is an empty map `{}'.

For more information about virtual columns see [here](../../../engines/table-engines/index.md#table_engines-virtual_columns).

## Implementation details {#implementation-details}

- Reads and writes can be parallel
- Not supported:
  - `ALTER` and `SELECT...SAMPLE` operations.
  - Indexes.
  - [Zero-copy](../../../operations/storing-data.md#zero-copy) replication is possible, but not supported.

:::note Zero-copy replication is not ready for production
Zero-copy replication is disabled by default in ClickHouse version 22.8 and higher.  This feature is not recommended for production use.
:::

## Wildcards in path {#wildcards-in-path}

`path` argument can specify multiple files using bash-like wildcards. For being processed file should exist and match to the whole path pattern. Listing of files is determined during `SELECT` (not at `CREATE` moment).

- `*` — Substitutes any number of any characters except `/` including empty string.
- `**` — Substitutes any number of any character include `/` including empty string.
- `?` — Substitutes any single character.
- `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`.
- `{N..M}` — Substitutes any number in range from N to M including both borders. N and M can have leading zeroes e.g. `000..078`.

Constructions with `{}` are similar to the [remote](../../../sql-reference/table-functions/remote.md) table function.

:::note
If the listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.
:::

**Example with wildcards 1**

Create table with files named `file-000.csv`, `file-001.csv`, ... , `file-999.csv`:

```sql
CREATE TABLE big_table (name String, value UInt32)
    ENGINE = S3('https://clickhouse-public-datasets.s3.amazonaws.com/my-bucket/my_folder/file-{000..999}.csv', 'CSV');
```

**Example with wildcards 2**

Suppose we have several files in CSV format with the following URIs on S3:

- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-bucket/some_folder/some_file_1.csv'
- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-bucket/some_folder/some_file_2.csv'
- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-bucket/some_folder/some_file_3.csv'
- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-bucket/another_folder/some_file_1.csv'
- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-bucket/another_folder/some_file_2.csv'
- 'https://clickhouse-public-datasets.s3.amazonaws.com/my-bucket/another_folder/some_file_3.csv'

There are several ways to make a table consisting of all six files:

1. Specify the range of file postfixes:

```sql
CREATE TABLE table_with_range (name String, value UInt32)
    ENGINE = S3('https://clickhouse-public-datasets.s3.amazonaws.com/my-bucket/{some,another}_folder/some_file_{1..3}', 'CSV');
```

2. Take all files with `some_file_` prefix (there should be no extra files with such prefix in both folders):

```sql
CREATE TABLE table_with_question_mark (name String, value UInt32)
    ENGINE = S3('https://clickhouse-public-datasets.s3.amazonaws.com/my-bucket/{some,another}_folder/some_file_?', 'CSV');
```

3. Take all the files in both folders (all files should satisfy format and schema described in query):

```sql
CREATE TABLE table_with_asterisk (name String, value UInt32)
    ENGINE = S3('https://clickhouse-public-datasets.s3.amazonaws.com/my-bucket/{some,another}_folder/*', 'CSV');
```

## Storage settings {#storage-settings}

- [s3_truncate_on_insert](/operations/settings/settings.md#s3_truncate_on_insert) - allows to truncate file before insert into it. Disabled by default.
- [s3_create_new_file_on_insert](/operations/settings/settings.md#s3_create_new_file_on_insert) - allows to create a new file on each insert if format has suffix. Disabled by default.
- [s3_skip_empty_files](/operations/settings/settings.md#s3_skip_empty_files) - allows to skip empty files while reading. Enabled by default.

## S3-related settings {#settings}

The following settings can be set before query execution or placed into configuration file.

- `s3_max_single_part_upload_size` — The maximum size of object to upload using singlepart upload to S3. Default value is `32Mb`.
- `s3_min_upload_part_size` — The minimum size of part to upload during multipart upload to [S3 Multipart upload](https://docs.aws.amazon.com/AmazonS3/latest/dev/uploadobjusingmpu.html). Default value is `16Mb`.
- `s3_max_redirects` — Max number of S3 redirects hops allowed. Default value is `10`.
- `s3_single_read_retries` — The maximum number of attempts during single read. Default value is `4`.
- `s3_max_put_rps` — Maximum PUT requests per second rate before throttling. Default value is `0` (unlimited).
- `s3_max_put_burst` — Max number of requests that can be issued simultaneously before hitting request per second limit. By default (`0` value) equals to `s3_max_put_rps`.
- `s3_max_get_rps` — Maximum GET requests per second rate before throttling. Default value is `0` (unlimited).
- `s3_max_get_burst` — Max number of requests that can be issued simultaneously before hitting request per second limit. By default (`0` value) equals to `s3_max_get_rps`.
- `s3_upload_part_size_multiply_factor` - Multiply `s3_min_upload_part_size` by this factor each time `s3_multiply_parts_count_threshold` parts were uploaded from a single write to S3. Default values is `2`.
- `s3_upload_part_size_multiply_parts_count_threshold` - Each time this number of parts was uploaded to S3, `s3_min_upload_part_size` is multiplied by `s3_upload_part_size_multiply_factor`. Default value is `500`.
- `s3_max_inflight_parts_for_one_file` - Limits the number of put requests that can be run concurrently for one object. Its number should be limited. The value `0` means unlimited. Default value is `20`. Each in-flight part has a buffer with size `s3_min_upload_part_size` for the first `s3_upload_part_size_multiply_factor` parts and more when file is big enough, see `upload_part_size_multiply_factor`. With default settings one uploaded file consumes not more than `320Mb` for a file which is less than `8G`. The consumption is greater for a larger file.

Security consideration: if malicious user can specify arbitrary S3 URLs, `s3_max_redirects` must be set to zero to avoid [SSRF](https://en.wikipedia.org/wiki/Server-side_request_forgery) attacks; or alternatively, `remote_host_filter` must be specified in server configuration.

## Endpoint-based settings {#endpoint-settings}

The following settings can be specified in configuration file for given endpoint (which will be matched by exact prefix of a URL):

- `endpoint` — Specifies prefix of an endpoint. Mandatory.
- `access_key_id` and `secret_access_key` — Specifies credentials to use with given endpoint. Optional.
- `use_environment_credentials` — If set to `true`, S3 client will try to obtain credentials from environment variables and [Amazon EC2](https://en.wikipedia.org/wiki/Amazon_Elastic_Compute_Cloud) metadata for given endpoint. Optional, default value is `false`.
- `region` — Specifies S3 region name. Optional.
- `use_insecure_imds_request` — If set to `true`, S3 client will use insecure IMDS request while obtaining credentials from Amazon EC2 metadata. Optional, default value is `false`.
- `expiration_window_seconds` — Grace period for checking if expiration-based credentials have expired. Optional, default value is `120`.
- `no_sign_request` - Ignore all the credentials so requests are not signed. Useful for accessing public buckets.
- `header` —  Adds specified HTTP header to a request to given endpoint. Optional, can be specified multiple times.
- `access_header` - Adds specified HTTP header to a request to given endpoint, in cases where there are no other credentials from another source.
- `server_side_encryption_customer_key_base64` — If specified, required headers for accessing S3 objects with SSE-C encryption will be set. Optional.
- `server_side_encryption_kms_key_id` - If specified, required headers for accessing S3 objects with [SSE-KMS encryption](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html) will be set. If an empty string is specified, the AWS managed S3 key will be used. Optional.
- `server_side_encryption_kms_encryption_context` - If specified alongside `server_side_encryption_kms_key_id`, the given encryption context header for SSE-KMS will be set. Optional.
- `server_side_encryption_kms_bucket_key_enabled` - If specified alongside `server_side_encryption_kms_key_id`, the header to enable S3 bucket keys for SSE-KMS will be set. Optional, can be `true` or `false`, defaults to nothing (matches the bucket-level setting).
- `max_single_read_retries` — The maximum number of attempts during single read. Default value is `4`. Optional.
- `max_put_rps`, `max_put_burst`, `max_get_rps` and `max_get_burst` - Throttling settings (see description above) to use for specific endpoint instead of per query. Optional.

**Example:**

```xml
<s3>
    <endpoint-name>
        <endpoint>https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/</endpoint>
        <!-- <access_key_id>ACCESS_KEY_ID</access_key_id> -->
        <!-- <secret_access_key>SECRET_ACCESS_KEY</secret_access_key> -->
        <!-- <region>us-west-1</region> -->
        <!-- <use_environment_credentials>false</use_environment_credentials> -->
        <!-- <use_insecure_imds_request>false</use_insecure_imds_request> -->
        <!-- <expiration_window_seconds>120</expiration_window_seconds> -->
        <!-- <no_sign_request>false</no_sign_request> -->
        <!-- <header>Authorization: Bearer SOME-TOKEN</header> -->
        <!-- <server_side_encryption_customer_key_base64>BASE64-ENCODED-KEY</server_side_encryption_customer_key_base64> -->
        <!-- <server_side_encryption_kms_key_id>KMS_KEY_ID</server_side_encryption_kms_key_id> -->
        <!-- <server_side_encryption_kms_encryption_context>KMS_ENCRYPTION_CONTEXT</server_side_encryption_kms_encryption_context> -->
        <!-- <server_side_encryption_kms_bucket_key_enabled>true</server_side_encryption_kms_bucket_key_enabled> -->
        <!-- <max_single_read_retries>4</max_single_read_retries> -->
    </endpoint-name>
</s3>
```

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

:::note
ClickHouse supports three archive formats:
ZIP
TAR
7Z
While ZIP and TAR archives can be accessed from any supported storage location, 7Z archives can only be read from the local filesystem where ClickHouse is installed.
:::

## Accessing public buckets {#accessing-public-buckets}

ClickHouse tries to fetch credentials from many different types of sources.
Sometimes, it can produce problems when accessing some buckets that are public causing the client to return `403` error code.
This issue can be avoided by using `NOSIGN` keyword, forcing the client to ignore all the credentials, and not sign the requests.

```sql
CREATE TABLE big_table (name String, value UInt32)
    ENGINE = S3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/aapl_stock.csv', NOSIGN, 'CSVWithNames');
```

## Optimizing performance {#optimizing-performance}

For details on optimizing the performance of the s3 function see [our detailed guide](/integrations/s3/performance).

## Role-based access {#role-based-access}

In ClickHouse Cloud, you can use role-based access to authenticate with S3 instead of using access keys. See [Secure S3](/cloud/data-sources/secure-s3) for configuration steps.

Once configured, a `roleARN` can be passed via an `extra_credentials` parameter:

```sql
CREATE TABLE my_s3_table(name String, value UInt32)
ENGINE = S3('https://my-bucket.s3.amazonaws.com/data/*.csv', extra_credentials(role_arn = 'arn:aws:iam::111111111111:role/ClickHouseAccessRole-001'), 'CSV')
```

## See also {#see-also}

- [s3 table function](../../../sql-reference/table-functions/s3.md)
- [Integrating S3 with ClickHouse](/integrations/s3)
)DOCS_MD";
    }
    else
    {
        String provider;
        if (name == COSNDefinition::storage_engine_name)
            provider = "[Tencent Cloud Object Storage (COS)](https://www.tencentcloud.com/products/cos)";
        else if (name == OSSDefinition::storage_engine_name)
            provider = "[Alibaba Cloud Object Storage Service (OSS)](https://www.alibabacloud.com/product/object-storage-service)";
        else if (name == GCSDefinition::storage_engine_name)
            provider = "[Google Cloud Storage (GCS)](https://cloud.google.com/storage)";

        description = "The `" + name + "` engine provides integration with " + provider
            + " through its Amazon S3-compatible API. It shares the implementation, parameters, and settings of the "
              "[`S3`](/engines/table-engines/integrations/s3) table engine; see that engine's documentation for the "
              "full description, parameters, examples, and settings.\n";
    }

    factory.registerStorage(name, [=](const StorageFactory::Arguments & args)
    {
        auto configuration = std::make_shared<StorageS3Configuration>();
        return createStorageObjectStorage(args, configuration);
    },
    {
        .supports_settings = true,
        .supports_sort_order = true, // for partition by
        .supports_schema_inference = true,
        .source_access_type = AccessTypeObjects::Source::S3,
        .has_builtin_setting_fn = StorageObjectStorageSettings::hasBuiltin,
    },
    Documentation{
        .description = description,
        .syntax = "ENGINE = " + name + "(url [, NOSIGN | access_key_id, secret_access_key [, session_token]] [, format] [, compression])",
        .related = {"AzureBlobStorage", "HDFS"}});
}

static void registerStorageS3(StorageFactory & factory)
{
    registerStorageS3Impl(S3Definition::storage_engine_name, factory);
}

static void registerStorageCOS(StorageFactory & factory)
{
    registerStorageS3Impl(COSNDefinition::storage_engine_name, factory);
}

static void registerStorageOSS(StorageFactory & factory)
{
    registerStorageS3Impl(OSSDefinition::storage_engine_name, factory);
}

static void registerStorageGCS(StorageFactory & factory)
{
    registerStorageS3Impl(GCSDefinition::storage_engine_name, factory);
}

#endif

#if USE_HDFS
static void registerStorageHDFS(StorageFactory & factory)
{
    factory.registerStorage(HDFSDefinition::storage_engine_name, [=](const StorageFactory::Arguments & args)
    {
        auto configuration = std::make_shared<StorageHDFSConfiguration>();
        return createStorageObjectStorage(args, configuration);
    },
    {
        .supports_settings = true,
        .supports_sort_order = true, // for partition by
        .supports_schema_inference = true,
        .source_access_type = AccessTypeObjects::Source::HDFS,
        .has_builtin_setting_fn = StorageObjectStorageSettings::hasBuiltin,
    },
    Documentation{
        .description = R"DOCS_MD(
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# HDFS table engine

<CloudNotSupportedBadge/>

This engine provides integration with the [Apache Hadoop](https://en.wikipedia.org/wiki/Apache_Hadoop) ecosystem by allowing to manage data on [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html) via ClickHouse. This engine is similar to the [File](/engines/table-engines/special/file) and [URL](/engines/table-engines/special/url) engines, but provides Hadoop-specific features.

This feature is not supported by ClickHouse engineers, and it is known to have a sketchy quality. In case of any problems, fix them yourself and submit a pull request.

## Usage {#usage}

```sql
ENGINE = HDFS(URI, format)
```

**Engine Parameters**

- `URI` - whole file URI in HDFS. The path part of `URI` may contain globs. In this case the table would be readonly.
- `format` - specifies one of the available file formats. To perform
`SELECT` queries, the format must be supported for input, and to perform
`INSERT` queries – for output. The available formats are listed in the
[Formats](/sql-reference/formats#formats-overview) section.
- [PARTITION BY expr]

### PARTITION BY {#partition-by}

`PARTITION BY` — Optional. In most cases you don't need a partition key, and if it is needed you generally don't need a partition key more granular than by month. Partitioning does not speed up queries (in contrast to the ORDER BY expression). You should never use too granular partitioning. Don't partition your data by client identifiers or names (instead, make client identifier or name the first column in the ORDER BY expression).

For partitioning by month, use the `toYYYYMM(date_column)` expression, where `date_column` is a column with a date of the type [Date](/sql-reference/data-types/date.md). The partition names here have the `"YYYYMM"` format.

**Example:**

**1.** Set up the `hdfs_engine_table` table:

```sql
CREATE TABLE hdfs_engine_table (name String, value UInt32) ENGINE=HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')
```

**2.** Fill file:

```sql
INSERT INTO hdfs_engine_table VALUES ('one', 1), ('two', 2), ('three', 3)
```

**3.** Query the data:

```sql
SELECT * FROM hdfs_engine_table LIMIT 2
```

```text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## Implementation details {#implementation-details}

- Reads and writes can be parallel.
- Not supported:
  - `ALTER` and `SELECT...SAMPLE` operations.
  - Indexes.
  - [Zero-copy](../../../operations/storing-data.md#zero-copy) replication is possible, but not recommended.

:::note Zero-copy replication is not ready for production
Zero-copy replication is disabled by default in ClickHouse version 22.8 and higher.  This feature is not recommended for production use.
:::

**Globs in path**

Multiple path components can have globs. For being processed file should exists and matches to the whole path pattern. Listing of files determines during `SELECT` (not at `CREATE` moment).

- `*` — Substitutes any number of any characters except `/` including empty string.
- `?` — Substitutes any single character.
- `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`.
- `{N..M}` — Substitutes any number in range from N to M including both borders.

Constructions with `{}` are similar to the [remote](../../../sql-reference/table-functions/remote.md) table function.

**Example**

1.  Suppose we have several files in TSV format with the following URIs on HDFS:

    - 'hdfs://hdfs1:9000/some_dir/some_file_1'
    - 'hdfs://hdfs1:9000/some_dir/some_file_2'
    - 'hdfs://hdfs1:9000/some_dir/some_file_3'
    - 'hdfs://hdfs1:9000/another_dir/some_file_1'
    - 'hdfs://hdfs1:9000/another_dir/some_file_2'
    - 'hdfs://hdfs1:9000/another_dir/some_file_3'

1.  There are several ways to make a table consisting of all six files:

<!-- -->

```sql
CREATE TABLE table_with_range (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV')
```

Another way:

```sql
CREATE TABLE table_with_question_mark (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_?', 'TSV')
```

Table consists of all the files in both directories (all files should satisfy format and schema described in query):

```sql
CREATE TABLE table_with_asterisk (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV')
```

:::note
If the listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.
:::

**Example**

Create table with files named `file000`, `file001`, ... , `file999`:

```sql
CREATE TABLE big_table (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV')
```
## Configuration {#configuration}

Similar to GraphiteMergeTree, the HDFS engine supports extended configuration using the ClickHouse config file. There are two configuration keys that you can use: global (`hdfs`) and user-level (`hdfs_*`). The global configuration is applied first, and then the user-level configuration is applied (if it exists).

```xml
<!-- Global configuration options for HDFS engine type -->
<hdfs>
<hadoop_kerberos_keytab>/tmp/keytab/clickhouse.keytab</hadoop_kerberos_keytab>
<hadoop_kerberos_principal>clickuser@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
<hadoop_security_authentication>kerberos</hadoop_security_authentication>
</hdfs>

<!-- Configuration specific for user "root" -->
<hdfs_root>
<hadoop_kerberos_principal>root@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
</hdfs_root>
```

### Configuration options {#configuration-options}

#### Supported by libhdfs3 {#supported-by-libhdfs3}

| **parameter**                                         | **default value**       |
| -                                                  | -                    |
| rpc\_client\_connect\_tcpnodelay                      | true                    |
| dfs\_client\_read\_shortcircuit                       | true                    |
| output\_replace-datanode-on-failure                   | true                    |
| input\_notretry-another-node                          | false                   |
| input\_localread\_mappedfile                          | true                    |
| dfs\_client\_use\_legacy\_blockreader\_local          | false                   |
| rpc\_client\_ping\_interval                           | 10  * 1000              |
| rpc\_client\_connect\_timeout                         | 600 * 1000              |
| rpc\_client\_read\_timeout                            | 3600 * 1000             |
| rpc\_client\_write\_timeout                           | 3600 * 1000             |
| rpc\_client\_socket\_linger\_timeout                  | -1                      |
| rpc\_client\_connect\_retry                           | 10                      |
| rpc\_client\_timeout                                  | 3600 * 1000             |
| dfs\_default\_replica                                 | 3                       |
| input\_connect\_timeout                               | 600 * 1000              |
| input\_read\_timeout                                  | 3600 * 1000             |
| input\_write\_timeout                                 | 3600 * 1000             |
| input\_localread\_default\_buffersize                 | 1 * 1024 * 1024         |
| dfs\_prefetchsize                                     | 10                      |
| input\_read\_getblockinfo\_retry                      | 3                       |
| input\_localread\_blockinfo\_cachesize                | 1000                    |
| input\_read\_max\_retry                               | 60                      |
| output\_default\_chunksize                            | 512                     |
| output\_default\_packetsize                           | 64 * 1024               |
| output\_default\_write\_retry                         | 10                      |
| output\_connect\_timeout                              | 600 * 1000              |
| output\_read\_timeout                                 | 3600 * 1000             |
| output\_write\_timeout                                | 3600 * 1000             |
| output\_close\_timeout                                | 3600 * 1000             |
| output\_packetpool\_size                              | 1024                    |
| output\_heartbeat\_interval                          | 10 * 1000               |
| dfs\_client\_failover\_max\_attempts                  | 15                      |
| dfs\_client\_read\_shortcircuit\_streams\_cache\_size | 256                     |
| dfs\_client\_socketcache\_expiryMsec                  | 3000                    |
| dfs\_client\_socketcache\_capacity                    | 16                      |
| dfs\_default\_blocksize                               | 64 * 1024 * 1024        |
| dfs\_default\_uri                                     | "hdfs://localhost:9000" |
| hadoop\_security\_authentication                      | "simple"                |
| hadoop\_security\_kerberos\_ticket\_cache\_path       | ""                      |
| dfs\_client\_log\_severity                            | "INFO"                  |
| dfs\_domain\_socket\_path                             | ""                      |

[HDFS Configuration Reference](https://hawq.apache.org/docs/userguide/2.3.0.0-incubating/reference/HDFSConfigurationParameterReference.html) might explain some parameters.

#### ClickHouse extras {#clickhouse-extras}

| **parameter**                                         | **default value**       |
| -                                                  | -                    |
|hadoop\_kerberos\_keytab                               | ""                      |
|hadoop\_kerberos\_principal                            | ""                      |
|libhdfs3\_conf                                         | ""                      |

### Limitations {#limitations}
* `hadoop_security_kerberos_ticket_cache_path` and `libhdfs3_conf` can be global only, not user specific

## Kerberos support {#kerberos-support}

If the `hadoop_security_authentication` parameter has the value `kerberos`, ClickHouse authenticates via Kerberos.
Parameters are [here](#clickhouse-extras) and `hadoop_security_kerberos_ticket_cache_path` may be of help.
Note that due to libhdfs3 limitations only old-fashioned approach is supported,
datanode communications are not secured by SASL (`HADOOP_SECURE_DN_USER` is a reliable indicator of such
security approach). Use `tests/integration/test_storage_kerberized_hdfs/hdfs_configs/bootstrap.sh` for reference.

If `hadoop_kerberos_keytab`, `hadoop_kerberos_principal` or `hadoop_security_kerberos_ticket_cache_path` are specified, Kerberos authentication will be used. `hadoop_kerberos_keytab` and `hadoop_kerberos_principal` are mandatory in this case.
## HDFS Namenode HA support {#namenode-ha}

libhdfs3 support HDFS namenode HA.

- Copy `hdfs-site.xml` from an HDFS node to `/etc/clickhouse-server/`.
- Add following piece to ClickHouse config file:

```xml
<hdfs>
    <libhdfs3_conf>/etc/clickhouse-server/hdfs-site.xml</libhdfs3_conf>
</hdfs>
```

- Then use `dfs.nameservices` tag value of `hdfs-site.xml` as the namenode address in the HDFS URI. For example, replace `hdfs://appadmin@192.168.101.11:8020/abc/` with `hdfs://appadmin@my_nameservice/abc/`.

## Virtual columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.

## Storage settings {#storage-settings}

- [hdfs_truncate_on_insert](/operations/settings/settings.md#hdfs_truncate_on_insert) - allows to truncate file before insert into it. Disabled by default.
- [hdfs_create_new_file_on_insert](/operations/settings/settings.md#hdfs_create_new_file_on_insert) - allows to create a new file on each insert if format has suffix. Disabled by default.
- [hdfs_skip_empty_files](/operations/settings/settings.md#hdfs_skip_empty_files) - allows to skip empty files while reading. Disabled by default.

**See Also**

- [Virtual columns](../../../engines/table-engines/index.md#table_engines-virtual_columns)
)DOCS_MD",
        .syntax = "ENGINE = HDFS(uri [, format] [, compression])",
        .related = {"S3", "AzureBlobStorage"}});
}
#endif

void registerStorageObjectStorage(StorageFactory & factory);
void registerStorageObjectStorage(StorageFactory & factory)
{
#if USE_AWS_S3
    registerStorageS3(factory);
    registerStorageCOS(factory);
    registerStorageOSS(factory);
    registerStorageGCS(factory);
#endif
#if USE_AZURE_BLOB_STORAGE
    registerStorageAzure(factory);
#endif
#if USE_HDFS
    registerStorageHDFS(factory);
#endif
    UNUSED(factory);
}

[[maybe_unused]] static DataLakeStorageSettingsPtr getDataLakeStorageSettings(const ASTStorage & storage_def)
{
    auto storage_settings = std::make_shared<DataLakeStorageSettings>();
    if (storage_def.settings)
        storage_settings->loadFromQuery(*storage_def.settings);
    return storage_settings;
}

#if USE_AVRO /// StorageIceberg depending on Avro to parse metadata with Avro format.

void registerStorageIceberg(StorageFactory & factory);
void registerStorageIceberg(StorageFactory & factory)
{
    factory.registerStorage(
        IcebergDefinition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            const auto disk_name = storage_settings && (*storage_settings)[DataLakeStorageSetting::disk].changed
                ? (*storage_settings)[DataLakeStorageSetting::disk].value
                : "";

            StorageObjectStorageConfigurationPtr configuration;
            if (!disk_name.empty())
            {
                auto disk = Context::getGlobalContextInstance()->getDisk(disk_name);
                switch (disk->getObjectStorage()->getType())
                {
#if USE_AWS_S3
                    case ObjectStorageType::S3:
                        configuration = std::make_shared<StorageS3IcebergConfiguration>(storage_settings);
                        break;
#endif
#if USE_AZURE_BLOB_STORAGE
                    case ObjectStorageType::Azure:
                        configuration = std::make_shared<StorageAzureIcebergConfiguration>(storage_settings);
                        break;
#endif
                    case ObjectStorageType::Local:
                        configuration = std::make_shared<StorageLocalIcebergConfiguration>(storage_settings);
                        break;
                    default:
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Unsupported disk type for {}: {}",
                            IcebergDefinition::storage_engine_name,
                            disk->getObjectStorage()->getType());
                }
            }
            else
#if USE_AWS_S3
                configuration = std::make_shared<StorageS3IcebergConfiguration>(storage_settings);
#endif
            if (configuration == nullptr)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "This storage configuration is not available at this build");
            }
            return createStorageObjectStorage(args, configuration);
        },
        {
            .supports_settings = true,
            .supports_sort_order = true,
            .supports_schema_inference = true,
            /// This source access type is probably a bug which was overlooked and we do not know how to fix it simply, so we keep it as it is.
            .source_access_type = AccessTypeObjects::Source::S3,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        },
        Documentation{
            .description = R"DOCS_MD(
:::warning
We recommend using the [Iceberg Table Function](/sql-reference/table-functions/iceberg.md) for working with Iceberg data in ClickHouse. The Iceberg Table Function currently provides sufficient functionality, offering a partial read-only interface for Iceberg tables.

The Iceberg Table Engine is available but may have limitations. ClickHouse wasn't originally designed to support tables with externally changing schemas, which can affect the functionality of the Iceberg Table Engine. As a result, some features that work with regular tables may be unavailable or may not function correctly, especially when using the old analyzer.

For optimal compatibility, we suggest using the Iceberg Table Function while we continue to improve support for the Iceberg Table Engine.
:::

This engine provides a read-only integration with existing Apache [Iceberg](https://iceberg.apache.org/) tables in Amazon S3, Azure, HDFS and locally stored tables.

## Create table {#create-table}

Note that the Iceberg table must already exist in the storage, this command does not take DDL parameters to create a new table.

```sql
CREATE TABLE iceberg_table_s3
    ENGINE = IcebergS3(url, [, NOSIGN | access_key_id, secret_access_key, [session_token]], format, [,compression], [,extra_credentials])

CREATE TABLE iceberg_table_azure
    ENGINE = IcebergAzure(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])

CREATE TABLE iceberg_table_hdfs
    ENGINE = IcebergHDFS(path_to_table, [,format] [,compression_method])

CREATE TABLE iceberg_table_local
    ENGINE = IcebergLocal(path_to_table, [,format] [,compression_method])
```

## Engine arguments {#engine-arguments}

Description of the arguments coincides with description of arguments in engines `S3`, `AzureBlobStorage`, `HDFS` and `File` correspondingly.
`format` stands for the format of data files in the Iceberg table.

For `IcebergS3`, an optional `extra_credentials` parameter can be used to pass a `role_arn` for role-based access in ClickHouse Cloud. See [Secure S3](/cloud/data-sources/secure-s3) for configuration steps.

Engine parameters can be specified using [Named Collections](../../../operations/named-collections.md)

### Example {#example}

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

Using named collections:

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3(iceberg_conf, filename = 'test_table')

```

## Aliases {#aliases}

The `Iceberg` table engine auto-detects the storage backend from the `disk` setting and dispatches to `IcebergS3`, `IcebergAzure`, or `IcebergLocal` accordingly. When no `disk` is specified, it defaults to the `IcebergS3` implementation.

## Data types {#data-types}

The following table shows how Iceberg data types are mapped to ClickHouse data types during schema inference (for reading purposes).

### Primitive types {#primitive-types}

| Iceberg type | ClickHouse type | Notes |
|---|---|---|
| `boolean` | `Bool` | |
| `int` | `Int32` | |
| `long`, `bigint` | `Int64` | |
| `float` | `Float32` | |
| `double` | `Float64` | |
| `date` | `Date32` | |
| `time` | `Int64` | Microseconds since midnight |
| `timestamp` | `DateTime64(6)` | Microseconds, no timezone |
| `timestamptz` | `DateTime64(6, 'UTC')` | Microseconds, UTC timezone |
| `timestamp_ns` | `DateTime64(9)` | Nanoseconds, no timezone (since Iceberg v3 only) |
| `timestamptz_ns` | `DateTime64(9, 'UTC')` | Nanoseconds, UTC timezone (since Iceberg v3 only) |
| `string`, `binary` | `String` | |
| `uuid` | `UUID` | |
| `fixed(N)` | `FixedString(N)` | |
| `decimal(P, S)` | `Decimal(P, S)` | |

### Complex types {#complex-types}

| Iceberg type | ClickHouse type |
|---|---|
| `list` | `Array` |
| `map` | `Map` |
| `struct` | `Tuple` |

## Schema evolution {#schema-evolution}
ClickHouse supports reading Iceberg tables whose schema has evolved over time. This includes tables where columns have been added, removed, or reordered, as well as columns changed from required to nullable. Additionally, the following type casts are supported:

* int -> long
* float -> double
* decimal(P, S) -> decimal(P', S) where P' > P.

Currently, it is not possible to change nested structures or the types of elements within arrays and maps.

To read a table where the schema has changed after its creation with dynamic schema inference, set allow_dynamic_metadata_for_data_lakes = true when creating the table.

## Partition pruning {#partition-pruning}

ClickHouse supports partition pruning during SELECT queries for Iceberg tables, which helps optimize query performance by skipping irrelevant data files. To enable partition pruning, set `use_iceberg_partition_pruning = 1`. For more information about iceberg partition pruning address https://iceberg.apache.org/spec/#partitioning

## Time travel {#time-travel}

ClickHouse supports time travel for Iceberg tables, allowing you to query historical data with a specific timestamp or snapshot ID.

## Processing of tables with deleted rows {#deleted-rows}

ClickHouse supports reading Iceberg tables that use the following deletion methods:

- [Position deletes](https://iceberg.apache.org/spec/#position-delete-files)
- [Equality deletes](https://iceberg.apache.org/spec/#equality-delete-files) (supported from version 25.8+)

The following deletion method is **not supported**:
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

- **Snapshots** are typically created when:
  - New data is written to the table
  - Some kind of data compaction is performed

- **Schema changes typically don't create snapshots** - This leads to important behaviors when using time travel with tables that have undergone schema evolution.

### Example scenarios {#example-scenarios}

All scenarios are written in Spark because CH doesn't support writing to Iceberg tables yet.

#### Scenario 1: Schema changes without new snapshots {#scenario-1}

Consider this sequence of operations:

```sql
-- Create a table with two columns
CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example (
order_number int,
product_code string
  )
USING iceberg
OPTIONS ('format-version'='2')

-- Insert data into the table
INSERT INTO spark_catalog.db.time_travel_example VALUES
    (1, 'Mars')

  ts1 = now() // A piece of pseudo code

-- Alter table to add a new column
ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)

ts2 = now()

-- Insert data into the table
INSERT INTO spark_catalog.db.time_travel_example VALUES (2, 'Venus', 100)

ts3 = now()

-- Query the table at each timestamp
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

- At ts1 & ts2: Only the original two columns appear
- At ts3: All three columns appear, with NULL for the price of the first row

#### Scenario 2: Historical vs. current schema differences {#scenario-2}

A time travel query at a current moment might show a different schema than the current table:

```sql
-- Create a table
CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_2 (
order_number int,
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

#### Scenario 3: Historical vs. current schema differences {#scenario-3}

The second one is that while doing time travel you can't get state of table before any data was written to it:

```sql
-- Create a table
CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_3 (
order_number int,
product_code string
  )
USING iceberg
OPTIONS ('format-version'='2');

ts = now();

-- Query the table at a specific timestamp
  SELECT * FROM spark_catalog.db.time_travel_example_3 TIMESTAMP AS OF ts; -- Finises with error: Cannot find a snapshot older than ts.
```

In Clickhouse the behavior is consistent with Spark. You can mentally replace Spark Select queries with Clickhouse Select queries and it will work the same way.

## Metadata file resolution {#metadata-file-resolution}
When using the `Iceberg` table engine in ClickHouse, the system needs to locate the correct metadata.json file that describes the Iceberg table structure. Here's how this resolution process works:

### Candidates search {#candidate-search}

1. **Direct Path Specification**:
* If you set `iceberg_metadata_file_path`, the system will use this exact path by combining it with the Iceberg table directory path.
* When this setting is provided, all other resolution settings are ignored.
2. **Table UUID Matching**:
* If `iceberg_metadata_table_uuid` is specified, the system will:
  * Look only at `.metadata.json` files in the `metadata` directory
  * Filter for files containing a `table-uuid` field matching your specified UUID (case-insensitive)

3. **Default Search**:
* If neither of the above settings are provided, all `.metadata.json` files in the `metadata` directory become candidates

### Selecting the most recent file {#most-recent-file}

After identifying candidate files using the above rules, the system determines which one is the most recent:

* If `iceberg_recent_metadata_file_by_last_updated_ms_field` is enabled:
  * The file with the largest `last-updated-ms` value is selected

* Otherwise:
  * The file with the highest version number is selected
  * (Version appears as `V` in filenames formatted as `V.metadata.json` or `V-uuid.metadata.json`)

**Note**: All mentioned settings (unless explicitly specified otherwise) are engine-level settings and must be specified during table creation as shown below:

```sql
CREATE TABLE example_table ENGINE = Iceberg(
    's3://bucket/path/to/iceberg_table'
) SETTINGS iceberg_metadata_table_uuid = '6f6f6407-c6a5-465f-a808-ea8900e35a38';
```

**Note**: While Iceberg Catalogs typically handle metadata resolution, the `Iceberg` table engine in ClickHouse directly interprets files stored in S3 as Iceberg tables, which is why understanding these resolution rules is important.

## Data cache {#data-cache}

`Iceberg` table engine and table function support data caching same as `S3`, `AzureBlobStorage`, `HDFS` storages. See [here](../../../engines/table-engines/integrations/s3.md#data-cache).

## Metadata cache {#metadata-cache}

`Iceberg` table engine and table function support metadata cache storing the information of manifest files, manifest list and metadata json. The cache is stored in memory. This feature is controlled by setting `use_iceberg_metadata_files_cache`, which is enabled by default.

## Asynchronous metadata prefetching {#async-metadata-prefetch}

Asynchronous metadata prefetching can be enabled at `Iceberg` table creation by setting `iceberg_metadata_async_prefetch_period_ms`. If set to 0 (default) or if metadata caching is not enabled, the asynchronous prefetching is disabled.
In order to enable this feature, a non-zero value of milliseconds should be given. It represents interval between prefetching cycles.

If enabled, the server will run a recurring background operation to list the remote catalog and to detect new metadata version. It will then parse it and recursively walk the snapshot, fetching active manifest list files and manifest files.
The files already available at the metadata cache, won't be downloaded again. At the end of each prefetching cycle, the latest metadata snapshot is available at the metadata cache.

```sql
CREATE TABLE example_table ENGINE = Iceberg(
    's3://bucket/path/to/iceberg_table'
) SETTINGS
    iceberg_metadata_async_prefetch_period_ms = 60000;
```

In order to make the most of asynchronous metadata prefetching at read operations, `iceberg_metadata_staleness_ms` parameter should be specified as Query or Session parameter. By default (0 - not specified) in the context of each query, the server will fetch latest metadata from the remote catalog.
By specifying tolerance to metadata staleness, the server is allowed to use the cached version of metadata snapshot without calling the remote catalog. If there's metadata version in cache, and it has been downloaded within the given window of staleness, it will be used to process the query.
Otherwise the latest version will be fetched from the remote catalog.

```sql
SELECT count() FROM icebench_table WHERE ...
SETTINGS iceberg_metadata_staleness_ms=120000
```

**Note**: Asynchronous metadata prefetching runs at `ICEBERG_SCEDULE_POOL`, which is server-side threadpool for background operations on active `Iceberg` tables. The size of this threadpool is controlled by `iceberg_background_schedule_pool_size` server configuration parameter (default is 10).

**Note**: Current expectation is that metadata cache size is sufficient to hold the latest metadata snapshot in full for all active tables, if asynchronous prefetching is enabled.

## See also {#see-also}

- [iceberg table function](/sql-reference/table-functions/iceberg.md)
)DOCS_MD",
            .syntax = "ENGINE = Iceberg(url [, access_key_id, secret_access_key])",
            .related = {"IcebergS3", "IcebergAzure", "IcebergHDFS", "IcebergLocal", "DeltaLake", "Hudi"}});
#if USE_AWS_S3
    factory.registerStorage(
        IcebergS3Definition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            const auto disk_name = storage_settings && (*storage_settings)[DataLakeStorageSetting::disk].changed
                ? (*storage_settings)[DataLakeStorageSetting::disk].value
                : "";

            StorageObjectStorageConfigurationPtr configuration;
            if (!disk_name.empty())
            {
                auto disk = Context::getGlobalContextInstance()->getDisk(disk_name);
                switch (disk->getObjectStorage()->getType())
                {
                case ObjectStorageType::S3:
                    configuration = std::make_shared<StorageS3IcebergConfiguration>(storage_settings);
                    break;
                default:
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported disk type for {}: {}", IcebergS3Definition::storage_engine_name, disk->getObjectStorage()->getType());
                }
            }
            else
                configuration = std::make_shared<StorageS3IcebergConfiguration>(storage_settings);
            return createStorageObjectStorage(args, configuration);
        },
        {
            .supports_settings = true,
            .supports_sort_order = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::S3,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        },
        Documentation{
            .description = "Provides a read-only integration with existing Apache Iceberg tables stored in Amazon S3 or S3-compatible object storage.",
            .syntax = "ENGINE = IcebergS3(url [, access_key_id, secret_access_key])",
            .related = {"Iceberg"}});
#    endif
#    if USE_AZURE_BLOB_STORAGE
    factory.registerStorage(
        IcebergAzureDefinition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            const auto disk_name = storage_settings && (*storage_settings)[DataLakeStorageSetting::disk].changed
                ? (*storage_settings)[DataLakeStorageSetting::disk].value
                : "";

            StorageObjectStorageConfigurationPtr configuration;
            if (!disk_name.empty())
            {
                auto disk = Context::getGlobalContextInstance()->getDisk(disk_name);
                switch (disk->getObjectStorage()->getType())
                {
                case ObjectStorageType::Azure:
                    configuration = std::make_shared<StorageAzureIcebergConfiguration>(storage_settings);
                    break;
                default:
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported disk type for {}: {}", IcebergAzureDefinition::storage_engine_name, disk->getObjectStorage()->getType());
                }
            }
            else
                configuration = std::make_shared<StorageAzureIcebergConfiguration>(storage_settings);
            return createStorageObjectStorage(args, configuration);
        },
        {
            .supports_settings = true,
            .supports_sort_order = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::AZURE,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        },
        Documentation{
            .description = "Provides a read-only integration with existing Apache Iceberg tables stored in Microsoft Azure Blob Storage.",
            .syntax = "ENGINE = IcebergAzure(connection_string | storage_account_url, container_name, blobpath)",
            .related = {"Iceberg"}});
#    endif
#    if USE_HDFS
    factory.registerStorage(
        IcebergHDFSDefinition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            auto configuration = std::make_shared<StorageHDFSIcebergConfiguration>(storage_settings);
            return createStorageObjectStorage(args, configuration);
        },
        {
            .supports_settings = true,
            .supports_sort_order = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::HDFS,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        },
        Documentation{
            .description = "Provides a read-only integration with existing Apache Iceberg tables stored in HDFS.",
            .syntax = "ENGINE = IcebergHDFS(uri)",
            .related = {"Iceberg"}});
#    endif
    factory.registerStorage(
        IcebergLocalDefinition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            const auto disk_name = storage_settings && (*storage_settings)[DataLakeStorageSetting::disk].changed
                ? (*storage_settings)[DataLakeStorageSetting::disk].value
                : "";

            StorageObjectStorageConfigurationPtr configuration;
            if (!disk_name.empty())
            {
                auto disk = Context::getGlobalContextInstance()->getDisk(disk_name);
                switch (disk->getObjectStorage()->getType())
                {
                    case ObjectStorageType::Local:
                        configuration = std::make_shared<StorageLocalIcebergConfiguration>(storage_settings);
                        break;
                    default:
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported disk type for {}: {}", IcebergLocalDefinition::storage_engine_name, disk->getObjectStorage()->getType());
                }
            }
            else
                configuration = std::make_shared<StorageLocalIcebergConfiguration>(storage_settings);
            return createStorageObjectStorage(args, configuration);
        },
        {
            .supports_settings = true,
            .supports_sort_order = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::FILE,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        },
        Documentation{
            .description = "Provides a read-only integration with existing Apache Iceberg tables stored on the local filesystem.",
            .syntax = "ENGINE = IcebergLocal(path)",
            .related = {"Iceberg"}});
}

void registerStoragePaimon(StorageFactory & factory);
void registerStoragePaimon(StorageFactory & factory)
{
    auto check_paimon_storage_engine_enabled = [](const StorageFactory::Arguments & args)
    {
        if (args.mode <= LoadingStrictnessLevel::CREATE
            && !args.getLocalContext()->getSettingsRef()[Setting::allow_experimental_paimon_storage_engine])
        {
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Paimon table engines are experimental. Set `allow_experimental_paimon_storage_engine` setting to enable them");
        }
    };

    /// Register default Paimon engine (auto-detect storage type based on disk)
    factory.registerStorage(
        PaimonDefinition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            check_paimon_storage_engine_enabled(args);
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            const auto disk_name = storage_settings && (*storage_settings)[DataLakeStorageSetting::disk].changed
                ? (*storage_settings)[DataLakeStorageSetting::disk].value
                : "";

            StorageObjectStorageConfigurationPtr configuration;
            if (!disk_name.empty())
            {
                auto disk = Context::getGlobalContextInstance()->getDisk(disk_name);
                switch (disk->getObjectStorage()->getType())
                {
#if USE_AWS_S3
                    case ObjectStorageType::S3:
                        configuration = std::make_shared<StorageS3PaimonConfiguration>(storage_settings);
                        break;
#endif
#if USE_AZURE_BLOB_STORAGE
                    case ObjectStorageType::Azure:
                        configuration = std::make_shared<StorageAzurePaimonConfiguration>(storage_settings);
                        break;
#endif
                    case ObjectStorageType::Local:
                        configuration = std::make_shared<StorageLocalPaimonConfiguration>(storage_settings);
                        break;
                    default:
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Unsupported disk type for {}: {}",
                            PaimonDefinition::storage_engine_name,
                            disk->getObjectStorage()->getType());
                }
            }
            else
            {
#if USE_AWS_S3
                configuration = std::make_shared<StorageS3PaimonConfiguration>(storage_settings);
#else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "S3 support is not available for Paimon in this build");
#endif
            }
            if (configuration == nullptr)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "This storage configuration is not available at this build");
            }
            expandPaimonKeeperMacrosIfNeeded(args, storage_settings);
            return createStorageObjectStorage(args, configuration);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            /// source_access_type is hardcoded to S3 even though this auto-detect
            /// engine can resolve to Azure / Local backends at runtime.  This is a
            /// known limitation shared by Iceberg (see its auto-detect registration
            /// comment) and DeltaLake auto-detect registrations: the registration
            /// struct requires a static value, but the actual backend is determined
            /// inside the factory lambda.  Fixing this requires refactoring the
            /// StorageFactory registration mechanism to support dynamic / deferred
            /// access-type resolution.
            .source_access_type = AccessTypeObjects::Source::S3,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        },
        Documentation{
            .description = R"DOCS_MD(
This engine provides a read-only integration with existing Apache [Paimon](https://paimon.apache.org/) tables in Amazon S3, Azure, HDFS and locally stored tables.
It supports snapshot reads, incremental reads, and basic partition pruning provided by the engine.

## Create table {#create-table}

Note that the Paimon table must already exist in the storage, this command does not take DDL parameters to create a new table.
Creating `Paimon*` tables is gated by `allow_experimental_paimon_storage_engine` (disabled by default), so enable it before running `CREATE TABLE`.

```sql
SET allow_experimental_paimon_storage_engine = 1;

CREATE TABLE paimon_table_s3
    ENGINE = PaimonS3(url, [, access_key_id, secret_access_key] [,format] [,structure] [,compression])

CREATE TABLE paimon_table_azure
    ENGINE = PaimonAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

CREATE TABLE paimon_table_hdfs
    ENGINE = PaimonHDFS(path_to_table, [,format] [,compression_method])

CREATE TABLE paimon_table_local
    ENGINE = PaimonLocal(path_to_table, [,format] [,compression_method])
```

## Engine arguments {#engine-arguments}

Description of the arguments coincides with description of arguments in engines `S3`, `AzureBlobStorage`, `HDFS` and `File` correspondingly.
`format` stands for the format of data files in the Paimon table.

Engine parameters can be specified using [Named Collections](../../../operations/named-collections.md)

### Example {#example}

```sql
CREATE TABLE paimon_table ENGINE=PaimonS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

Using named collections:

```xml
<clickhouse>
    <named_collections>
        <paimon_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </paimon_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE paimon_table ENGINE=PaimonS3(paimon_conf, filename = 'test_table')
```

## Capabilities {#capabilities}

- Snapshot reads from the latest table snapshot.
- Incremental reads based on committed snapshot id when enabled.
- Partition pruning when `use_paimon_partition_pruning` is enabled.
- Optional background refresh of metadata when configured.
- Stable table UUID when using Atomic/Replicated databases, enabling `{uuid}` macros in Keeper paths.

## Settings {#settings}

This engine uses the same settings as the corresponding object storage engines and adds Paimon-specific settings:

- `allow_experimental_paimon_storage_engine` — enables creation of `Paimon`, `PaimonS3`, `PaimonAzure`, `PaimonHDFS`, and `PaimonLocal` table engines. Default: `0` (disabled).
- `paimon_incremental_read` — enable incremental read mode.
- `paimon_metadata_refresh_interval_sec` — background metadata refresh interval in seconds. When set to a value greater than 0, a background task periodically pulls the latest snapshot and schema from object storage. Default: 30.
- `paimon_keeper_path` — Keeper path for incremental read state. Must be set and unique per table; supports macros such as `{database}`, `{table}`, `{uuid}`.
- `paimon_replica_name` — Replica name for incremental read state. Must be set and unique per replica; supports macros such as `{replica}`.

## Incremental read examples {#incremental-read-examples}

Incremental read with Keeper state:

```sql
CREATE TABLE paimon_inc
ENGINE = PaimonS3(paimon_conf, filename = 'paimon_all_types')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/{database}/{uuid}',
    paimon_replica_name = '{replica}';
```

### Query-level settings for incremental read {#query-level-settings-for-incremental-read}

The following settings are **query-level** (passed via `SELECT ... SETTINGS`, not in `CREATE TABLE`). They control per-query behavior of incremental reads:

- `paimon_target_snapshot_id` — read only the delta of the specified snapshot. The committed watermark in Keeper is **not** advanced, so the same snapshot can be re-read any number of times. Default: `-1` (disabled).
- `max_consume_snapshots` — maximum number of snapshots to consume in a single incremental read. When the source has accumulated many unread snapshots, this limits how many are consumed per query to control batch size. `0` means no limit. Default: `0`.

**Targeted snapshot read** — always returns the delta of snapshot 1, regardless of the current watermark:

```sql
SELECT count()
FROM paimon_inc
SETTINGS paimon_target_snapshot_id = 1;
```

**Limiting snapshots per batch** — if three new snapshots are pending, consume at most two per query:

```sql
SELECT count()
FROM paimon_inc
SETTINGS max_consume_snapshots = 2;
```

## Paimon to MergeTree via Refreshable Materialized View {#paimon-to-mergetree-via-refresh-mv}

You can build an end-to-end pipeline that continuously syncs data from a Paimon table into a MergeTree table using a refreshable Materialized View in `APPEND` mode. Each refresh cycle reads only new incremental data from Paimon and appends it to the destination table.

**Step 1 — Create the Paimon source table with incremental read and metadata refresh enabled.**

The example below uses `PaimonLocal`. Replace the engine with `PaimonS3`, `PaimonAzure`, `PaimonHDFS`, or the `Paimon` alias as appropriate for your storage backend:

```sql
SET allow_experimental_paimon_storage_engine = 1;

-- Local storage
CREATE TABLE paimon_mv_source
ENGINE = PaimonLocal('/path/to/paimon/table')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/tables/{uuid}',
    paimon_replica_name = '{replica}',
    paimon_metadata_refresh_interval_sec = 1;

-- S3 storage (Paimon is an alias for PaimonS3)
CREATE TABLE paimon_mv_source
ENGINE = Paimon('http://minio:9000/bucket/path/to/table', 'access_key', 'secret_key')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/tables/{uuid}',
    paimon_replica_name = '{replica}',
    paimon_metadata_refresh_interval_sec = 1;
```

`paimon_metadata_refresh_interval_sec` sets the background metadata refresh interval in seconds. When greater than 0, a background task periodically pulls the latest snapshot and schema from object storage, so that the MV refresh cycle can see newly committed data without waiting for a query to trigger the metadata update. Default is 30. Use cautiously on many tables to avoid excessive object storage and Keeper I/O.

**Step 2 — Create the MergeTree destination table (schema cloned from the Paimon table):**

```sql
CREATE TABLE paimon_mv_dest AS paimon_mv_source
ENGINE = MergeTree()
ORDER BY tuple();
```

**Step 3 — Create the refreshable Materialized View:**

```sql
CREATE MATERIALIZED VIEW paimon_mv
REFRESH EVERY 10 SECOND
APPEND
TO paimon_mv_dest
AS SELECT * FROM paimon_mv_source;
```

Every 10 seconds the MV fires a `SELECT * FROM paimon_mv_source`, which returns only the rows added since the last committed snapshot, and appends them to `paimon_mv_dest`.

**Cleanup:**

```sql
SYSTEM STOP VIEW paimon_mv;
DROP VIEW IF EXISTS paimon_mv SYNC;
DROP TABLE IF EXISTS paimon_mv_dest SYNC;
DROP TABLE IF EXISTS paimon_mv_source SYNC;
```

:::note
Stop the MV before dropping it to prevent background refresh from blocking DDL operations.
:::

## Limitations {#limitations}

- Incremental read requires Keeper (ZooKeeper) to be configured.
- Incremental read requires `paimon_keeper_path` to be set and unique per table.
- `paimon_replica_name` must be unique per replica within the same Keeper path.
- Incremental read uses at-most-once delivery: the committed snapshot is advanced when data files are collected, before the data is actually consumed. If the query fails after file collection, the skipped snapshots will not be re-read on retry.
- The table engine is read-only; data modification is not supported.
- Incremental read does not handle historical data deletions from the Paimon source. If upstream Paimon data is deleted or updated, the corresponding rows already written to a ClickHouse MergeTree destination table will not be automatically removed. You must manually issue `ALTER TABLE ... DELETE` on the MergeTree table to clean up stale data.

## Aliases {#aliases}

The `Paimon` table engine auto-detects the storage backend from the `disk` setting and dispatches to `PaimonS3`, `PaimonAzure`, or `PaimonLocal` accordingly. When no `disk` is specified, it defaults to the `PaimonS3` implementation.

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_etag` — The etag of the file. Type: `LowCardinality(String)`. If the etag is unknown, the value is `NULL`.

## Data Types supported {#data-types-supported}

| Paimon Data Type | ClickHouse Data Type |
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
)DOCS_MD",
            .syntax = "ENGINE = Paimon(url [, access_key_id, secret_access_key])",
            .related = {"PaimonS3", "PaimonAzure", "PaimonHDFS", "PaimonLocal", "Iceberg", "DeltaLake"}});

#if USE_AWS_S3
    /// Register PaimonS3 engine
    factory.registerStorage(
        PaimonS3Definition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            check_paimon_storage_engine_enabled(args);
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            const auto disk_name = storage_settings && (*storage_settings)[DataLakeStorageSetting::disk].changed
                ? (*storage_settings)[DataLakeStorageSetting::disk].value
                : "";

            StorageObjectStorageConfigurationPtr configuration;
            if (!disk_name.empty())
            {
                auto disk = Context::getGlobalContextInstance()->getDisk(disk_name);
                switch (disk->getObjectStorage()->getType())
                {
                    case ObjectStorageType::S3:
                        configuration = std::make_shared<StorageS3PaimonConfiguration>(storage_settings);
                        break;
                    default:
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Unsupported disk type for {}: {}",
                            PaimonS3Definition::storage_engine_name,
                            disk->getObjectStorage()->getType());
                }
            }
            else
                configuration = std::make_shared<StorageS3PaimonConfiguration>(storage_settings);
            expandPaimonKeeperMacrosIfNeeded(args, storage_settings);
            return createStorageObjectStorage(args, configuration);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::S3,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        },
        Documentation{
            .description = "Provides a read-only integration with existing Apache Paimon tables stored in Amazon S3 or S3-compatible object storage. "
                "This engine is experimental: enable it with the `allow_experimental_paimon_storage_engine` setting.",
            .syntax = "ENGINE = PaimonS3(url [, access_key_id, secret_access_key])",
            .related = {"Paimon"}});
#endif

#if USE_AZURE_BLOB_STORAGE
    /// Register PaimonAzure engine
    factory.registerStorage(
        PaimonAzureDefinition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            check_paimon_storage_engine_enabled(args);
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            const auto disk_name = storage_settings && (*storage_settings)[DataLakeStorageSetting::disk].changed
                ? (*storage_settings)[DataLakeStorageSetting::disk].value
                : "";

            StorageObjectStorageConfigurationPtr configuration;
            if (!disk_name.empty())
            {
                auto disk = Context::getGlobalContextInstance()->getDisk(disk_name);
                switch (disk->getObjectStorage()->getType())
                {
                    case ObjectStorageType::Azure:
                        configuration = std::make_shared<StorageAzurePaimonConfiguration>(storage_settings);
                        break;
                    default:
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Unsupported disk type for {}: {}",
                            PaimonAzureDefinition::storage_engine_name,
                            disk->getObjectStorage()->getType());
                }
            }
            else
                configuration = std::make_shared<StorageAzurePaimonConfiguration>(storage_settings);
            expandPaimonKeeperMacrosIfNeeded(args, storage_settings);
            return createStorageObjectStorage(args, configuration);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::AZURE,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        },
        Documentation{
            .description = "Provides a read-only integration with existing Apache Paimon tables stored in Microsoft Azure Blob Storage. "
                "This engine is experimental: enable it with the `allow_experimental_paimon_storage_engine` setting.",
            .syntax = "ENGINE = PaimonAzure(connection_string | storage_account_url, container_name, blobpath)",
            .related = {"Paimon"}});
#endif

#if USE_HDFS
    /// Register PaimonHDFS engine
    factory.registerStorage(
        PaimonHDFSDefinition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            check_paimon_storage_engine_enabled(args);
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            auto configuration = std::make_shared<StorageHDFSPaimonConfiguration>(storage_settings);
            expandPaimonKeeperMacrosIfNeeded(args, storage_settings);
            return createStorageObjectStorage(args, configuration);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::HDFS,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        },
        Documentation{
            .description = "Provides a read-only integration with existing Apache Paimon tables stored in HDFS. "
                "This engine is experimental: enable it with the `allow_experimental_paimon_storage_engine` setting.",
            .syntax = "ENGINE = PaimonHDFS(uri)",
            .related = {"Paimon"}});
#endif

    /// Register PaimonLocal engine
    factory.registerStorage(
        PaimonLocalDefinition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            check_paimon_storage_engine_enabled(args);
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            const auto disk_name = storage_settings && (*storage_settings)[DataLakeStorageSetting::disk].changed
                ? (*storage_settings)[DataLakeStorageSetting::disk].value
                : "";

            StorageObjectStorageConfigurationPtr configuration;
            if (!disk_name.empty())
            {
                auto disk = Context::getGlobalContextInstance()->getDisk(disk_name);
                switch (disk->getObjectStorage()->getType())
                {
                    case ObjectStorageType::Local:
                        configuration = std::make_shared<StorageLocalPaimonConfiguration>(storage_settings);
                        break;
                    default:
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Unsupported disk type for {}: {}",
                            PaimonLocalDefinition::storage_engine_name,
                            disk->getObjectStorage()->getType());
                }
            }
            else
                configuration = std::make_shared<StorageLocalPaimonConfiguration>(storage_settings);
            expandPaimonKeeperMacrosIfNeeded(args, storage_settings);
            return createStorageObjectStorage(args, configuration);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::FILE,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        },
        Documentation{
            .description = "Provides a read-only integration with existing Apache Paimon tables stored on the local filesystem. "
                "This engine is experimental: enable it with the `allow_experimental_paimon_storage_engine` setting.",
            .syntax = "ENGINE = PaimonLocal(path)",
            .related = {"Paimon"}});
}

#endif


#if USE_PARQUET && USE_DELTA_KERNEL_RS
void registerStorageDeltaLake(StorageFactory & factory);
void registerStorageDeltaLake(StorageFactory & factory)
{
#if USE_AWS_S3
    factory.registerStorage(
        DeltaLakeDefinition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            const auto disk_name = storage_settings && (*storage_settings)[DataLakeStorageSetting::disk].changed
                ? (*storage_settings)[DataLakeStorageSetting::disk].value
                : "";

            StorageObjectStorageConfigurationPtr configuration;
            if (!disk_name.empty())
            {
                auto disk = Context::getGlobalContextInstance()->getDisk(disk_name);
                switch (disk->getObjectStorage()->getType())
                {
                    case ObjectStorageType::S3:
                    {
                        configuration = std::make_shared<StorageS3DeltaLakeConfiguration>(storage_settings);
                        break;
                    }
                    case ObjectStorageType::Azure:
                        configuration = std::make_shared<StorageAzureDeltaLakeConfiguration>(storage_settings);
                        break;
                    case ObjectStorageType::Local:
                        configuration = std::make_shared<StorageLocalDeltaLakeConfiguration>(storage_settings);
                        break;
                    default:
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported disk type for {}: {}", DeltaLakeDefinition::storage_engine_name, disk->getObjectStorage()->getType());
                }
            }
            else
                configuration = std::make_shared<StorageS3DeltaLakeConfiguration>(storage_settings);

            return createStorageObjectStorage(args, configuration);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::S3,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        },
        Documentation{
            .description = R"DOCS_MD(
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# DeltaLake table engine

This engine provides an integration with existing [Delta Lake](https://github.com/delta-io/delta) tables in S3, GCP and Azure storage and supports both reads and writes (from v25.10).

## Create a DeltaLake table {#create-table}

To create a DeltaLake table it must already exist in S3, GCP or Azure storage. The commands below do not take DDL parameters to create a new table.

<Tabs>
<TabItem value="S3" label="S3" default>

**Syntax**

```sql
CREATE TABLE table_name
ENGINE = DeltaLake(url, [aws_access_key_id, aws_secret_access_key,] [extra_credentials])
```

**Engine parameters**

- `url` — Bucket url with path to the existing Delta Lake table.
- `aws_access_key_id`, `aws_secret_access_key` - Long-term credentials for the [AWS](https://aws.amazon.com/) account user.  You can use these to authenticate your requests. Parameter is optional. If credentials are not specified, they are used from the configuration file.
- `extra_credentials` - Optional. Used to pass a `role_arn` for role-based access in ClickHouse Cloud. See [Secure S3](/cloud/data-sources/secure-s3) for configuration steps.

Engine parameters can be specified using [Named Collections](/operations/named-collections.md).

**Example**

```sql
CREATE TABLE deltalake
ENGINE = DeltaLake('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
```

Using named collections:

```xml
<clickhouse>
    <named_collections>
        <deltalake_conf>
            <url>http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/</url>
            <access_key_id>ABC123<access_key_id>
            <secret_access_key>Abc+123</secret_access_key>
        </deltalake_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE deltalake
ENGINE = DeltaLake(deltalake_conf, filename = 'test_table')
```
</TabItem>

<TabItem value="GCP" label="GCP" default>

**Syntax**

```sql
-- Using HTTPS URL (recommended)
CREATE TABLE table_name
ENGINE = DeltaLake('https://storage.googleapis.com/<bucket>/<path>/', '<access_key_id>', '<secret_access_key>')
```

:::note[Unsupported gsutil URI]
gsutil URI such as `gs://clickhouse-docs-example-bucket` is not supported, please use a URL starting `https://storage.googleapis.com`
:::

**Arguments**

- `url` — GCS bucket URL to the Delta Lake table. Must use `https://storage.googleapis.com/<bucket>/<path>/`
   format (the GCS XML API endpoint), or `gs://<bucket>/<path>/` which is auto-converted.
- `access_key_id` — GCS Access Key. Create via Google Cloud Console → Cloud Storage → Settings → Interoperability.
- `secret_access_key` — GCS secret.

**Named collections**

You can also use named collections.
For example:

```sql
CREATE NAMED COLLECTION gcs_creds AS
access_key_id = '<access_key>',
secret_access_key = '<secret>';

CREATE TABLE gcpDeltaLake
ENGINE = DeltaLake(gcs_creds, url = 'https://storage.googleapis.com/<bucket>/<path>')
```

</TabItem>

<TabItem value="Azure" label="Azure" default>

**Syntax**

```sql
CREATE TABLE table_name
ENGINE = DeltaLake(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])
```

**Arguments**

- `connection_string` — Azure connection string
- `storage_account_url` — Azure storage account URL (e.g., https://account.blob.core.windows.net)
- `container_name` — Azure container name
- `blobpath` — Path to the Delta Lake table within the container
- `account_name` — Azure storage account name
- `account_key` — Azure storage account key

</TabItem>
</Tabs>

## Write data using a DeltaLake table {#insert-data}

Once you have created a table using the DeltaLake table engine, you can insert data into it with:

```sql
SET allow_experimental_delta_lake_writes = 1;

INSERT INTO deltalake(id, firstname, lastname, gender, age)
VALUES (1, 'John', 'Smith', 'M', 32);
```

:::note
Writing using the table engine is supported only through delta kernel.
Writes to Azure are not yet supported but work for S3 and GCS.
:::

### Data cache {#data-cache}

The `DeltaLake` table engine and table function support data caching, the same as `S3`, `AzureBlobStorage`, `HDFS` storages. See ["S3 table engine"](../../../engines/table-engines/integrations/s3.md#data-cache) for more details.

## See also {#see-also}

- [deltaLake table function](../../../sql-reference/table-functions/deltalake.md)
)DOCS_MD",
            .syntax = "ENGINE = DeltaLake(url [, access_key_id, secret_access_key])",
            .related = {"DeltaLakeS3", "DeltaLakeAzure", "DeltaLakeLocal", "Iceberg", "Hudi"}});
    factory.registerStorage(
        DeltaLakeS3Definition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            const auto disk_name = storage_settings && (*storage_settings)[DataLakeStorageSetting::disk].changed
                ? (*storage_settings)[DataLakeStorageSetting::disk].value
                : "";

            StorageObjectStorageConfigurationPtr configuration;
            if (!disk_name.empty())
            {
                auto disk = Context::getGlobalContextInstance()->getDisk(disk_name);
                switch (disk->getObjectStorage()->getType())
                {
                case ObjectStorageType::S3:
                {
                    configuration = std::make_shared<StorageS3DeltaLakeConfiguration>(storage_settings);
                    break;
                }
                default:
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported disk type for {}: {}", DeltaLakeS3Definition::storage_engine_name, disk->getObjectStorage()->getType());
                }
            }
            else
                configuration = std::make_shared<StorageS3DeltaLakeConfiguration>(storage_settings);

            return createStorageObjectStorage(args, configuration);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::S3,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        },
        Documentation{
            .description = "Provides a read-only integration with existing Delta Lake tables stored in Amazon S3 or S3-compatible object storage.",
            .syntax = "ENGINE = DeltaLakeS3(url [, access_key_id, secret_access_key])",
            .related = {"DeltaLake"}});
#    endif
#    if USE_AZURE_BLOB_STORAGE
    factory.registerStorage(
        DeltaLakeAzureDefinition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            const auto disk_name = storage_settings && (*storage_settings)[DataLakeStorageSetting::disk].changed
                ? (*storage_settings)[DataLakeStorageSetting::disk].value
                : "";

            StorageObjectStorageConfigurationPtr configuration;
            if (!disk_name.empty())
            {
                auto disk = Context::getGlobalContextInstance()->getDisk(disk_name);
                switch (disk->getObjectStorage()->getType())
                {
                    case ObjectStorageType::Azure:
                        configuration = std::make_shared<StorageAzureDeltaLakeConfiguration>(storage_settings);
                        break;
                    default:
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported disk type for {}: {}", DeltaLakeAzureDefinition::storage_engine_name, disk->getObjectStorage()->getType());
                }
            }
            else
                configuration = std::make_shared<StorageAzureDeltaLakeConfiguration>(storage_settings);
            return createStorageObjectStorage(args, configuration);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::AZURE,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        },
        Documentation{
            .description = "Provides a read-only integration with existing Delta Lake tables stored in Microsoft Azure Blob Storage.",
            .syntax = "ENGINE = DeltaLakeAzure(connection_string | storage_account_url, container_name, blobpath)",
            .related = {"DeltaLake"}});
#    endif
    factory.registerStorage(
        DeltaLakeLocalDefinition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            const auto disk_name = storage_settings && (*storage_settings)[DataLakeStorageSetting::disk].changed
                ? (*storage_settings)[DataLakeStorageSetting::disk].value
                : "";

            StorageObjectStorageConfigurationPtr configuration;
            if (!disk_name.empty())
            {
                auto disk = Context::getGlobalContextInstance()->getDisk(disk_name);
                switch (disk->getObjectStorage()->getType())
                {
                    case ObjectStorageType::Local:
                        configuration = std::make_shared<StorageLocalDeltaLakeConfiguration>(storage_settings);
                        break;
                    default:
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported disk type for {}: {}", DeltaLakeLocalDefinition::storage_engine_name, disk->getObjectStorage()->getType());
                }
            }
            else
                configuration = std::make_shared<StorageLocalDeltaLakeConfiguration>(storage_settings);
            return createStorageObjectStorage(args, configuration);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::FILE,
            .has_builtin_setting_fn = StorageObjectStorageSettings::hasBuiltin,
        },
        Documentation{
            .description = "Provides a read-only integration with existing Delta Lake tables stored on the local filesystem.",
            .syntax = "ENGINE = DeltaLakeLocal(path)",
            .related = {"DeltaLake"}});
}
#endif

void registerStorageHudi(StorageFactory & factory);
void registerStorageHudi(StorageFactory & factory)
{
#if USE_AWS_S3
    factory.registerStorage(
        HudiDefinition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            auto configuration = std::make_shared<StorageS3HudiConfiguration>(storage_settings);
            return createStorageObjectStorage(args, configuration);
        },
        {
            .supports_settings = false,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::S3,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        },
        Documentation{
            .description = R"DOCS_MD(
This engine provides a read-only integration with existing Apache [Hudi](https://hudi.apache.org/) tables in Amazon S3.

## Create table {#create-table}

Note that the Hudi table must already exist in S3, this command does not take DDL parameters to create a new table.

```sql
CREATE TABLE hudi_table
    ENGINE = Hudi(url, [aws_access_key_id, aws_secret_access_key,] [extra_credentials])
```

**Engine parameters**

- `url` — Bucket url with the path to an existing Hudi table.
- `aws_access_key_id`, `aws_secret_access_key` - Long-term credentials for the [AWS](https://aws.amazon.com/) account user.  You can use these to authenticate your requests. Parameter is optional. If credentials are not specified, they are used from the configuration file.
- `extra_credentials` - Optional. Used to pass a `role_arn` for role-based access in ClickHouse Cloud. See [Secure S3](/cloud/data-sources/secure-s3) for configuration steps.

Engine parameters can be specified using [Named Collections](/operations/named-collections.md).

**Example**

```sql
CREATE TABLE hudi_table ENGINE=Hudi('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
```

Using named collections:

```xml
<clickhouse>
    <named_collections>
        <hudi_conf>
            <url>http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/</url>
            <access_key_id>ABC123<access_key_id>
            <secret_access_key>Abc+123</secret_access_key>
        </hudi_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE hudi_table ENGINE=Hudi(hudi_conf, filename = 'test_table')
```

## See also {#see-also}

- [hudi table function](/sql-reference/table-functions/hudi.md)
)DOCS_MD",
            .syntax = "ENGINE = Hudi(url [, access_key_id, secret_access_key])",
            .related = {"Iceberg", "DeltaLake"}});
#endif
    UNUSED(factory);
}
}
