---
description: 'Provides a table-like interface to select/insert files in Azure Blob
  Storage. Similar to the s3 function.'
keywords: ['azure blob storage']
sidebar_label: 'azureBlobStorage'
sidebar_position: 10
slug: /sql-reference/table-functions/azureBlobStorage
title: 'azureBlobStorage'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# azureBlobStorage Table Function

Provides a table-like interface to select/insert files in [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs). This table function is similar to the [s3 function](../../sql-reference/table-functions/s3.md).

## Syntax {#syntax}

<Tabs>
<TabItem value="connection_string" label="Connection string" default>

Credentials are embedded in the connection string, so no separate `account_name`/`account_key` is needed:

```sql
azureBlobStorage(connection_string, container_name, blobpath [, format, compression, structure])
```

</TabItem>
<TabItem value="storage_account_url" label="Storage account URL">

Requires `account_name` and `account_key` as separate arguments:

```sql
azureBlobStorage(storage_account_url, container_name, blobpath, account_name, account_key [, format, compression, structure])
```

</TabItem>
<TabItem value="named_collection" label="Named collection">

See [Named Collections](#named-collections) below for the full list of supported keys:

```sql
azureBlobStorage(named_collection[, option=value [,..]])
```

</TabItem>
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
| `format`                         | The [format](/sql-reference/formats) of the file.                                                                                                                                                                                                                                                                                                         |
| `compression`                    | Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. By default, it will autodetect compression by file extension (same as setting to `auto`).                                                                                                                                                                                       |
| `structure`                      | Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                             |
| `partition_strategy`             | Optional. Supported values: `WILDCARD` or `HIVE`. `WILDCARD` requires a `{_partition_id}` in the path, which is replaced with the partition key. `HIVE` does not allow wildcards, assumes the path is the table root, and generates Hive-style partitioned directories with Snowflake IDs as filenames and the file format as the extension. Defaults to `WILDCARD`. |
| `partition_columns_in_data_file` | Optional. Only used with `HIVE` partition strategy. Tells ClickHouse whether to expect partition columns to be written in the data file. Defaults `false`.                                                                                                                                                                                                 |
| `extra_credentials`              | Use `client_id` and `tenant_id` for authentication. If extra_credentials are provided, they are given priority over `account_name` and `account_key`.                                                                                                                                                                                                     |

## Named Collections {#named-collections}

Arguments can also be passed using [named collections](/operations/named-collections). In this case the following keys are supported:

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

:::note
Named collection key names differ from positional function argument names: `container` (not `container_name`) and `blob_path` (not `blobpath`).
:::

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

```sql
INSERT INTO TABLE FUNCTION azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'test_{_partition_id}.csv',
    'CSV',
    'auto',
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

`WILDCARD` (default): Replaces the `{_partition_id}` wildcard in the file path with the actual partition key.

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
- [AzureBlobStorage Table Engine](engines/table-engines/integrations/azureBlobStorage.md)
