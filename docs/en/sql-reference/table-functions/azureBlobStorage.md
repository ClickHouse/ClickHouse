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

# azureBlobStorage Table Function

Provides a table-like interface to select/insert files in [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs). This table function is similar to the [s3 function](../../sql-reference/table-functions/s3.md).

## Syntax {#syntax}

```sql
azureBlobStorage(- connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure, partition_strategy, partition_columns_in_data_file, extra_credentials(client_id=, tenant_id=)])
```

## Arguments {#arguments}

| Argument                                    | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|---------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `connection_string`\| `storage_account_url` | connection_string includes account name & key ([Create connection string](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)) or you could also provide the storage account url here and account name & account key as separate parameters (see parameters account_name & account_key) |
| `container_name`                            | Container name                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `blobpath`                                  | file path. Supports following wildcards in readonly mode: `*`, `**`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc'`, `'def'` — strings.                                                                                                                                                                                                                                                                                                                                   |
| `account_name`                              | if storage_account_url is used, then account name can be specified here                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `account_key`                               | if storage_account_url is used, then account key can be specified here                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `format`                                    | The [format](/sql-reference/formats) of the file.                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `compression`                               | Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. By default, it will autodetect compression by file extension. (same as setting to `auto`).                                                                                                                                                                                                                                                                                                                        | 
| `structure`                                 | Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                                                                                                                              |
| `partition_strategy`                        | Parameter is optional. Supported values: `WILDCARD` or `HIVE`. `WILDCARD` requires a `{_partition_id}` in the path, which is replaced with the partition key. `HIVE` does not allow wildcards, assumes the path is the table root, and generates Hive-style partitioned directories with Snowflake IDs as filenames and the file format as the extension. Defaults to `WILDCARD`                                                                                                           |
| `partition_columns_in_data_file`            | Parameter is optional. Only used with `HIVE` partition strategy. Tells ClickHouse whether to expect partition columns to be written in the data file. Defaults `false`.                                                                                                                                                                                                                                                                                                                    |
| `extra_credentials`                         | Use `client_id` and `tenant_id` for authentication. If extra_credentials are provided, they are given priority over `account_name` and `account_key`.

## Returned value {#returned_value}

A table with the specified structure for reading or writing data in the specified file.

## Examples {#examples}

Similar to the [AzureBlobStorage](/engines/table-engines/integrations/azureBlobStorage) table engine, users can use Azurite emulator for local Azure Storage development. Further details [here](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub%2Cblob-storage). Below we assume Azurite is available at the hostname `azurite1`.

Write data into azure blob storage using the following :

```sql
INSERT INTO TABLE FUNCTION azureBlobStorage('http://azurite1:10000/devstoreaccount1',
    'testcontainer', 'test_{_partition_id}.csv', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==',
    'CSV', 'auto', 'column1 UInt32, column2 UInt32, column3 UInt32') PARTITION BY column3 VALUES (1, 2, 3), (3, 2, 1), (78, 43, 3);
```

And then it can be read using

```sql
SELECT * FROM azureBlobStorage('http://azurite1:10000/devstoreaccount1',
    'testcontainer', 'test_1.csv', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==',
    'CSV', 'auto', 'column1 UInt32, column2 UInt32, column3 UInt32');
```

```response
┌───column1─┬────column2─┬───column3─┐
│     3     │       2    │      1    │
└───────────┴────────────┴───────────┘
```

or using connection_string

```sql
SELECT count(*) FROM azureBlobStorage('DefaultEndpointsProtocol=https;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;EndPointSuffix=core.windows.net',
    'testcontainer', 'test_3.csv', 'CSV', 'auto' , 'column1 UInt32, column2 UInt32, column3 UInt32');
```

```text
┌─count()─┐
│      2  │
└─────────┘
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
INSERT INTO TABLE FUNCTION azureBlobStorage(azure_conf2, storage_account_url = 'http://localhost:30000/devstoreaccount1', container='cont', blob_path='azure_table_root', format='CSVWithNames', compression='auto', structure='year UInt16, country String, id Int32', partition_strategy='hive') PARTITION BY (year, country) VALUES (2020, 'Russia', 1), (2021, 'Brazil', 2);
```

```result
select _path, * from azureBlobStorage(azure_conf2, storage_account_url = 'http://localhost:30000/devstoreaccount1', container='cont', blob_path='azure_table_root/**.csvwithnames')

   ┌─_path───────────────────────────────────────────────────────────────────────────┬─id─┬─year─┬─country─┐
1. │ cont/azure_table_root/year=2021/country=Brazil/7351307847391293440.csvwithnames │  2 │ 2021 │ Brazil  │
2. │ cont/azure_table_root/year=2020/country=Russia/7351307847378710528.csvwithnames │  1 │ 2020 │ Russia  │
   └─────────────────────────────────────────────────────────────────────────────────┴────┴──────┴─────────┘
```

## use_hive_partitioning setting {#hive-style-partitioning}

This is a hint for ClickHouse to parse hive style partitioned files upon reading time. It has no effect on writing. For symmetrical reads and writes, use the `partition_strategy` argument.

When setting `use_hive_partitioning` is set to 1, ClickHouse will detect Hive-style partitioning in the path (`/name=value/`) and will allow to use partition columns as virtual columns in the query. These virtual columns will have the same names as in the partitioned path, but starting with `_`.

**Example**

Use virtual column, created with Hive-style partitioning

```sql
SELECT * FROM azureBlobStorage(config, storage_account_url='...', container='...', blob_path='http://data/path/date=*/country=*/code=*/*.parquet') WHERE _date > '2020-01-01' AND _country = 'Netherlands' AND _code = 42;
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
