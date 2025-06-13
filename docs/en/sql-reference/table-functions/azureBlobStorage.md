---
slug: /en/sql-reference/table-functions/azureBlobStorage
sidebar_position: 10
sidebar_label: azureBlobStorage
keywords: [azure blob storage]
---

# azureBlobStorage Table Function

Provides a table-like interface to select/insert files in [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs). This table function is similar to the [s3 function](../../sql-reference/table-functions/s3.md).

**Syntax**

``` sql
azureBlobStorage(- connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])
```

**Arguments**

- `connection_string|storage_account_url` — connection_string includes account name & key ([Create connection string](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)) or you could also provide the storage account url here and account name & account key as separate parameters (see parameters account_name & account_key)
- `container_name` - Container name
- `blobpath` - file path. Supports following wildcards in readonly mode: `*`, `**`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc'`, `'def'` — strings.
- `account_name` - if storage_account_url is used, then account name can be specified here
- `account_key` - if storage_account_url is used, then account key can be specified here
- `format` — The [format](../../interfaces/formats.md#formats) of the file.
- `compression` — Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. By default, it will autodetect compression by file extension. (same as setting to `auto`).
- `structure` — Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.

**Returned value**

A table with the specified structure for reading or writing data in the specified file.

**Examples**

Write data into azure blob storage using the following :

```sql
INSERT INTO TABLE FUNCTION azureBlobStorage('http://azurite1:10000/devstoreaccount1',
    'test_container', 'test_{_partition_id}.csv', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==',
    'CSV', 'auto', 'column1 UInt32, column2 UInt32, column3 UInt32') PARTITION BY column3 VALUES (1, 2, 3), (3, 2, 1), (78, 43, 3);
```

And then it can be read using

```sql
SELECT * FROM azureBlobStorage('http://azurite1:10000/devstoreaccount1',
    'test_container', 'test_1.csv', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==',
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
    'test_container', 'test_3.csv', 'CSV', 'auto' , 'column1 UInt32, column2 UInt32, column3 UInt32');
```

``` text
┌─count()─┐
│      2  │
└─────────┘
```

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinalty(String)`.
- `_file` — Name of the file. Type: `LowCardinalty(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.

**See Also**

- [AzureBlobStorage Table Engine](/docs/en/engines/table-engines/integrations/azureBlobStorage.md)

## Hive-style partitioning {#hive-style-partitioning}

When setting `use_hive_partitioning` is set to 1, ClickHouse will detect Hive-style partitioning in the path (`/name=value/`) and will allow to use partition columns as virtual columns in the query. These virtual columns will have the same names as in the partitioned path, but starting with `_`.

**Example**

Use virtual column, created with Hive-style partitioning

``` sql
SET use_hive_partitioning = 1;
SELECT * from azureBlobStorage(config, storage_account_url='...', container='...', blob_path='http://data/path/date=*/country=*/code=*/*.parquet') where _date > '2020-01-01' and _country = 'Netherlands' and _code = 42;
```
