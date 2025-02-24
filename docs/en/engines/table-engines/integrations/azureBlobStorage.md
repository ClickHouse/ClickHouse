---
slug: /en/engines/table-engines/integrations/azureBlobStorage
sidebar_position: 10
sidebar_label: Azure Blob Storage
---

# AzureBlobStorage Table Engine

This engine provides an integration with [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) ecosystem.

## Create Table

``` sql
CREATE TABLE azure_blob_storage_table (name String, value UInt32)
    ENGINE = AzureBlobStorage(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])
    [PARTITION BY expr]
    [SETTINGS ...]
```

### Engine parameters

- `endpoint` — AzureBlobStorage endpoint URL with container & prefix. Optionally can contain account_name if the authentication method used needs it. (http://azurite1:{port}/[account_name]{container_name}/{data_prefix}) or these parameters can be provided separately using storage_account_url, account_name & container. For specifying prefix, endpoint should be used.
- `endpoint_contains_account_name` - This flag is used to specify if endpoint contains account_name as it is only needed for certain authentication methods. (Default : true)
- `connection_string|storage_account_url` — connection_string includes account name & key ([Create connection string](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)) or you could also provide the storage account url here and account name & account key as separate parameters (see parameters account_name & account_key)
- `container_name` - Container name
- `blobpath` - file path. Supports following wildcards in readonly mode: `*`, `**`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc'`, `'def'` — strings.
- `account_name` - if storage_account_url is used, then account name can be specified here
- `account_key` - if storage_account_url is used, then account key can be specified here
- `format` — The [format](/docs/en/interfaces/formats.md) of the file.
- `compression` — Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. By default, it will autodetect compression by file extension. (same as setting to `auto`).

**Example**

``` sql
CREATE TABLE test_table (key UInt64, data String)
    ENGINE = AzureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;',
    'test_container', 'test_table', 'CSV');

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

- `_path` — Path to the file. Type: `LowCardinalty(String)`.
- `_file` — Name of the file. Type: `LowCardinalty(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.

## Authentication

Currently there are 3 ways to authenticate:
- `Managed Identity` - Can be used by providing an `endpoint`, `connection_string` or `storage_account_url`.
- `SAS Token` - Can be used by providing an `endpoint`, `connection_string` or `storage_account_url`. It is identified by presence of '?' in the url.
- `Workload Identity` - Can be used by providing an `endpoint` or `storage_account_url`. If `use_workload_identity` parameter is set in config, ([workload identity](https://github.com/Azure/azure-sdk-for-cpp/tree/main/sdk/identity/azure-identity#authenticate-azure-hosted-applications)) is used for authentication.

### Data cache {#data-cache}

`Azure` table engine supports data caching on local disk.
See filesystem cache configuration options and usage in this [section](/docs/en/operations/storing-data.md/#using-local-cache).
Caching is made depending on the path and ETag of the storage object, so clickhouse will not read a stale cache version.

To enable caching use a setting `filesystem_cache_name = '<name>'` and `enable_filesystem_cache = 1`.

```sql
SELECT *
FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'test_container', 'test_table', 'CSV')
SETTINGS filesystem_cache_name = 'cache_for_azure', enable_filesystem_cache = 1;
```

1. add the following section to clickhouse configuration file:

``` xml
<clickhouse>
    <filesystem_caches>
        <cache_for_azure>
            <path>path to cache directory</path>
            <max_size>10Gi</max_size>
        </cache_for_azure>
    </filesystem_caches>
</clickhouse>
```

2. reuse cache configuration (and therefore cache storage) from clickhouse `storage_configuration` section, [described here](/docs/en/operations/storing-data.md/#using-local-cache)

## See also

[Azure Blob Storage Table Function](/docs/en/sql-reference/table-functions/azureBlobStorage)
