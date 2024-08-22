---
slug: /en/sql-reference/table-functions/azureBlobStorageCluster
sidebar_position: 15
sidebar_label: azureBlobStorageCluster
title: "azureBlobStorageCluster Table Function"
---

Allows processing files from [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) in parallel from many nodes in a specified cluster. On initiator it creates a connection to all nodes in the cluster, discloses asterisks in S3 file path, and dispatches each file dynamically. On the worker node it asks the initiator about the next task to process and processes it. This is repeated until all tasks are finished.
This table function is similar to the [s3Cluster function](../../sql-reference/table-functions/s3Cluster.md).

**Syntax**

``` sql
azureBlobStorageCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])
```

**Arguments**

- `cluster_name` — Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.
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

Select the count for the file `test_cluster_*.csv`, using all the nodes in the `cluster_simple` cluster:

``` sql
SELECT count(*) from azureBlobStorageCluster(
        'cluster_simple', 'http://azurite1:10000/devstoreaccount1', 'test_container', 'test_cluster_count.csv', 'devstoreaccount1',
        'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV',
        'auto', 'key UInt64')
```

**See Also**

- [AzureBlobStorage engine](../../engines/table-engines/integrations/azureBlobStorage.md)
- [azureBlobStorage table function](../../sql-reference/table-functions/azureBlobStorage.md)
