---
description: '允许在指定集群中由多个节点并行处理 Azure Blob 存储中的文件。'
sidebar_label: 'azureBlobStorageCluster'
sidebar_position: 15
slug: /sql-reference/table-functions/azureBlobStorageCluster
title: 'azureBlobStorageCluster'
doc_type: 'reference'
---

允许在指定集群中由多个节点并行处理 [Azure Blob 存储](https://azure.microsoft.com/en-us/products/storage/blobs) 中的文件。在发起节点上，它会与集群中的所有节点建立连接，展开 S3 文件路径中的星号通配符，并动态分发各个文件。在工作节点上，它会向发起节点请求下一个待处理任务并进行处理。此过程会一直重复，直到所有任务都处理完成。
此表函数类似于 [s3Cluster 函数](../../sql-reference/table-functions/s3Cluster.md)。

<div id="syntax">
  ## 语法
</div>

```sql
azureBlobStorageCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])
```

<div id="arguments">
  ## 参数
</div>

| Argument            | Description                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`      | 用于构建远程和本地 server 地址集合及连接参数的集群名称。                                                                                                                                                                                                                                                                                                                                                                                                      |
| `connection_string` | storage&#95;account&#95;url&#96; — connection&#95;string 包含账户名称和 key ([创建连接字符串](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json\&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)) ，或者你也可以在此处提供 storage account URL，并将账户名称和账户 key 作为单独的参数提供 (参见参数 account&#95;name 和 account&#95;key)  |
| `container_name`    | 容器名称                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `blobpath`          | 文件路径。在 readonly 模式下支持以下通配符：`*`、`**`、`?`、`{abc,def}` 和 `{N..M}`，其中 `N`、`M` 表示数字，`'abc'`、`'def'` 表示字符串。                                                                                                                                                                                                                                                                                                                                        |
| `account_name`      | 如果使用 storage&#95;account&#95;url，则可在此处指定账户名称                                                                                                                                                                                                                                                                                                                                                                                                 |
| `account_key`       | 如果使用 storage&#95;account&#95;url，则可在此处指定账户 key                                                                                                                                                                                                                                                                                                                                                                                               |
| `format`            | 文件的[格式](/zh/sql-reference/formats)。                                                                                                                                                                                                                                                                                                                                                                                                             |
| `compression`       | 支持的值：`none`、`gzip/gz`、`brotli/br`、`xz/LZMA`、`zstd/zst`。默认情况下，会根据文件扩展名自动检测压缩方式。 (等同于设置为 `auto`。)                                                                                                                                                                                                                                                                                                                                              |
| `structure`         | 表的结构。格式为 `'column1_name column1_type, column2_name column2_type, ...'`。                                                                                                                                                                                                                                                                                                                                                                      |

<div id="returned_value">
  ## 返回值
</div>

一个具有指定结构、用于读取或写入指定文件中数据的表。

<div id="examples">
  ## 示例
</div>

与 [AzureBlobStorage](/zh/engines/table-engines/integrations/azureBlobStorage) 表引擎类似，用户也可以使用 Azurite 模拟器在本地进行 Azure Storage 开发。更多信息请参见[此处](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub%2Cblob-storage)。下面假设 Azurite 可通过主机名 `azurite1` 访问。

使用 `cluster_simple` 集群中的所有节点，统计文件 `test_cluster_*.csv` 的数量：

```sql
SELECT count(*) FROM azureBlobStorageCluster(
        'cluster_simple', 'http://azurite1:10000/devstoreaccount1', 'testcontainer', 'test_cluster_count.csv', 'devstoreaccount1',
        'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV',
        'auto', 'key UInt64')
```

<div id="using-shared-access-signatures-sas-sas-tokens">
  ## 使用共享访问签名 (SAS)
</div>

示例请参见 [azureBlobStorage](/zh/sql-reference/table-functions/azureBlobStorage#using-shared-access-signatures-sas-sas-tokens)。

<div id="related">
  ## 相关
</div>

* [AzureBlobStorage 引擎](../../engines/table-engines/integrations/azureBlobStorage.md)
* [azureBlobStorage 表函数](../../sql-reference/table-functions/azureBlobStorage.md)