---
description: '这是对 [iceberg](/sql-reference/table-functions/iceberg.md) 表函数的扩展，允许在指定
  集群中由多个节点并行处理来自 Apache Iceberg 的文件。'
sidebar_label: 'icebergCluster'
sidebar_position: 91
slug: /sql-reference/table-functions/icebergCluster
title: 'icebergCluster'
doc_type: 'reference'
---

这是对 [iceberg](/zh/sql-reference/table-functions/iceberg.md) 表函数的扩展。

它允许在指定的集群中由多个节点并行处理来自 Apache [Iceberg](https://iceberg.apache.org/) 的文件。在发起节点上，它会与集群中的所有节点建立连接，并动态分发各个文件。在工作节点上，它会向发起节点请求下一个要处理的任务，并进行处理。该过程会重复进行，直到所有任务都处理完成。

<div id="syntax">
  ## 语法
</div>

```sql
icebergS3Cluster(cluster_name, url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method] [,extra_credentials])
icebergS3Cluster(cluster_name, named_collection[, option=value [,..]])

icebergAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzureCluster(cluster_name, named_collection[, option=value [,..]])

icebergHDFSCluster(cluster_name, path_to_table, [,format] [,compression_method])
icebergHDFSCluster(cluster_name, named_collection[, option=value [,..]])
```

<div id="arguments">
  ## 参数
</div>

* `cluster_name` — 用于构建远程和本地服务器地址集合及连接参数的集群名称。
* 其余所有参数的说明与对应的 [iceberg](/zh/sql-reference/table-functions/iceberg.md) 表函数中的参数说明一致。
* 可选的 `extra_credentials` 参数可用于传递 `role_arn`，以便在 ClickHouse Cloud 中使用基于角色的访问控制。配置步骤请参见 [Secure S3](/zh/cloud/data-sources/secure-s3)。

**返回值**

一个具有指定结构的表，用于从集群中指定的 Iceberg 表读取数据。

**示例**

```sql
SELECT * FROM icebergS3Cluster('cluster_simple', 'http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_path` — 文件路径。类型：`LowCardinality(String)`。
* `_file` — 文件名。类型：`LowCardinality(String)`。
* `_size` — 文件大小 (以字节为单位) 。类型：`Nullable(UInt64)`。如果文件大小未知，则值为 `NULL`。
* `_time` — 文件的最后修改时间。类型：`Nullable(DateTime)`。如果时间未知，则值为 `NULL`。
* `_etag` — 文件的 etag 值。类型：`LowCardinality(String)`。如果 etag 未知，则值为 `NULL`。

**另请参见**

* [Iceberg 引擎](/zh/engines/table-engines/integrations/iceberg.md)
* [Iceberg 表函数](/zh/sql-reference/table-functions/iceberg.md)