---
description: '这是 deltaLake 表函数的扩展。'
sidebar_label: 'deltaLakeCluster'
sidebar_position: 46
slug: /sql-reference/table-functions/deltalakeCluster
title: 'deltaLakeCluster'
doc_type: 'reference'
---

这是 [deltaLake](/zh/sql-reference/table-functions/deltalake.md) 表函数的扩展。

它允许在指定集群中的多个节点上并行处理亚马逊 S3 中 [Delta Lake](https://github.com/delta-io/delta) 表的文件。在发起节点上，它会与集群中的所有节点建立连接，并动态分发各个文件。在工作节点上，它会向发起节点请求下一个待处理任务并执行处理。如此重复，直到所有任务都完成。

<div id="syntax">
  ## 语法
</div>

```sql
deltaLakeCluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
deltaLakeCluster(cluster_name, named_collection[, option=value [,..]])

deltaLakeS3Cluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
deltaLakeS3Cluster(cluster_name, named_collection[, option=value [,..]])

deltaLakeAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
deltaLakeAzureCluster(cluster_name, named_collection[, option=value [,..]])
```

`deltaLakeS3Cluster` 是 `deltaLakeCluster` 的别名，二者均用于 S3。

<div id="arguments">
  ## 参数
</div>

* `cluster_name` — 用于构建远程和本地 server 的地址集合及连接参数的 cluster 名称。
* 其他所有参数的说明与对应的 [deltaLake](/zh/sql-reference/table-functions/deltalake.md) 表函数 中的参数说明一致。
* 可使用可选的 `extra_credentials` 参数传递 `role_arn`，以便在 ClickHouse Cloud 中启用基于角色的访问。配置步骤请参见 [Secure S3](/zh/cloud/data-sources/secure-s3)。

<div id="returned_value">
  ## 返回值
</div>

一个具有指定结构的表，用于从集群中读取位于 S3 中指定 Delta Lake 表的数据。

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_path` — 文件路径。类型：`LowCardinality(String)`。
* `_file` — 文件名。类型：`LowCardinality(String)`。
* `_size` — 文件大小 (以字节为单位) 。类型：`Nullable(UInt64)`。如果文件大小未知，则值为 `NULL`。
* `_time` — 文件的最后修改时间。类型：`Nullable(DateTime)`。如果时间未知，则值为 `NULL`。
* `_etag` — 文件的 etag 值。类型：`LowCardinality(String)`。如果 etag 未知，则值为 `NULL`。

<div id="related">
  ## 相关
</div>

* [deltaLake 引擎](/zh/engines/table-engines/integrations/deltalake.md)
* [deltaLake 表函数](/zh/sql-reference/table-functions/deltalake.md)