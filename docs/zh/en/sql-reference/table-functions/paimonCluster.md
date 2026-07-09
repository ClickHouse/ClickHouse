---
description: 'paimon 表函数的扩展，可在指定集群中的多个节点上并行处理来自 Apache
  Paimon 的文件。'
sidebar_label: 'paimonCluster'
sidebar_position: 91
slug: /sql-reference/table-functions/paimonCluster
title: 'paimonCluster'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="paimoncluster-table-function">
  # paimonCluster 表函数
</div>

<ExperimentalBadge />

这是 [paimon](/zh/sql-reference/table-functions/paimon.md) 表函数的扩展版本。

允许在指定集群的多个节点上并行处理 Apache [Paimon](https://paimon.apache.org/) 中的文件。发起节点会与集群中的所有节点建立连接，并动态分发各个文件。工作节点则向发起节点请求下一个待处理任务并进行处理。如此重复，直到所有任务完成。

<div id="syntax">
  ## 语法
</div>

```sql
paimonS3Cluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

paimonAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

paimonHDFSCluster(cluster_name, path_to_table, [,format] [,compression_method])
```

<div id="arguments">
  ## 参数
</div>

* `cluster_name` — 用于构建远程和本地服务器地址集合及连接参数的集群名称。
* 其他所有参数的说明与对应的 [paimon](/zh/sql-reference/table-functions/paimon.md) 表函数中的参数说明一致。
* 可选的 `extra_credentials` 参数可用于传递 `role_arn`，以便在 ClickHouse Cloud 中实现基于角色的访问控制。配置步骤请参见 [Secure S3](/zh/cloud/data-sources/secure-s3)。

**返回值**

一个具有指定结构的表，用于从指定的 Paimon 表中读取指定集群中的数据。

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_path` — 文件路径。类型：`LowCardinality(String)`。
* `_file` — 文件名。类型：`LowCardinality(String)`。
* `_size` — 文件大小 (以字节为单位) 。类型：`Nullable(UInt64)`。如果文件大小未知，则值为 `NULL`。
* `_time` — 文件的最后修改时间。类型：`Nullable(DateTime)`。如果时间未知，则值为 `NULL`。
* `_etag` — 文件的 etag。类型：`LowCardinality(String)`。如果 etag 未知，则值为 `NULL`。

**另请参见**

* [Paimon 表函数](/zh/sql-reference/table-functions/paimon.md)