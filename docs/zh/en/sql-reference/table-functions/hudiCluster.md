---
description: '这是对 hudi 表函数的扩展。支持在指定 集群 的多个节点上并行处理亚马逊 S3 中 Apache Hudi 表的文件。'
sidebar_label: 'hudiCluster'
sidebar_position: 86
slug: /sql-reference/table-functions/hudiCluster
title: 'hudiCluster 表函数'
doc_type: 'reference'
---

这是对 [hudi](/zh/sql-reference/table-functions/hudi.md) 表函数的扩展。

支持在指定 集群 的多个节点上并行处理亚马逊 S3 中 Apache [Hudi](https://hudi.apache.org/) 表的文件。在发起节点上，它会与 集群 中的所有节点建立连接，并动态分发各个文件。在工作节点上，它会向发起节点请求下一个待处理任务并执行处理。该过程会重复进行，直到所有任务全部完成。

<div id="syntax">
  ## 语法
</div>

```sql
hudiCluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
```

<div id="arguments">
  ## 参数
</div>

| Argument                                     | Description                                                                                                                                                                                                     |
| -------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`                               | 集群名称，用于构建远程和本地服务器的地址集合及连接参数。                                                                                                                                                                                    |
| `url`                                        | 存储桶 URL，包含 S3 中现有 Hudi 表的路径。                                                                                                                                                                                    |
| `aws_access_key_id`, `aws_secret_access_key` | AWS 账户用户的长期凭证。您可以使用这些凭证对请求进行身份验证。这些参数是可选的。如果未指定凭证，则使用 ClickHouse 配置中的凭证。更多信息，请参阅[使用 S3 进行数据存储](/zh/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3)和[AWS](https://aws.amazon.com/)。 |
| `format`                                     | 文件的 [format](/zh/interfaces/formats)。                                                                                                                                                                              |
| `structure`                                  | 表的结构。格式为 `'column1_name column1_type, column2_name column2_type, ...'`。                                                                                                                                         |
| `compression`                                | 此参数为可选。支持的值：`none`、`gzip/gz`、`brotli/br`、`xz/LZMA`、`zstd/zst`。默认情况下，会根据文件扩展名自动检测压缩方式。                                                                                                                           |
| `extra_credentials`                          | 此参数为可选。用于在 ClickHouse Cloud 中传递用于基于角色的访问控制的 `role_arn`。配置步骤请参阅 [Secure S3](/zh/cloud/data-sources/secure-s3) 和 [ClickHouse Cloud 文档](/zh/cloud/)。                                                                     |

<div id="returned_value">
  ## 返回值
</div>

一个具有指定结构的表，用于从集群中读取 S3 中指定 Hudi 表的数据。

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_path` — 文件路径。类型：`LowCardinality(String)`。
* `_file` — 文件名。类型：`LowCardinality(String)`。
* `_size` — 文件大小 (以字节为单位) 。类型：`Nullable(UInt64)`。如果文件大小未知，则值为 `NULL`。
* `_time` — 文件的最后修改时间。类型：`Nullable(DateTime)`。如果时间未知，则值为 `NULL`。
* `_etag` — 文件的 etag。类型：`LowCardinality(String)`。如果 etag 未知，则值为 `NULL`。

<div id="related">
  ## 相关内容
</div>

* [Hudi 引擎](/zh/engines/table-engines/integrations/hudi.md)
* [Hudi 表函数](/zh/sql-reference/table-functions/hudi.md)