---
description: '对 s3 表函数的扩展，可在指定集群中的多个节点上并行处理来自
  亚马逊 S3 和 Google Cloud Storage 的文件。'
sidebar_label: 's3Cluster'
sidebar_position: 181
slug: /sql-reference/table-functions/s3Cluster
title: 's3Cluster'
doc_type: 'reference'
---

这是对 [s3](/zh/sql-reference/table-functions/s3.md) 表函数的扩展。

支持在指定集群中的多个节点上并行处理来自 [亚马逊 S3](https://aws.amazon.com/s3/) 和 Google Cloud Storage [Google Cloud Storage](https://cloud.google.com/storage/) 的文件。在发起节点上，它会与集群中的所有节点建立连接，展开 S3 文件路径中的星号，并动态分发各个文件。在工作节点上，它会向发起节点请求下一个待处理任务并进行处理。如此重复，直到所有任务都处理完毕。

<div id="syntax">
  ## 语法
</div>

```sql
s3Cluster(cluster_name, url[, NOSIGN | access_key_id, secret_access_key,[session_token]][, format][, structure][, compression_method][, headers][, extra_credentials])
s3Cluster(cluster_name, named_collection[, option=value [,..]])
```

<div id="arguments">
  ## 参数
</div>

| Argument                                | Description                                                                                                                                                                                                  |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `cluster_name`                          | 用于构建远程和本地 server 地址集合及 connection 参数的 集群 名称。                                                                                                                                                            |
| `url`                                   | 指向单个文件或一组文件的 path。在 readonly 模式下支持以下 wildcards：`*`、`**`、`?`、`{'abc','def'}` 和 `{N..M}`，其中 `N`、`M` 表示数字，`abc`、`def` 表示字符串。更多信息请参见[路径中的通配符](../../engines/table-engines/integrations/s3.md#wildcards-in-path)。 |
| `NOSIGN`                                | 如果在 credentials 的位置提供此关键字，则所有 request 都不会被签名。                                                                                                                                                                |
| `access_key_id` and `secret_access_key` | 用于指定访问给定端点所用 credentials 的 key。可选。                                                                                                                                                                           |
| `session_token`                         | 与给定 key 一起使用的 session token。传入 key 时，此参数可选。                                                                                                                                                                  |
| `format`                                | 文件的 [format](/zh/sql-reference/formats)。                                                                                                                                                                        |
| `structure`                             | 表的结构。格式为 `'column1_name column1_type, column2_name column2_type, ...'`。                                                                                                                                      |
| `compression_method`                    | 此 parameter 为可选。支持的值有：`none`、`gzip` 或 `gz`、`brotli` 或 `br`、`xz` 或 `LZMA`、`zstd` 或 `zst`。默认会根据文件扩展名自动检测压缩方法。                                                                                                  |
| `headers`                               | 此 parameter 为可选。允许在 S3 request 中传递请求头。传递格式为 `headers(key=value)`，例如 `headers('x-amz-request-payer' = 'requester')`。用法示例见[这里](/zh/sql-reference/table-functions/s3#accessing-requester-pays-buckets)。            |
| `extra_credentials`                     | 可选。可通过此 parameter 传递 `roleARN`。示例见[这里](/zh/cloud/data-sources/secure-s3#access-your-s3-bucket-with-the-clickhouseaccess-role)。                                                                                  |

参数也可以通过 [named collections](/zh/operations/named-collections.md) 传递。在这种情况下，`url`、`access_key_id`、`secret_access_key`、`format`、`structure`、`compression_method` 的用法相同，并且还支持一些额外参数：

| Argument                      | Description                                                                                                                                                               |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `filename`                    | 如果指定，则会追加到 url 后面。                                                                                                                                                        |
| `use_environment_credentials` | 默认 enabled，允许通过环境变量 `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`、`AWS_CONTAINER_CREDENTIALS_FULL_URI`、`AWS_CONTAINER_AUTHORIZATION_TOKEN`、`AWS_EC2_METADATA_DISABLED` 传递额外参数。 |
| `no_sign_request`             | 默认禁用。                                                                                                                                                                     |
| `expiration_window_seconds`   | default value 为 120。                                                                                                                                                      |

<div id="returned_value">
  ## 返回值
</div>

一个具有指定结构的表，可用于读取指定文件中的数据或向其中写入数据。

<div id="examples">
  ## 示例
</div>

使用 `cluster_simple` 集群中的所有节点，查询 `/root/data/clickhouse` 和 `/root/data/database/` 文件夹内所有文件中的数据：

```sql
SELECT * FROM s3Cluster(
    'cluster_simple',
    'http://minio1:9001/root/data/{clickhouse,database}/*',
    'minio',
    'ClickHouse_Minio_P@ssw0rd',
    'CSV',
    'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))'
) ORDER BY (name, value, polygon);
```

统计集群 `cluster_simple` 中所有文件的总行数：

:::tip
如果列出的文件包含带前导零的数字范围，请对每一位数字分别使用花括号写法，或使用 `?`。
:::

对于生产环境，建议使用 [named collections](/zh/operations/named-collections.md)。示例如下：

```sql

CREATE NAMED COLLECTION creds AS
        access_key_id = 'minio',
        secret_access_key = 'ClickHouse_Minio_P@ssw0rd';
SELECT count(*) FROM s3Cluster(
    'cluster_simple', creds, url='https://s3-object-url.csv',
    format='CSV', structure='name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))'
)
```

<div id="accessing-private-and-public-buckets">
  ## 访问私有和公共存储桶
</div>

用户可以采用与[此处](/zh/sql-reference/table-functions/s3#accessing-public-buckets)为 s3 函数所记录的相同方法。

<div id="optimizing-performance">
  ## 优化性能
</div>

有关如何优化 s3 函数性能的更多信息，请参阅[我们的详细指南](/zh/integrations/s3/performance)。

<div id="related">
  ## 相关
</div>

* [S3 引擎](../../engines/table-engines/integrations/s3.md)
* [S3 表函数](../../sql-reference/table-functions/s3.md)