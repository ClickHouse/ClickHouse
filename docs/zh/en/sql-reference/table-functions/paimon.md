---
description: '为存储在亚马逊 S3、Azure、HDFS 或本地的 Apache Paimon 表提供只读的表格式接口。'
sidebar_label: 'paimon'
sidebar_position: 90
slug: /sql-reference/table-functions/paimon
title: 'paimon'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="paimon-table-function">
  # paimon 表函数
</div>

<ExperimentalBadge />

提供对存储在亚马逊 S3、Azure、HDFS 或本地的 Apache [Paimon](https://paimon.apache.org/) 表的只读表格式接口。

<div id="syntax">
  ## 语法
</div>

```sql
paimon(url [,access_key_id, secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

paimonS3(url [,access_key_id, secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

paimonAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

paimonHDFS(path_to_table, [,format] [,compression_method])

paimonLocal(path_to_table, [,format] [,compression_method])
```

<div id="arguments">
  ## 参数
</div>

这些参数的说明分别与表函数 `s3`、`azureBlobStorage`、`HDFS` 和 `file` 中对应参数的说明一致。
`format` 表示 Paimon 表中数据文件的格式。

对于 `paimonS3`，可以使用可选的 `extra_credentials` 参数传递 `role_arn`，以便在 ClickHouse Cloud 中启用基于角色的访问控制。有关配置步骤，请参见 [Secure S3](/zh/cloud/data-sources/secure-s3)。

<div id="returned-value">
  ### 返回值
</div>

一个具有指定结构的表，用于读取指定 Paimon 表中的数据。

<div id="defining-a-named-collection">
  ## 定义 named collection
</div>

下面示例展示了如何配置一个 named collection 来存储 URL 和凭证：

```xml
<clickhouse>
    <named_collections>
        <paimon_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
            <format>auto</format>
            <structure>auto</structure>
        </paimon_conf>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM paimonS3(paimon_conf, filename = 'test_table')
DESCRIBE paimonS3(paimon_conf, filename = 'test_table')
```

<div id="aliases">
  ## 别名
</div>

表函数 `paimon` 现为 `paimonS3` 的别名。

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_path` — 文件路径。类型：`LowCardinality(String)`。
* `_file` — 文件名。类型：`LowCardinality(String)`。
* `_size` — 文件大小 (字节) 。类型：`Nullable(UInt64)`。如果文件大小未知，则值为 `NULL`。
* `_time` — 文件的最后修改时间。类型：`Nullable(DateTime)`。如果时间未知，则值为 `NULL`。
* `_etag` — 文件的 etag。类型：`LowCardinality(String)`。如果 etag 未知，则值为 `NULL`。

<div id="data-types-supported">
  ## 支持的数据类型
</div>

| Paimon 数据类型                       | ClickHouse 数据类型           |
| --------------------------------- | ------------------------- |
| BOOLEAN                           | Int8                      |
| TINYINT                           | Int8                      |
| SMALLINT                          | Int16                     |
| INTEGER                           | Int32                     |
| BIGINT                            | Int64                     |
| FLOAT                             | Float32                   |
| DOUBLE                            | Float64                   |
| STRING,VARCHAR,BYTES,VARBINARY    | String                    |
| DATE                              | Date                      |
| TIME(p),TIME                      | Time(&#39;UTC&#39;)       |
| TIMESTAMP(p) WITH LOCAL TIME ZONE | DateTime64                |
| TIMESTAMP(p)                      | DateTime64(&#39;UTC&#39;) |
| CHAR                              | FixedString(1)            |
| BINARY(n)                         | FixedString(n)            |
| DECIMAL(P,S)                      | Decimal(P,S)              |
| ARRAY                             | Array                     |
| MAP                               | Map                       |

<div id="partition-supported">
  ## 支持的分区
</div>

Paimon 分区键支持的数据类型：

* `CHAR`
* `VARCHAR`
* `BOOLEAN`
* `DECIMAL`
* `TINYINT`
* `SMALLINT`
* `INTEGER`
* `DATE`
* `TIME`
* `TIMESTAMP`
* `TIMESTAMP WITH LOCAL TIME ZONE`
* `BIGINT`
* `FLOAT`
* `DOUBLE`

<div id="see-also">
  ## 另请参阅
</div>

* [Paimon cluster 表函数](/zh/sql-reference/table-functions/paimonCluster.md)