---
description: '为亚马逊 S3 中的 Delta Lake 表提供只读的类似表的接口。'
sidebar_label: 'deltaLake'
sidebar_position: 45
slug: /sql-reference/table-functions/deltalake
title: 'deltaLake'
doc_type: 'reference'
---

为亚马逊 S3、Azure Blob 存储或本地挂载文件系统中的 [Delta Lake](https://github.com/delta-io/delta) 表提供类似表的接口，并支持读取和写入 (自 v25.10 起)

<div id="syntax">
  ## 语法
</div>

`deltaLake` 是 `deltaLakeS3` 的别名，保留此支持是为了兼容性。

```sql
deltaLake(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

deltaLakeS3(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

deltaLakeAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

deltaLakeLocal(path, [,format])
```

<div id="arguments">
  ## 参数
</div>

此表函数的参数分别与 `s3`、`azureBlobStorage`、`HDFS` 和 `file` 表函数的参数相同。
`format` 参数表示 Delta Lake 表中数据文件的格式。

可使用可选的 `extra_credentials` 参数传递 `role_arn`，以便在 ClickHouse Cloud 中启用基于角色的访问。配置步骤请参见 [Secure S3](/zh/cloud/data-sources/secure-s3)。

<div id="returned_value">
  ## 返回值
</div>

返回一个具有指定结构的表，可用于从指定的 Delta Lake 表读取数据或向其中写入数据。

<div id="examples">
  ## 示例
</div>

<div id="reading-data">
  ### 读取数据
</div>

假设 S3 存储中有一个表，位于 `https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/`。
要读取 ClickHouse 中该表的数据，请运行：

```sql title="Query"
SELECT
    URL,
    UserAgent
FROM deltaLake('https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/')
WHERE URL IS NOT NULL
LIMIT 2
```

```response title="Response"
┌─URL───────────────────────────────────────────────────────────────────┬─UserAgent─┐
│ http://auto.ria.ua/search/index.kz/jobinmoscow/detail/55089/hasimages │         1 │
│ http://auto.ria.ua/search/index.kz/jobinmoscow.ru/gosushi             │         1 │
└───────────────────────────────────────────────────────────────────────┴───────────┘
```

<div id="inserting-data">
  ### 插入数据
</div>

假设在 `s3://ch-docs-s3-bucket/people_10k/` 的 S3 存储中有一张表。
Delta Lake 写入是一项 Beta 功能，默认处于禁用状态。请按以下方式启用 (`allow_delta_lake_writes` 从 26.7 版本开始可用；在更早的版本中，请使用 `allow_experimental_delta_lake_writes`) ：

```sql title="Query"
SET allow_delta_lake_writes=1
```

然后输入：

```sql title="Query"
INSERT INTO TABLE FUNCTION deltaLake('s3://ch-docs-s3-bucket/people_10k/', '<access_key>', '<secret>') VALUES (10001, 'John', 'Smith', 'Male', 30)
```

```response title="Response"
Query id: 09069b47-89fa-4660-9e42-3d8b1dde9b17

Ok.

1 row in set. Elapsed: 3.426 sec.
```

你可以再次读取该表，以确认 insert 已成功：

```sql title="Query"
SELECT *
FROM deltaLake('s3://ch-docs-s3-bucket/people_10k/', '<access_key>', '<secret>')
WHERE (firstname = 'John') AND (lastname = 'Smith')
```

```response title="Response"
Query id: 65032944-bed6-4d45-86b3-a71205a2b659

   ┌────id─┬─firstname─┬─lastname─┬─gender─┬─age─┐
1. │ 10001 │ John      │ Smith    │ Male   │  30 │
   └───────┴───────────┴──────────┴────────┴─────┘
```

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_path` — 文件路径。类型：`LowCardinality(String)`。
* `_file` — 文件名。类型：`LowCardinality(String)`。
* `_size` — 文件大小 (单位：字节) 。类型：`Nullable(UInt64)`。如果文件大小未知，则该值为 `NULL`。
* `_time` — 文件的最后修改时间。类型：`Nullable(DateTime)`。如果时间未知，则该值为 `NULL`。
* `_etag` — 文件的 etag。类型：`LowCardinality(String)`。如果 etag 未知，则该值为 `NULL`。

<div id="related">
  ## 相关
</div>

* [DeltaLake 引擎](/zh/engines/table-engines/integrations/deltalake.md)
* [DeltaLake cluster 表函数](/zh/sql-reference/table-functions/deltalakeCluster.md)