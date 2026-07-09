---
description: '为存储在亚马逊 S3、Azure、HDFS 或本地的 Apache Iceberg 表提供只读的类表接口。'
sidebar_label: 'iceberg'
sidebar_position: 90
slug: /sql-reference/table-functions/iceberg
title: 'iceberg'
doc_type: 'reference'
---

为存储在亚马逊 S3、Azure、HDFS 或本地的 Apache [Iceberg](https://iceberg.apache.org/) 表提供只读的类表接口。

<div id="syntax">
  ## 语法
</div>

```sql
icebergS3(url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method] [,extra_credentials])
icebergS3(named_collection[, option=value [,..]])

icebergAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzure(named_collection[, option=value [,..]])

icebergHDFS(path_to_table, [,format] [,compression_method])
icebergHDFS(named_collection[, option=value [,..]])

icebergLocal(path_to_table, [,format] [,compression_method])
icebergLocal(named_collection[, option=value [,..]])
```

<div id="arguments">
  ## 参数
</div>

这些参数的说明分别与表函数 `s3`、`azureBlobStorage`、`HDFS` 和 `file` 中对应参数的说明一致。
`format` 表示 Iceberg 表中数据文件的格式。

对于 `icebergS3`，可使用可选参数 `extra_credentials` 传递用于基于角色的访问控制的 `role_arn` (在 ClickHouse Cloud 中) 。有关配置步骤，请参见 [Secure S3](/zh/cloud/data-sources/secure-s3)。

<div id="returned-value">
  ### 返回值
</div>

具有指定结构的表，用于读取指定 Iceberg 表中的数据。

<div id="example">
  ### 示例
</div>

```sql
SELECT * FROM icebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

:::important
ClickHouse 目前支持通过 `icebergS3`、`icebergAzure`、`icebergHDFS` 和 `icebergLocal` 表函数，以及 `IcebergS3`、`icebergAzure`、`IcebergHDFS` 和 `IcebergLocal` 表引擎读取 Iceberg v1 和 v2 格式。
:::

<div id="defining-a-named-collection">
  ## 定义命名集合
</div>

以下示例展示了如何配置一个用于存储 URL 和凭据的命名集合：

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
            <format>auto</format>
            <structure>auto</structure>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM icebergS3(iceberg_conf, filename = 'test_table')
DESCRIBE icebergS3(iceberg_conf, filename = 'test_table')
```

<div id="iceberg-writes-catalogs">
  ## 使用数据目录
</div>

Iceberg 表也可以与各种数据目录配合使用，例如 [REST Catalog](https://iceberg.apache.org/rest-catalog-spec/)、[AWS Glue Data Catalog](https://docs.aws.amazon.com/prescriptive-guidance/latest/serverless-etl-aws-glue/aws-glue-data-catalog.html) 和 [Unity Catalog](https://www.unitycatalog.io/)。

:::important
使用目录时，大多数用户都会希望使用 `DataLakeCatalog` 数据库引擎，它会将 ClickHouse 连接到您的目录以发现其中的表。您可以使用该数据库引擎，而无需通过 `IcebergS3` 表引擎手动逐个创建表。
:::

要使用它们，请使用 `IcebergS3` 引擎创建一个表，并提供所需的设置。

例如，将 REST Catalog 与 MinIO 存储配合使用：

```sql
CREATE TABLE `database_name.table_name`
ENGINE = IcebergS3(
  'http://minio:9000/warehouse-rest/table_name/',
  'minio_access_key',
  'minio_secret_key'
)
```

或者，使用 AWS Glue 数据目录和 S3：

```sql
CREATE TABLE `my_database.my_table`  
ENGINE = IcebergS3(
  's3://my-data-bucket/warehouse/my_database/my_table/',
  'aws_access_key',
  'aws_secret_key'
)
```

<div id="schema-evolution">
  ## schema 演进
</div>

目前，借助 CH，你可以读取 schema 会随时间变化的 Iceberg 表。当前支持读取发生过以下变化的表：新增或删除列，以及列顺序发生变化。你还可以将原本必须有值的列更改为允许 NULL 的列。此外，我们还支持简单类型之间允许的类型转换，即：  

* int -&gt; long
* float -&gt; double
* decimal(P, S) -&gt; decimal(P&#39;, S) where P&#39; &gt; P.

目前，还不支持更改嵌套结构，或更改数组和 Map 中元素的类型。

<div id="partition-pruning">
  ## 分区裁剪
</div>

ClickHouse 支持在针对 Iceberg 表的 SELECT 查询中进行分区裁剪，这有助于通过跳过无关的数据文件来优化查询性能。要启用分区裁剪，请设置 `use_iceberg_partition_pruning = 1`。有关 Iceberg 分区裁剪的更多信息，请参阅 https://iceberg.apache.org/spec/#partitioning

<div id="time-travel">
  ## 时间旅行
</div>

ClickHouse 支持 Iceberg 表的时间旅行功能，使您能够通过特定时间戳或快照 ID 查询历史数据。

<div id="deleted-rows">
  ## 含已删除行的表的处理
</div>

目前，仅支持带有[位置删除](https://iceberg.apache.org/spec/#position-delete-files)的 Iceberg 表。

以下删除方法**暂不支持**：

* [相等删除](https://iceberg.apache.org/spec/#equality-delete-files)
* [删除向量](https://iceberg.apache.org/spec/#deletion-vectors) (在 v3 中引入)

<div id="basic-usage">
  ### 基本用法
</div>

```sql
 SELECT * FROM example_table ORDER BY 1 
 SETTINGS iceberg_timestamp_ms = 1714636800000
```

```sql
 SELECT * FROM example_table ORDER BY 1 
 SETTINGS iceberg_snapshot_id = 3547395809148285433
```

注意：不能在同一查询中同时指定 `iceberg_timestamp_ms` 和 `iceberg_snapshot_id` 参数。

<div id="important-considerations">
  ### 重要注意事项
</div>

* **快照**通常会在以下情况下创建：

* 有新数据写入表时

* 执行某种数据合并整理时

* **schema 变更通常不会创建快照**——这会导致在对经历过 schema 演进的表使用时间旅行时出现一些需要特别注意的行为。

<div id="example-scenarios">
  ### 示例场景
</div>

所有场景均使用 Spark 编写，因为 CH 目前尚不支持写入 Iceberg 表。

<div id="scenario-1">
  #### 场景 1：无新快照时的 schema 变更
</div>

考虑以下操作顺序：

```sql
 -- Create a table with two columns
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

- - Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES 
    (1, 'Mars')

  ts1 = now() // A piece of pseudo code

- - Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)
 
  ts2 = now()

- - Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES (2, 'Venus', 100)

   ts3 = now()

- - Query the table at each timestamp
  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts1;

+------------+------------+
|order_number|product_code|
+------------+------------+
|           1|        Mars|
+------------+------------+
  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts2;

+------------+------------+
|order_number|product_code|
+------------+------------+
|           1|        Mars|
+------------+------------+

  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts3;

+------------+------------+-----+
|order_number|product_code|price|
+------------+------------+-----+
|           1|        Mars| NULL|
|           2|       Venus|100.0|
+------------+------------+-----+
```

不同时间戳对应的查询结果：

* 在 ts1 和 ts2：仅显示最初的两列
* 在 ts3：显示全部三列，其中第一行的 `price` 为 NULL

<div id="scenario-2">
  #### 场景 2：历史 schema 与当前 schema 的差异
</div>

在当前时刻执行时间旅行查询时，显示出的 schema 可能与当前表的 schema 不同：

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_2 (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

-- Insert initial data into the table
  INSERT INTO spark_catalog.db.time_travel_example_2 VALUES (2, 'Venus');

-- Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example_2 ADD COLUMN (price double);

  ts = now();

-- Query the table at a current moment but using timestamp syntax

  SELECT * FROM spark_catalog.db.time_travel_example_2 TIMESTAMP AS OF ts;

    +------------+------------+
    |order_number|product_code|
    +------------+------------+
    |           2|       Venus|
    +------------+------------+

-- Query the table at a current moment
  SELECT * FROM spark_catalog.db.time_travel_example_2;
    +------------+------------+-----+
    |order_number|product_code|price|
    +------------+------------+-----+
    |           2|       Venus| NULL|
    +------------+------------+-----+
```

发生这种情况，是因为 `ALTER TABLE` 不会创建新的 快照；而对于当前表，Spark 读取 `schema_id` 的值时，取自最新的 元数据文件，而不是某个 快照。

<div id="scenario-3">
  #### 场景 3：历史 schema 与当前 schema 的差异
</div>

第二种情况是，在进行时间旅行时，你无法获取该表在写入任何数据之前的状态：

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_3 (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2');

  ts = now();

-- Query the table at a specific timestamp
  SELECT * FROM spark_catalog.db.time_travel_example_3 TIMESTAMP AS OF ts; -- Finises with error: Cannot find a snapshot older than ts.
```

在 ClickHouse 中，这种行为与 Spark 保持一致。你可以把其中的 Spark Select 查询理解为 ClickHouse Select 查询，效果是一样的。

<div id="metadata-file-resolution">
  ## 元数据文件解析
</div>

在 ClickHouse 中使用 `iceberg` 表函数时，系统需要找到用于描述 Iceberg 表结构的正确 `metadata.json` 文件。以下说明这一解析过程的工作原理：

<div id="candidate-search">
  ### 候选项搜索 (按优先顺序)
</div>

1. **直接指定路径**：
   *如果你设置了 `iceberg_metadata_file_path`，系统会将其与 Iceberg 表目录路径拼接，使用该精确路径。

* 一旦提供此设置，其他所有解析设置都会被忽略。

2. **表 UUID 匹配**：
   *如果指定了 `iceberg_metadata_table_uuid`，系统将：
   * 仅检查 `metadata` 目录中的 `.metadata.json` 文件
   * 筛选出包含 `table-uuid` 字段且与指定 UUID 匹配的文件 (不区分大小写)

3. **默认搜索**：
   *如果上述两个设置都未提供，则 `metadata` 目录中的所有 `.metadata.json` 文件都会成为候选项

<div id="most-recent-file">
  ### 选择最新的文件
</div>

根据上述规则识别出候选文件后，系统会进一步确定其中哪个文件最新：

* 如果启用了 `iceberg_recent_metadata_file_by_last_updated_ms_field`：

* 选择 `last-updated-ms` 值最大的文件

* 否则：

* 选择版本号最高的文件

* (对于格式为 `V.metadata.json` 或 `V-uuid.metadata.json` 的文件名，其中 `V` 表示版本号)

**注意**：上述所有设置都是表函数设置 (而非全局设置或查询级别设置) ，必须按如下所示指定：

```sql
SELECT * FROM iceberg('s3://bucket/path/to/iceberg_table', 
    SETTINGS iceberg_metadata_table_uuid = 'a90eed4c-f74b-4e5b-b630-096fb9d09021');
```

**注意**：虽然 Iceberg 目录通常负责进行元数据解析，但 ClickHouse 中的 `iceberg` 表函数会直接将存储在 S3 中的文件解析为 Iceberg 表，因此理解这些解析规则非常重要。

<div id="metadata-cache">
  ## 元数据缓存
</div>

`Iceberg` 表引擎和表函数支持元数据缓存，可存储 manifest 文件、manifest 列表和元数据 JSON 的相关信息。缓存存储在内存中。此功能由设置 `use_iceberg_metadata_files_cache` 控制，且默认启用。

<div id="aliases">
  ## 别名
</div>

表函数 `iceberg` 现为 `icebergS3` 的别名。

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_path` — 文件路径。类型：`LowCardinality(String)`。
* `_file` — 文件名。类型：`LowCardinality(String)`。
* `_size` — 文件大小 (字节) 。类型：`Nullable(UInt64)`。如果文件大小未知，则值为 `NULL`。
* `_time` — 文件的最后修改时间。类型：`Nullable(DateTime)`。如果时间未知，则值为 `NULL`。
* `_etag` — 文件的 etag。类型：`LowCardinality(String)`。如果 etag 未知，则值为 `NULL`。

<div id="writes-into-iceberg-table">
  ## 写入 Iceberg 表
</div>

从 25.7 版本开始，ClickHouse 支持对用户的 Iceberg 表进行修改。

目前这是一项 Experimental 功能，因此你需要先启用它：

```sql
SET allow_insert_into_iceberg = 1;
```

<div id="create-iceberg-table">
  ### 创建表
</div>

要创建自己的空 Iceberg 表，请使用与读取时相同的命令，但需要显式指定 schema。
写入支持 Iceberg 规范中的所有数据格式，例如 Parquet、Avro 和 ORC。

<div id="example">
  ### 示例
</div>

```sql
CREATE TABLE iceberg_writes_example
(
    x Nullable(String),
    y Nullable(Int32)
)
ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/')
```

注意：要创建 version hint 文件，请启用 `iceberg_use_version_hint` 设置。
如果要压缩 metadata.json 文件，请在 `iceberg_metadata_compression_method` 设置中指定压缩 codec 的名称。

<div id="writes-inserts">
  ### INSERT
</div>

创建新表后，您可以使用标准的 ClickHouse 语法插入数据。

<div id="example">
  ### 示例
</div>

```sql
INSERT INTO iceberg_writes_example VALUES ('Pavel', 777), ('Ivanov', 993);

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Pavel
y: 777

Row 2:
──────
x: Ivanov
y: 993
```

<div id="iceberg-writes-delete">
  ### DELETE
</div>

ClickHouse 也支持在 merge-on-read 格式下删除多余的行。
该查询会创建一个带有位置删除文件的新快照。

<div id="example">
  ### 示例
</div>

```sql
ALTER TABLE iceberg_writes_example DELETE WHERE x != 'Ivanov';

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

<div id="iceberg-writes-schema-evolution">
  ### schema 演进
</div>

ClickHouse 允许你对简单类型的列 (不包括 Tuple、Array 和 Map) 进行添加、删除、修改或重命名。

<div id="example">
  ### 示例
</div>

```sql
ALTER TABLE iceberg_writes_example MODIFY COLUMN y Nullable(Int64);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

ALTER TABLE iceberg_writes_example ADD COLUMN z Nullable(Int32);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64),                                 ↴│
   │↳    `z` Nullable(Int32)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
z: ᴺᵁᴸᴸ

ALTER TABLE iceberg_writes_example DROP COLUMN z;
SHOW CREATE TABLE iceberg_writes_example;
   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993

ALTER TABLE iceberg_writes_example RENAME COLUMN y TO value;
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `value` Nullable(Int64)                              ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
value: 993
```

<div id="iceberg-writes-compaction">
  ### 合并整理
</div>

ClickHouse 支持对 Iceberg 表执行合并整理。目前，它可以在更新元数据的同时，将位置删除文件合并到数据文件中。先前的快照 ID 和时间戳保持不变，因此仍可使用相同的值进行时间旅行查询。

使用方法：

```sql
SET allow_experimental_iceberg_compaction = 1

OPTIMIZE TABLE iceberg_writes_example;

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

<div id="iceberg-expire-snapshots">
  ### 清理过期快照
</div>

Iceberg 表会在每次 INSERT、DELETE 或 UPDATE 操作时累积快照。随着时间推移，这可能会产生大量快照及其关联的数据文件。`expire_snapshots` 命令会删除旧快照，并清理不再被任何保留快照引用的数据文件。

**语法：**

```sql
ALTER TABLE iceberg_table EXECUTE expire_snapshots(
    ['timestamp']
    [, expire_before = 'timestamp']
    [, retention_period = '3d']
    [, retain_last = 100]
    [, snapshot_ids = [1, 2, 3, 4]]
    [, dry_run = 1]
);
```

默认情况下，保留哪些快照由[保留策略](#iceberg-snapshot-retention-policy)决定 (表属性 `min-snapshots-to-keep`、`max-snapshot-age-ms` 以及各 ref 的覆盖设置) 。指定 &#96;snapshot&#95;ids&#96;&#96; 时，将绕过保留策略，只对列出的快照进行过期处理。

**参数：**

* `'timestamp'` (位置参数) 或 `expire_before = 'timestamp'` —— 按**服务器时区**解释的日期时间字符串 (例如 `'2024-06-01 00:00:00'`) 。它相当于一个安全阈值：`timestamp-ms` 等于或晚于该值的快照都会受到保护，不会过期，即使按保留策略原本应被过期处理也是如此。可与 `snapshot_ids` 结合使用；在这种情况下，列表中等于或晚于该时间戳的快照不会过期。
* `retention_period = '<duration>'` —— 仅对此次调用覆盖表级 `history.expire.max-snapshot-age-ms`。早于该时长的快照 (从当前时间起计算) 会成为过期候选。该值是一个时长字符串，由一个或多个连续拼接的 `{number}{unit}` 对组成。支持的单位：`y` (365 天) 、`w` (7 天) 、`d` (24 小时) 、`h` (60 分钟) 、`m` (60 秒) 、`s` (1 秒) 、`ms` (1 毫秒) 。单位可以组合，例如 `'3d'`、`'12h'`、`'1d12h30m'`、`'500ms'`。
* `retain_last = N` —— 仅对此次调用覆盖表级 `history.expire.min-snapshots-to-keep`。无论快照有多旧，始终至少保留 `N` 个快照。
* `snapshot_ids = [id1, id2, ...]` —— 仅过期列出的这些快照 ID (当前快照、分支或标签引用的快照除外) 。此模式会完全绕过保留策略，且不能与 `retention_period` 或 `retain_last` 组合使用。
* `dry_run = 1` —— 计算将会过期的内容并返回指标，但不会写入新元数据或删除文件。

:::note
`retention_period` 和 `retain_last` 只会覆盖**表级**保留默认值。在 Iceberg 表属性中配置的各 ref (分支/标签) 保留覆盖设置 (例如 `refs.<branch>.min-snapshots-to-keep`) 绝不会被覆盖——它们始终按表元数据中的配置生效。
:::

**示例：**

```sql
SET allow_insert_into_iceberg = 1;

-- Create some snapshots by inserting data
INSERT INTO iceberg_table VALUES (1);
INSERT INTO iceberg_table VALUES (2);
INSERT INTO iceberg_table VALUES (3);

-- Expire using retention policy only
ALTER TABLE iceberg_table EXECUTE expire_snapshots();

-- Expire with a safety fuse: protect snapshots newer than the timestamp (positional syntax)
ALTER TABLE iceberg_table EXECUTE expire_snapshots('2025-01-01 00:00:00');

-- Same using the named argument form
ALTER TABLE iceberg_table EXECUTE expire_snapshots(expire_before = '2025-01-01 00:00:00');

-- Override retention parameters for one execution
ALTER TABLE iceberg_table EXECUTE expire_snapshots(retention_period = '3d', retain_last = 10);

-- Expire explicit snapshots
ALTER TABLE iceberg_table EXECUTE expire_snapshots(snapshot_ids = [101, 102, 103]);

-- Dry-run preview (no metadata updates, no file deletes)
ALTER TABLE iceberg_table EXECUTE expire_snapshots(retention_period = '1d', dry_run = 1);
```

**输出：**

该命令会返回一个包含两列 (`metric_name String`、`metric_value Int64`) 的表，其中每个指标对应一行。指标名称遵循 [Iceberg 规范](https://iceberg.apache.org/docs/latest/spark-procedures/#output)：

| metric&#95;name                       | 描述                              |
| ------------------------------------- | ------------------------------- |
| `deleted_data_files_count`            | 已删除的数据文件数量                      |
| `deleted_position_delete_files_count` | 已删除的位置删除文件数量                    |
| `deleted_equality_delete_files_count` | 已删除的等值删除文件数量                    |
| `deleted_manifest_files_count`        | 已删除的 manifest 文件数量              |
| `deleted_manifest_lists_count`        | 已删除的 manifest 列表文件数量            |
| `deleted_statistics_files_count`      | 已删除的 statistics 文件数量 (当前始终为 0)  |
| `dry_run`                             | `1` 表示 `dry-run` 模式，`0` 表示正常执行  |

该命令执行以下步骤：

1. 评估保留策略 (见下文) ，以确定必须保留哪些快照
2. 如果提供了时间戳参数，则额外保护该时间戳及之后的所有快照
3. 将既未被策略保留、也未受时间戳保护机制保护的快照设为过期
4. 计算哪些文件仅与已过期的快照相关
5. 在正常模式下：生成不包含已过期快照的新元数据
6. 在正常模式下：物理删除不可达的 manifest 列表、manifest 文件和数据文件
7. 在 `dry_run = 1` 模式下：跳过步骤 5 和 6，仅返回计算出的指标

<div id="iceberg-snapshot-retention-policy">
  #### 快照保留策略
</div>

`expire_snapshots` 命令遵循 [Iceberg 快照保留策略](https://iceberg.apache.org/spec/#snapshot-retention-policy)。保留规则通过 Iceberg 表属性以及按引用设置的覆盖项进行配置：

| 属性                                     | 范围 | 默认值                                                                  | 说明                                |
| -------------------------------------- | -- | -------------------------------------------------------------------- | --------------------------------- |
| `history.expire.min-snapshots-to-keep` | 表  | `iceberg_expire_default_min_snapshots_to_keep` (默认值为 `1`)            | 每个分支祖先链中至少要保留的快照数量                |
| `history.expire.max-snapshot-age-ms`   | 表  | `iceberg_expire_default_max_snapshot_age_ms` (默认值为 `432000000`，5 天)  | 分支中可保留快照的最大时长 (毫秒)                |
| `history.expire.max-ref-age-ms`        | 表  | `iceberg_expire_default_max_ref_age_ms` (默认值为 `∞`)                   | 快照引用 (分支或标签) 在被移除前允许保留的最大时长 (毫秒)  |

每个快照引用 (Iceberg 元数据中的 `refs`) 都可以通过按引用字段覆盖这些设置：`min-snapshots-to-keep`、`max-snapshot-age-ms` 和 `max-ref-age-ms`。

**保留规则评估：**

* **对于每个分支** (包括 `main`) ：从分支 head 开始沿祖先链遍历。满足以下任一条件时，快照会被保留：
  * 该快照位于祖先链前 `min-snapshots-to-keep` 个快照之内
  * 该快照的存在时长未超过 `max-snapshot-age-ms` (即 `now - timestamp-ms <= max-snapshot-age-ms`)
* **对于标签**：被标记的快照会被保留，除非该标签已超过其 `max-ref-age-ms`，此时会移除该标签引用
* **非 `main` 引用** 如果其存在时长超过 `max-ref-age-ms`，则会被完全移除 (`main` 分支永远不会被移除)
* **悬空引用** (指向不存在快照的引用) 会在发出警告后被移除
* **当前快照始终会被保留**，无论保留设置如何

**所需特权：**

需要 `ALTER TABLE EXECUTE` 特权，它在 ClickHouse 访问控制层级中是 `ALTER TABLE` 的子级。你可以单独授予该特权，也可以通过父级授予：

```sql
-- Grant only EXECUTE permission
GRANT ALTER TABLE EXECUTE ON my_iceberg_table TO my_user;

-- Or grant all ALTER TABLE permissions (includes ALTER TABLE EXECUTE)
GRANT ALTER TABLE ON my_iceberg_table TO my_user;
```

:::note

* 仅支持 Iceberg format version 2 表 (v1 快照 不保证提供安全识别待清理文件所需的 `manifest-list`)
* current 快照 始终会被保留，即使它早于指定的 timestamp
* 要求启用 `allow_insert_into_iceberg` setting
* 要求启用 `allow_experimental_expire_snapshots` setting
* 当 ClickHouse 更新 metadata 时，目录 自身的授权 (REST 目录 auth、AWS Glue IAM 等) 仍会独立生效
  :::

<div id="iceberg-remove-orphan-files">
  ### 移除孤立文件
</div>

孤立文件是指存储中存在、但未被 Iceberg 表元数据中的任何快照引用的文件。它们可能因写入失败、合并整理后的部分清理以及操作中断而不断累积，导致存储无限增长。`remove_orphan_files` 命令用于识别并移除这些孤立文件。

**语法：**

```sql
-- Positional form: single unnamed older_than argument
ALTER TABLE iceberg_table EXECUTE remove_orphan_files('timestamp')

-- Named form
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    older_than = 'timestamp',
    location = 'path',
    dry_run = 0|1
)

-- No arguments: use all defaults (older_than = 3 days ago)
ALTER TABLE iceberg_table EXECUTE remove_orphan_files()
```

**参数：**

| 参数           | 类型              | 默认值                                                      | 描述                                                  |
| ------------ | --------------- | -------------------------------------------------------- | --------------------------------------------------- |
| `older_than` | `String` (时间戳)  | 3 天前 (可通过 `iceberg_orphan_files_older_than_seconds` 配置)  | 仅将最后修改时间早于此时间戳的文件视为可能的孤立文件。这样可避免删除仍在进行中的写入所产生的文件。   |
| `location`   | `String`        | 表位置                                                      | 将扫描范围限制为表位置下的特定子目录 (例如 `'data/'` 或 `'metadata/'`) 。 |
| `dry_run`    | `UInt64`        | `0`                                                      | 当值为 `1` 时，仅识别孤立文件并返回结果摘要，不会实际删除任何内容。                |

**示例：**

```sql
-- Remove orphan files older than a specific timestamp
ALTER TABLE iceberg_table EXECUTE remove_orphan_files('2026-03-01 00:00:00');

-- Dry run: preview which files would be deleted
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(dry_run = 1);

-- Scan only the data directory
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    older_than = '2026-03-01 00:00:00',
    location = 'data/'
);

-- Combine positional older_than with named arguments
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    '2026-03-01 00:00:00',
    dry_run = 1
);
```

**输出：**

该命令会返回一个表，其中包含 `metric_name` 和 `metric_value` 列，用于按类别显示已删除文件的数量 (在 dry&#95;run 模式下则显示将要删除的文件数量) 。文件类别会根据文件命名约定，采用尽力而为的启发式方法进行分类；未匹配任何特定模式的文件默认归类为 `deleted_data_files_count`：

| metric&#95;name                                     | metric&#95;value |
| --------------------------------------------------- | ---------------- |
| deleted&#95;data&#95;files&#95;count                | 5                |
| deleted&#95;position&#95;delete&#95;files&#95;count | 2                |
| deleted&#95;equality&#95;delete&#95;files&#95;count | 0                |
| deleted&#95;manifest&#95;files&#95;count            | 3                |
| deleted&#95;manifest&#95;lists&#95;count            | 1                |
| deleted&#95;metadata&#95;files&#95;count            | 0                |
| deleted&#95;statistics&#95;files&#95;count          | 0                |
| skipped&#95;missing&#95;metadata&#95;count          | 0                |
| failed&#95;deletions&#95;count                      | 0                |

**设置：**

| Setting                                   | Type     | Default           | Description                        |
| ----------------------------------------- | -------- | ----------------- | ---------------------------------- |
| `allow_iceberg_remove_orphan_files`       | `Bool`   | `false`           | 用于启用该功能的门控设置 (Experimental) 。      |
| `iceberg_orphan_files_older_than_seconds` | `UInt64` | `259200` (3 days) | 省略该参数时，`older_than` 的默认阈值 (单位为秒) 。 |

:::note

* **要求使用 Iceberg format version 2 (或更高版本) 。** Version 1 表会被拒绝，因为其快照中缺少 `manifest-list` 指针，而安全确定可达文件集合需要这些指针。对 v1 表运行该命令会返回 `BAD_ARGUMENTS` 错误。
* 需要同时启用 `allow_insert_into_iceberg` 和 `allow_iceberg_remove_orphan_files` 设置
* 建议先运行 `expire_snapshots`，再运行 `remove_orphan_files`，以便优先清理由已过期快照唯一引用的文件
* 使用 `dry_run = 1` 可在删除前预览孤立文件
* `older_than` 阈值可防止删除仍在进行中的写入所产生的文件——默认 3 天的阈值提供了较充足的安全余量
  :::

<div id="see-also">
  ## 另请参阅
</div>

* [Iceberg 引擎](/zh/engines/table-engines/integrations/iceberg.md)
* [Iceberg cluster 表函数](/zh/sql-reference/table-functions/icebergCluster.md)