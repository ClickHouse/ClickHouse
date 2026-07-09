---
description: '此引擎为现有的 Apache Iceberg
  表提供只读集成支持，这些表存储在亚马逊 S3、Azure、HDFS 以及本地环境中。'
sidebar_label: 'Iceberg'
sidebar_position: 90
slug: /engines/table-engines/integrations/iceberg
title: 'Iceberg 表引擎'
doc_type: 'reference'
---

:::warning
我们建议在 ClickHouse 中处理 Iceberg 数据时使用 [Iceberg Table Function](/zh/sql-reference/table-functions/iceberg.md)。当前，Iceberg Table Function 已提供足够的功能，可为 Iceberg 表提供有限的只读接口。

Iceberg Table Engine 已可用，但可能存在一些限制。ClickHouse 最初并不是为支持 schema 会被外部更改的表而设计的，这可能会影响 Iceberg Table Engine 的功能。因此，一些适用于常规表的特性可能无法使用，或无法正常工作，尤其是在使用旧版 analyzer 时。

为获得最佳兼容性，建议在我们持续改进 Iceberg Table Engine 支持期间，使用 Iceberg Table Function。
:::

此引擎为现有的 Apache [Iceberg](https://iceberg.apache.org/) 表提供只读集成支持，这些表存储在亚马逊 S3、Azure、HDFS 以及本地环境中。

<div id="create-table">
  ## 创建表
</div>

请注意，存储中必须已存在 Iceberg 表；此命令不接受用于创建新表的 DDL 参数。

```sql
CREATE TABLE iceberg_table_s3
    ENGINE = IcebergS3(url,  [, NOSIGN | access_key_id, secret_access_key, [session_token]], format, [,compression], [,extra_credentials])

CREATE TABLE iceberg_table_azure
    ENGINE = IcebergAzure(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])

CREATE TABLE iceberg_table_hdfs
    ENGINE = IcebergHDFS(path_to_table, [,format] [,compression_method])

CREATE TABLE iceberg_table_local
    ENGINE = IcebergLocal(path_to_table, [,format] [,compression_method])
```

<div id="engine-arguments">
  ## 引擎参数
</div>

这些参数的说明分别与 `S3`、`AzureBlobStorage`、`HDFS` 和 `File` 表引擎中对应参数的说明一致。
`format` 表示 Iceberg 表中数据文件的格式。

对于 `IcebergS3`，可使用可选参数 `extra_credentials` 传入 `role_arn`，以便在 ClickHouse Cloud 中实现基于角色的访问控制。有关配置步骤，请参见 [Secure S3](/zh/cloud/data-sources/secure-s3)。

也可以使用[命名集合](../../../operations/named-collections.md)来指定引擎参数。

<div id="example">
  ### 示例
</div>

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

使用命名集合：

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3(iceberg_conf, filename = 'test_table')

```

<div id="aliases">
  ## 别名
</div>

`Iceberg` 表引擎会根据 `disk` 设置自动检测存储后端，并相应使用 `IcebergS3`、`IcebergAzure` 或 `IcebergLocal`。如果未指定 `disk`，则默认使用 `IcebergS3` 实现。

<div id="data-types">
  ## 数据类型
</div>

下表展示了在 schema 推断过程中 (用于读取时) ，Iceberg 数据类型与 ClickHouse 数据类型之间的映射关系。

<div id="primitive-types">
  ### 基本类型
</div>

| Iceberg 类型         | ClickHouse 类型          | 说明                             |
| ------------------ | ---------------------- | ------------------------------ |
| `boolean`          | `Bool`                 |                                |
| `int`              | `Int32`                |                                |
| `long`, `bigint`   | `Int64`                |                                |
| `float`            | `Float32`              |                                |
| `double`           | `Float64`              |                                |
| `date`             | `Date32`               |                                |
| `time`             | `Int64`                | 自午夜开始的微秒数                      |
| `timestamp`        | `DateTime64(6)`        | 微秒，无时区                         |
| `timestamptz`      | `DateTime64(6, 'UTC')` | 微秒，UTC 时区                      |
| `timestamp_ns`     | `DateTime64(9)`        | 纳秒，无时区 (仅自 Iceberg v3 起支持)     |
| `timestamptz_ns`   | `DateTime64(9, 'UTC')` | 纳秒，UTC 时区 (仅自 Iceberg v3 起支持)  |
| `string`, `binary` | `String`               |                                |
| `uuid`             | `UUID`                 |                                |
| `fixed(N)`         | `FixedString(N)`       |                                |
| `decimal(P, S)`    | `Decimal(P, S)`        |                                |

<div id="complex-types">
  ### 复合类型
</div>

| Iceberg 类型 | ClickHouse 类型 |
| ---------- | ------------- |
| `list`     | `Array`       |
| `map`      | `Map`         |
| `struct`   | `Tuple`       |

<div id="schema-evolution">
  ## Schema 演化
</div>

ClickHouse 支持读取 schema 随时间发生演化的 Iceberg 表。这包括添加、删除或重排列后的表，以及列从必填变为 Nullable 的表。此外，还支持以下类型转换：

* int -&gt; long
* float -&gt; double
* decimal(P, S) -&gt; decimal(P&#39;, S)，其中 P&#39; &gt; P。

目前，尚不支持更改嵌套结构，或更改数组和 Map 中元素的类型。

如果某个表是在启用动态 schema 推断的情况下创建的，而其 schema 在创建后又发生了变化，要读取该表，请在创建表时设置 allow&#95;dynamic&#95;metadata&#95;for&#95;data&#95;lakes = true。

<div id="partition-pruning">
  ## 分区裁剪
</div>

ClickHouse 支持在对 Iceberg 表执行 SELECT 查询时进行分区裁剪，这可以通过跳过无关的数据文件来优化查询性能。要启用分区裁剪，请设置 `use_iceberg_partition_pruning = 1`。有关 Iceberg 分区裁剪的更多信息，请参阅 https://iceberg.apache.org/spec/#partitioning

<div id="time-travel">
  ## 时间旅行
</div>

ClickHouse 支持 Iceberg 表的时间旅行，让您可以使用特定的时间戳或快照 ID 查询历史数据。

<div id="deleted-rows">
  ## 包含已删除行的表的处理
</div>

ClickHouse 支持读取采用以下删除方法的 Iceberg 表：

* [位置删除](https://iceberg.apache.org/spec/#position-delete-files)
* [等值删除](https://iceberg.apache.org/spec/#equality-delete-files) (自 25.8+ 版本起支持)

以下删除方法**不支持**：

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

注意：在同一查询中，不能同时指定 `iceberg_timestamp_ms` 和 `iceberg_snapshot_id` 参数。

<div id="important-considerations">
  ### 重要注意事项
</div>

* **快照**通常会在以下情况下创建：
  * 向表中写入新数据时
  * 执行数据合并整理时

* **schema 变更通常不会创建快照** - 这会导致在对经历过 schema 演进的表使用时间旅行时，出现一些需要特别注意的行为。

<div id="example-scenarios">
  ### 示例场景
</div>

以下所有场景均以 Spark 为例，因为 CH 目前尚不支持写入 Iceberg 表。

<div id="scenario-1">
  #### 场景 1：没有新 snapshot 的 schema 变更
</div>

请看以下操作序列：

```sql
 -- Create a table with two columns
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example (
  order_number int, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

-- Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES 
    (1, 'Mars')

  ts1 = now() // A piece of pseudo code

-- Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)
 
  ts2 = now()

-- Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES (2, 'Venus', 100)

   ts3 = now()

-- Query the table at each timestamp
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

不同时间戳下的查询结果：

* 在 ts1 和 ts2：仅显示原始的两列
* 在 ts3：三列都会显示，其中第一行的 price 为 NULL

<div id="scenario-2">
  #### 场景 2：历史 schema 与当前 schema 的差异
</div>

在当前时刻执行时间旅行查询时，看到的 schema 可能与当前表的 schema 不同：

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_2 (
  order_number int, 
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

之所以会这样，是因为 `ALTER TABLE` 不会创建新的快照；而对于当前表，Spark 读取 `schema_id` 的值时依据的是最新的元数据文件，而不是快照。

<div id="scenario-3">
  #### 场景 3：历史 schema 与当前 schema 的差异
</div>

第二种情况是：进行时间旅行时，你无法获取表在尚未写入任何数据之前的状态：

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_3 (
  order_number int, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2');

  ts = now();

-- Query the table at a specific timestamp
  SELECT * FROM spark_catalog.db.time_travel_example_3 TIMESTAMP AS OF ts; -- Finises with error: Cannot find a snapshot older than ts.
```

在 ClickHouse 中，其行为与 Spark 保持一致。你可以把其中的 Spark Select 查询理解为 ClickHouse Select 查询，两者的工作方式完全相同。

<div id="metadata-file-resolution">
  ## 元数据文件定位
</div>

在 ClickHouse 中使用 `Iceberg` 表引擎时，系统需要找到用于描述 Iceberg 表结构的正确 metadata.json 文件。下面说明这一定位过程的工作方式：

<div id="candidate-search">
  ### 候选文件搜索
</div>

1. **直接指定路径**：

* 如果设置了 `iceberg_metadata_file_path`，系统会将其与 Iceberg 表目录路径拼接，使用该精确路径。
* 提供此设置后，所有其他解析设置都会被忽略。

2. **表 UUID 匹配**：

* 如果指定了 `iceberg_metadata_table_uuid`，系统将：
  * 仅查看 `metadata` 目录中的 `.metadata.json` 文件
  * 筛选出包含 `table-uuid` 字段且与指定 UUID 匹配的文件 (不区分大小写)

3. **默认搜索**：

* 如果以上两项设置都未提供，则 `metadata` 目录中的所有 `.metadata.json` 文件都会成为候选项

<div id="most-recent-file">
  ### 选择最新的文件
</div>

在根据上述规则识别出候选文件后，系统会进一步确定其中最新的文件：

* 如果启用了 `iceberg_recent_metadata_file_by_last_updated_ms_field`：
  * 选择 `last-updated-ms` 值最大的文件

* 否则：
  * 选择版本号最高的文件
  * (对于格式为 `V.metadata.json` 或 `V-uuid.metadata.json` 的文件名，版本号显示为 `V`)

**注意**：上文提到的所有设置 (除非另有明确说明) 都是引擎级设置，必须在创建表时指定，如下所示：

```sql
CREATE TABLE example_table ENGINE = Iceberg(
    's3://bucket/path/to/iceberg_table'
) SETTINGS iceberg_metadata_table_uuid = '6f6f6407-c6a5-465f-a808-ea8900e35a38';
```

**注意**：虽然 Iceberg 目录通常负责元数据解析，但 ClickHouse 中的 `Iceberg` 表引擎会直接把存储在 S3 中的文件解析为 Iceberg 表，因此理解这些解析规则很重要。

<div id="data-cache">
  ## 数据缓存
</div>

`Iceberg` 表引擎和表函数支持与 `S3`、`AzureBlobStorage`、`HDFS` 存储相同的数据缓存机制。请参见[此处](../../../engines/table-engines/integrations/s3.md#data-cache)。

<div id="metadata-cache">
  ## 元数据缓存
</div>

`Iceberg` 表引擎和表函数支持元数据缓存，可存储 manifest 文件、manifest 列表和 metadata json 的信息。该缓存存储在内存中。此功能由设置 `use_iceberg_metadata_files_cache` 控制，且默认启用。

<div id="async-metadata-prefetch">
  ## 异步元数据预取
</div>

可在创建 `Iceberg` 表时，通过设置 `iceberg_metadata_async_prefetch_period_ms` 启用异步元数据预取。如果将其设为 0 (默认值) ，或者未启用元数据缓存，则异步预取会被禁用。
要启用此功能，应设置一个非零的毫秒值，表示各次预取周期之间的时间间隔。

启用后，server 将运行一个周期性的后台操作，列出远程 catalog 并检测新的元数据版本。随后会对其进行解析，并递归遍历快照，拉取活动的 manifest 列表文件和 manifest 文件。
元数据缓存中已存在的文件不会被重复下载。每个预取周期结束时，最新的元数据快照即可在元数据缓存中使用。

```sql
CREATE TABLE example_table ENGINE = Iceberg(
    's3://bucket/path/to/iceberg_table'
) SETTINGS
    iceberg_metadata_async_prefetch_period_ms = 60000;
```

为了在读取操作中最大限度地利用异步元数据预取，应将 `iceberg_metadata_staleness_ms` 参数指定为查询参数或会话参数。默认情况下 (0，即未指定) ，在每次查询的上下文中，服务器都会从远程 catalog 拉取最新元数据。
通过指定可接受的元数据过期容忍时间，服务器便可在不访问远程 catalog 的情况下使用缓存中的元数据快照版本。如果缓存中存在元数据版本，且其下载时间位于给定的 staleness window 内，则会使用该版本来处理查询。
否则，将从远程 catalog 拉取最新版本。

```sql
SELECT count() FROM icebench_table WHERE ...
SETTINGS iceberg_metadata_staleness_ms=120000
```

**注意**：异步元数据预取在 `ICEBERG_SCEDULE_POOL` 中运行；它是服务器端的线程池，用于对处于活动状态的 `Iceberg` 表执行后台操作。该线程池的大小由服务器配置参数 `iceberg_background_schedule_pool_size` 控制 (默认值为 10) 。

**注意**：当前预期是，如果启用了异步预取，元数据缓存的大小应足以完整容纳所有活动表的最新元数据快照。

<div id="see-also">
  ## 另请参阅
</div>

* [Iceberg 表函数](/zh/sql-reference/table-functions/iceberg.md)