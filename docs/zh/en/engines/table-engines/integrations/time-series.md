---
description: '一种用于存储时间序列的表引擎，即与时间戳和标签（或标记）相关联的一组值。'
sidebar_label: 'TimeSeries'
sidebar_position: 60
slug: /engines/table-engines/special/time_series
title: 'TimeSeries 表引擎'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="timeseries-table-engine">
  # TimeSeries 表引擎
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

一种存储时间序列的表引擎，即一组与时间戳和标签 (或标记) 相关联的值：

```sql
metric_name1[tag1=value1, tag2=value2, ...] = {timestamp1: value1, timestamp2: value2, ...}
metric_name2[...] = ...
```

:::info
这是一个 Experimental 功能，未来的发行版中可能会发生不向后兼容的更改。
通过 [allow&#95;experimental&#95;time&#95;series&#95;table](/zh/operations/settings/settings#allow_experimental_time_series_table) 设置启用 TimeSeries 表引擎。
执行命令 `set allow_experimental_time_series_table = 1`。
:::

<div id="syntax">
  ## 语法
</div>

```sql
CREATE TABLE name [(columns)] ENGINE=TimeSeries
[SETTINGS var1=value1, ...]
[SAMPLES db.samples_table_name | [SAMPLES INNER COLUMNS (...)] [SAMPLES INNER ENGINE engine(arguments)]]
[TAGS db.tags_table_name | [TAGS INNER COLUMNS (...)] [TAGS INNER ENGINE engine(arguments)]]
[METRICS db.metrics_table_name | [METRICS INNER COLUMNS (...)] [METRICS INNER ENGINE engine(arguments)]]
```

:::note
关键字 `SAMPLES` 保留了别名 `DATA`，以保持向后兼容性。
:::

<div id="usage">
  ## 用法
</div>

建议一开始先全部使用默认设置 (允许在不指定列列表的情况下创建 `TimeSeries` 表) ：

```sql
CREATE TABLE my_table ENGINE=TimeSeries
```

该表随后可用于以下协议 (必须在服务器配置中分配端口) ：

* [prometheus remote-write](/zh/interfaces/prometheus#remote-write)
* [prometheus remote-read](/zh/interfaces/prometheus#remote-read)

<div id="outer-columns">
  ### 外部列
</div>

TimeSeries 表的列是自动生成的。这些列属于外部列，本身不存储数据，仅为 SELECT/INSERT 提供接口。实际数据存储在[目标表](#target-tables)中。以下是外部列列表：

| Name            | Type                                              | Description                                                                                               |
| --------------- | ------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `metric_name`   | `String`                                          | 指标名称                                                                                                      |
| `tags`          | `Map(String, String)`                             | 时间序列的标签映射 (标记)                                                                                            |
| `time_series`   | `Array(Tuple(DateTime64(3), Float64))` by default | 时间序列的 (时间戳、值) 对数组。该 Tuple 的时间戳和标量元素类型可根据样本 `INNER COLUMNS` 声明推导得出 (参见[指定外部列](#specifying-outer-columns))  |
| `metric_family` | `String`                                          | 指标族名称 (用于指标元数据)                                                                                           |
| `type`          | `String`                                          | 指标类型 (例如 &quot;counter&quot;、&quot;gauge&quot;)                                                           |
| `unit`          | `String`                                          | 指标单位                                                                                                      |
| `help`          | `String`                                          | 指标说明                                                                                                      |

示例：

```sql
INSERT INTO my_table (metric_name, tags, time_series) VALUES
    ('cpu_usage', {'job': 'node_exporter', 'instance': 'host1:9100'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 0.5), (toDateTime64('2024-01-01 00:01:00', 3), 0.7)])
```

插入时，`metric_name` 可以为空，这表示指标名称在 `tags` 的 `__name__` 中指定，例如：

```sql
INSERT INTO my_table (tags, time_series) VALUES
    ({'__name__': 'cpu_usage', 'job': 'test'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 0.5)])
```

要插入指标元数据，请将其插入 `metric_family`、`type`、`unit` 和 `help` 列：

```sql
INSERT INTO my_table (metric_name, tags, time_series, metric_family, type, unit, help) VALUES
    ('http_requests_total', {'method': 'GET'}, [(now64(), 100.0)],
     'http_requests_total', 'counter', 'requests', 'Total HTTP requests')
```

<div id="specifying-outer-columns">
  ### 指定外部列
</div>

可以在 `CREATE TABLE` 语句中显式列出外部 `time_series` 列，以覆盖其默认的 `Array(Tuple(DateTime64(3), Float64))` 类型。ClickHouse 会从该 Tuple 中提取时间戳和标量类型，并将它们传递到内部的 samples 表中：

```sql
CREATE TABLE my_table (time_series Array(Tuple(UInt32, Float32))) ENGINE=TimeSeries
```

这相当于直接在 samples 的 `INNER COLUMNS` 子句中声明 timestamp 和 value 列类型：

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp UInt32, value Float32)
```

如果在同一条 `CREATE TABLE` 语句中同时使用这两种形式，声明的类型必须一致。

<div id="target-tables">
  ## 目标表
</div>

`TimeSeries` 表本身不存储数据，所有数据都保存在其目标表中。
这与 [materialized view](../../../sql-reference/statements/create/view#materialized-view) 的工作方式类似，
不同之处在于，materialized view 只有一个目标表，
而 `TimeSeries` 表有三个目标表，分别名为 [samples](#samples-table)、[标签](#tags-table) 和 [metrics](#metrics-table)。

这些目标表既可以在 `CREATE TABLE` 查询中显式指定，
也可以由 `TimeSeries` 表引擎自动生成内部目标表。

插入到 `TimeSeries` 表中的行会被转换、拆分成块，并写入这三个目标表中。

这些目标表如下：

<div id="samples-table">
  ### Samples 表
</div>

*samples* 表包含与某个标识符关联的时间序列。

*samples* 表必须包含以下列：

| 名称          | 必填？ | 默认类型            | 可选类型                  | 描述                 |
| ----------- | --- | --------------- | --------------------- | ------------------ |
| `id`        | [x] | `UUID`          | 任意                    | 标识指标名称和标签的组合       |
| `timestamp` | [x] | `DateTime64(3)` | `DateTime64(X)`       | 一个时间点              |
| `value`     | [x] | `Float64`       | `Float32` 或 `Float64` | 与 `timestamp` 关联的值 |

<div id="tags-table">
  ### 标签表
</div>

*标签* 表包含针对每种指标名称与标签组合计算得到的标识符。

*标签* 表必须包含以下列：

| Name                 | Mandatory? | Default type                          | Possible types                                                                                                        | Description                                                                                          |
| -------------------- | ---------- | ------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `id`                 | [x]        | `UUID`                                | 任意 (必须与[samples](#samples-table)表中 `id` 的类型一致)                                                                             | `id` 用于标识一种指标名称与标签的组合。DEFAULT 表达式指定了如何计算此类标识符                                                        |
| `metric_name`        | [x]        | `LowCardinality(String)`              | `String` 或 `LowCardinality(String)`                                                                                   | 指标名称                                                                                                 |
| `<tag_value_column>` | [ ]        | `String`                              | `String` 或 `LowCardinality(String)` 或 `LowCardinality(Nullable(String))`                                              | 某个特定标签的值。该标签的名称以及对应列的名称在 [tags&#95;to&#95;columns](#settings) 设置中指定                                  |
| `tags`               | [x]        | `Map(LowCardinality(String), String)` | `Map(String, String)` 或 `Map(LowCardinality(String), String)` 或 `Map(LowCardinality(String), LowCardinality(String))` | 标签映射，不包括包含指标名称的 `__name__` 标签，也不包括名称在 [tags&#95;to&#95;columns](#settings) 设置中枚举的标签                  |
| `all_tags`           | [ ]        | `Map(String, String)`                 | `Map(String, String)` 或 `Map(LowCardinality(String), String)` 或 `Map(LowCardinality(String), LowCardinality(String))` | 临时列，每一行都是仅排除包含指标名称的 `__name__` 标签后的全部标签映射。该列的唯一用途是在计算 `id` 时使用                                       |
| `min_time`           | [ ]        | `Nullable(DateTime64(3))`             | `DateTime64(X)` 或 `Nullable(DateTime64(X))`                                                                           | 具有该 `id` 的时间序列的最小时间戳。如果 [store&#95;min&#95;time&#95;and&#95;max&#95;time](#settings) 为 `true`，则会创建此列 |
| `max_time`           | [ ]        | `Nullable(DateTime64(3))`             | `DateTime64(X)` 或 `Nullable(DateTime64(X))`                                                                           | 具有该 `id` 的时间序列的最大时间戳。如果 [store&#95;min&#95;time&#95;and&#95;max&#95;time](#settings) 为 `true`，则会创建此列 |

<div id="metrics-table">
  ### 指标表
</div>

*metrics* 表包含有关已采集指标的一些信息，包括这些指标的类型及其说明。

*metrics* 表必须包含以下列：

| 名称                   | 必填？ | 默认类型                     | 可选类型                                 | 描述                                                                                                                                            |
| -------------------- | --- | ------------------------ | ------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `metric_family_name` | [x] | `String`                 | `String` or `LowCardinality(String)` | 指标族名称                                                                                                                                         |
| `type`               | [x] | `LowCardinality(String)` | `String` or `LowCardinality(String)` | 指标族类型，可取值为 &quot;counter&quot;、&quot;gauge&quot;、&quot;summary&quot;、&quot;stateset&quot;、&quot;histogram&quot;、&quot;gaugehistogram&quot; 之一 |
| `unit`               | [x] | `LowCardinality(String)` | `String` or `LowCardinality(String)` | 指标使用的单位                                                                                                                                       |
| `help`               | [x] | `String`                 | `String` or `LowCardinality(String)` | 指标说明                                                                                                                                          |

<div id="creation">
  ## 创建
</div>

使用 `TimeSeries` 表引擎创建表有多种方式。
最简单的语句

```sql
CREATE TABLE my_table ENGINE=TimeSeries
```

实际上会创建如下表 (可通过执行 `SHOW CREATE TABLE my_table` 查看) ：

```sql
CREATE TABLE my_table
(
    `metric_name` String,
    `tags` Map(String, String),
    `time_series` Array(Tuple(DateTime64(3), Float64)),
    `metric_family` String,
    `type` String,
    `unit` String,
    `help` String
)
ENGINE = TimeSeries
SAMPLES INNER COLUMNS
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp)
TAGS INNER COLUMNS
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
TAGS INNER ENGINE = AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1
METRICS INNER COLUMNS
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
METRICS INNER ENGINE = ReplacingMergeTree ORDER BY metric_family_name
```

因此，这些列会自动生成，另外还会有三个内部目标表，它们各自的列定义都存储在 `INNER COLUMNS` 子句中。

内部目标表的名称类似于 `.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`、
`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`、`.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
并且每个目标表都有自己的一组列：

```sql
CREATE TABLE default.`.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp)
```

```sql
CREATE TABLE default.`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
ENGINE = AggregatingMergeTree
PRIMARY KEY metric_name
ORDER BY (metric_name, id)
SETTINGS allow_dimensions_outside_sorting_key = 1
```

```sql
CREATE TABLE default.`.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
ENGINE = ReplacingMergeTree
ORDER BY metric_family_name
```

<div id="create-as">
  ## 基于现有表创建表
</div>

语句 `CREATE TABLE new_table AS existing_table` 会从 `existing_table` 复制以下内容：

* `SETTINGS`
* 每种 kind 的 `INNER COLUMNS`
* 每种 kind 的 `INNER ENGINE`

如果 `existing_table` 存在外部目标，则不允许使用该语句。
外层列列表会重新生成，不会直接复制。

<div id="adjusting-column-types">
  ## 调整列类型
</div>

您可以使用 `INNER COLUMNS` 子句来调整内部目标表中各列的类型。例如，要将时间戳以微秒存储，并将值存储为 `Float32`：

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(6), value Float32)
```

同一个子句也可用于指定编解码器和其他列属性：

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(3) CODEC(DoubleDelta))
```

<div id="id-column">
  ## `id` 列
</div>

`id` 列包含标识符；每个标识符都是根据指标名称与标签的组合计算生成的。
用于生成标识符的类型和 `DEFAULT` 表达式可通过 `TAGS INNER COLUMNS` 子句自定义：

```sql
CREATE TABLE my_table ENGINE=TimeSeries
TAGS INNER COLUMNS (id UInt64 DEFAULT sipHash64(metric_name, all_tags))
```

`id` 列类型必须是 `UUID`、`UInt64`、`UInt128` 或 `FixedString(16)` 之一。如果未提供 `DEFAULT` 表达式，ClickHouse 会根据 `id` 类型自动选择。`samples` 和 `tags` 内部表中声明的 `id` 类型必须一致。

`id_generator` 设置提供了相同的自定义方式，无需使用 `INNER COLUMNS` 子句：

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SETTINGS id_generator = 'sipHash64(metric_name, all_tags)'
```

如果设置了此项，即使该列的 `DEFAULT` 包含不同的表达式，也会用它来生成 `id`。

<div id="tags-and-all-tags">
  ## `tags` 和 `all_tags` 列
</div>

有两列包含标签 Map：`tags` 和 `all_tags`。在这个示例中，它们的含义相同；不过，如果使用 `tags_to_columns` 设置，
它们也可能不同。此设置允许指定将特定标签存储在单独的列中，而不是存储
在 `tags` 列中的 Map 里：

```sql
CREATE TABLE my_table
ENGINE = TimeSeries 
SETTINGS tags_to_columns = {'instance': 'instance', 'job': 'job'}
```

该 statement 会将列 `instance` 和 `job` 添加到内部 [标签](#tags-table) 目标表中。
在这种情况下，`tags` 列将不包含 `instance` 和 `job` 这两个标签，
但 `all_tags` 列会包含它们。`all_tags` 列是临时列，其唯一用途是用于 `id` 列的 DEFAULT expression
中。

<div id="inner-table-engines">
  ## 内部目标表的表引擎
</div>

默认情况下，内部目标表使用以下表引擎：

* [samples](#samples-table) 表使用 [MergeTree](../mergetree-family/mergetree)；
* [标签](#tags-table) 表使用 [AggregatingMergeTree](../mergetree-family/aggregatingmergetree)，因为相同的数据经常会被多次插入该表，因此需要一种去重方式，
  同时也因为需要对列 `min_time` 和 `max_time` 进行聚合；
* [metrics](#metrics-table) 表使用 [ReplacingMergeTree](../mergetree-family/replacingmergetree)，因为相同的数据经常会被多次插入该表，因此需要一种去重方式。

如果进行了相应指定，内部目标表也可以使用其他表引擎：

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES ENGINE=ReplicatedMergeTree
TAGS ENGINE=ReplicatedAggregatingMergeTree
METRICS ENGINE=ReplicatedReplacingMergeTree
```

[标签](#tags-table)表会将标签列 (以及 `tags`/`all_tags` Map) 放在其排序键之外，
而 `AggregatingMergeTree` 默认会拒绝这种做法 (参见 [`allow_dimensions_outside_sorting_key`](../mergetree-family/aggregatingmergetree)) 。
这在此处是安全的，因为这些列在函数上依赖于 `id`，而 `id` 是排序键的一部分，因此所有
被后台 merge 折叠到一起的行都具有相同的值。像上文这样，当内部标签表自动生成，或其
engine 以内联方式指定时，`TimeSeries` 会自动为其设置 `allow_dimensions_outside_sorting_key = 1`；
对于手动创建的[外部](#external-target-tables)聚合标签表，则必须自行设置。

<div id="external-target-tables">
  ## 外部目标表
</div>

也可以让 `TimeSeries` 表使用手动创建的表：

```sql
CREATE TABLE samples_for_my_table
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp);

CREATE TABLE tags_for_my_table ...

CREATE TABLE metrics_for_my_table ...

CREATE TABLE my_table ENGINE=TimeSeries SAMPLES samples_for_my_table TAGS tags_for_my_table METRICS metrics_for_my_table;
```

外部表的列类型 (`id`、`timestamp`、`value` 以及 [`tags_to_columns`](#settings) 中列出的 `<tag_value_column>`) 必须与 `TimeSeries` 表原本会在内部生成的类型相匹配 (类型约束请参见 [Samples table](#samples-table)、[Tags table](#tags-table) 和 [Metrics table](#metrics-table)) 。类型不匹配会在 `CREATE` 时报错。

外部标签目标的 id 生成器表达式会在写入时按以下顺序解析：首先是 [`id_generator`](#settings) 设置 (如果已设置) ，其次是外部表 `id` 列上声明的 `DEFAULT` (如果有) ，最后是根据 `id` 类型派生出的规范生成器。因此，该设置会覆盖外部表上声明的任何 `DEFAULT`——详见 [The `id` column](#id-column)。

<div id="altering-settings">
  ## 修改设置
</div>

在 `CREATE` 之后，可以修改以下两个设置：

* `id_generator`
* `filter_by_min_time_and_max_time`

```sql
ALTER TABLE my_table MODIFY SETTING id_generator = 'sipHash64(metric_name, all_tags)';
ALTER TABLE my_table MODIFY SETTING filter_by_min_time_and_max_time = 0;
```

请注意，如果在标签表中已有数据时更改 `id_generator`，同一指标+标签组合可能会产生不同的 ID——旧行保留原有 ID，新行则使用新的生成器。

其他设置不能通过 `ALTER ... MODIFY SETTING` 更改，因为它们在 `CREATE` 时就已经固化在内部表的 schema 中。

<div id="settings">
  ## 设置
</div>

以下是在定义 `TimeSeries` 表时可指定的设置列表：

| 名称                                   | 类型         | 默认值         | 描述                                                                                                                                                 |
| ------------------------------------ | ---------- | ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id_generator`                       | Expression | 取决于 `id` 类型 | 用于根据时间序列的标签计算其标识符 (指纹) 的表达式。若未设置，则使用 `id` 列的默认表达式。如果 `id` 列的默认表达式也未设置，则会自动选择一个表达式                                                                  |
| `tags_to_columns`                    | Map        | {}          | 指定应在 [标签](#tags-table) 表中拆分到单独列的标签的 Map。语法：`{'tag1': 'column1', 'tag2' : column2, ...}`                                                            |
| `use_all_tags_column_to_generate_id` | Bool       | true        | 在生成用于计算时间序列标识符的表达式时，此标志会启用在计算中使用 `all_tags` 列                                                                                                      |
| `store_min_time_and_max_time`        | Bool       | true        | 如果设为 true，则该表会为每个时间序列存储 `min_time` 和 `max_time`                                                                                                    |
| `aggregate_min_time_and_max_time`    | Bool       | true        | 创建内部目标 `tags` 表时，此标志会启用将 `min_time` 列的类型设为 `SimpleAggregateFunction(min, Nullable(DateTime64(3)))`，而不是仅设为 `Nullable(DateTime64(3))`，`max_time` 列同理 |
| `filter_by_min_time_and_max_time`    | Bool       | true        | 如果设为 true，则该表会使用 `min_time` 和 `max_time` 列来筛选时间序列                                                                                                  |

<div id="functions">
  # 函数
</div>

以下是支持以 `TimeSeries` 表作为参数的函数列表：

* [timeSeriesSamples](../../../sql-reference/table-functions/timeSeriesSamples.md)
* [timeSeriesTags](../../../sql-reference/table-functions/timeSeriesTags.md)
* [timeSeriesMetrics](../../../sql-reference/table-functions/timeSeriesMetrics.md)