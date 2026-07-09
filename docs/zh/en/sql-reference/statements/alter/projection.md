---
description: '关于管理投影的文档'
sidebar_label: 'PROJECTION'
sidebar_position: 49
slug: /sql-reference/statements/alter/projection
title: '投影'
doc_type: '参考'
---

本页介绍投影是什么、如何使用投影，以及管理投影的各种选项。

<div id="overview">
  ## 投影概述
</div>

投影以有利于优化查询执行的格式存储数据，此功能适用于以下场景：

* 在不属于主键的列上运行查询
* 对列进行预聚合，从而同时减少计算和 IO

你可以为一张表定义一个或多个投影；在查询分析期间，ClickHouse 会自动选择需要扫描数据量最少的投影，而无需修改用户提交的查询。

:::note[磁盘使用情况]
投影会在内部创建一个新的隐藏表，这意味着会需要更多的 IO 和磁盘空间。
例如，如果投影定义了不同的主键，原始表中的所有数据都会被复制一份。
:::

你可以在这个[页面](/zh/guides/best-practices/sparse-primary-indexes.md/#option-3-projections)查看有关投影内部工作原理的更多技术细节。

<div id="examples">
  ## 使用投影
</div>

<div id="example-filtering-without-using-primary-keys">
  ### 不使用主键进行筛选的示例
</div>

创建表：

```sql
CREATE TABLE visits_order
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String
)
ENGINE = MergeTree()
PRIMARY KEY user_agent
```

使用 `ALTER TABLE`，即可向现有表添加投影：

```sql
ALTER TABLE visits_order ADD PROJECTION user_name_projection (
    SELECT *
    ORDER BY user_name
)

ALTER TABLE visits_order MATERIALIZE PROJECTION user_name_projection
```

插入数据：

```sql
INSERT INTO visits_order SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```

投影使我们能够按 `user_name` 快速过滤，即使在原始表中 `user_name` 并未定义为 `PRIMARY_KEY`。
在查询时，由于数据按 `user_name` 排序，ClickHouse 会判断使用该投影可减少需要处理的数据量。

```sql
SELECT
    *
FROM visits_order
WHERE user_name='test'
LIMIT 2
```

要验证某个查询是否使用了该 projection，我们可以查看 `system.query_log` 表。在 `projections` 字段中，会显示所使用 projection 的名称；如果未使用任何 projection，则该字段为空：

```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

<div id="example-pre-aggregation-query">
  ### 预聚合查询示例
</div>

使用投影 `projection_visits_by_user` 创建表：

```sql
CREATE TABLE visits
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String,
   PROJECTION projection_visits_by_user
   (
       SELECT
           user_agent,
           sum(pages_visited)
       GROUP BY user_id, user_agent
   )
)
ENGINE = MergeTree()
ORDER BY user_agent
```

插入数据：

```sql
INSERT INTO visits SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```

```sql
INSERT INTO visits SELECT
    number,
    'test',
    1. * (number / 2),
   'IOS'
FROM numbers(100, 500);
```

先使用字段 `user_agent` 执行一个包含 `GROUP BY` 的查询。
该查询不会使用已定义的投影，因为预聚合条件不匹配。

```sql
SELECT
    user_agent,
    count(DISTINCT user_id)
FROM visits
GROUP BY user_agent
```

要使用该投影，您可以执行查询，选择部分或全部预聚合字段以及 `GROUP BY` 字段：

```sql
SELECT
    user_agent
FROM visits
WHERE user_id > 50 AND user_id < 150
GROUP BY user_agent
```

```sql
SELECT
    user_agent,
    sum(pages_visited)
FROM visits
GROUP BY user_agent
```

如前所述，你可以查看 `system.query_log` 表，判断是否使用了投影。
`projections` 字段会显示所使用投影的名称。
如果未使用任何投影，该字段将为空：

```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

<div id="projection-indexes">
  ### 创建和使用投影索引
</div>

创建[投影索引](../../../engines/table-engines/mergetree-family/mergetree.md#projection-index)：

```sql
CREATE TABLE events
(
    `event_time` DateTime,
    `event_id` UInt64,
    `user_id` UInt64,
    `huge_string` String,
    PROJECTION order_by_user_id INDEX user_id TYPE basic
)
ENGINE = MergeTree()
ORDER BY (event_id);
```

<details markdown="1">
  <summary>通过显式指定 `_part_offset` 字段创建投影</summary>

  也可以使用以下语法来创建投影索引 (不推荐) ：

  ```sql
  CREATE TABLE events
  (
      `event_time` DateTime,
      `event_id` UInt64,
      `user_id` UInt64,
      `huge_string` String,
      PROJECTION order_by_user_id
      (
          SELECT
              _part_offset
          ORDER BY user_id
      )
  )
  ENGINE = MergeTree()
  ORDER BY (event_id);
  ```
</details>

插入一些样本数据：

```sql
INSERT INTO events SELECT * FROM generateRandom() LIMIT 100000;
```

`_part_offset` 字段在合并和变更后仍会保留其值，因此对次级索引很有价值。我们可以在查询中利用这一点：

```sql
SELECT
    count()
FROM events
WHERE _part_starting_offset + _part_offset IN (
    SELECT _part_starting_offset + _part_offset
    FROM events
    WHERE user_id = 42
)
SETTINGS enable_shared_storage_snapshot_in_query = 1
```

<div id="example-projection-with-where">
  ### 带 `WHERE` 子句的示例投影
</div>

投影可以包含 `WHERE` 子句，仅存储部分行。当查询经常按已知条件进行过滤时，这种方式很有用——投影只会物化匹配的行，从而减少存储占用并提升查询性能。

创建表并添加带过滤条件的投影：

```sql
CREATE TABLE events
(
    `event_type` String,
    `time` DateTime,
    `message` String
)
ENGINE = MergeTree()
ORDER BY time;

ALTER TABLE events ADD PROJECTION proj_pageview (
    SELECT event_type, time, message
    WHERE event_type = 'pageview'
    ORDER BY time
);

ALTER TABLE events MATERIALIZE PROJECTION proj_pageview;
```

插入数据：

```sql
INSERT INTO events VALUES
    ('pageview', '2024-01-01', 'homepage'),
    ('click', '2024-01-02', 'button'),
    ('pageview', '2024-01-03', 'about');
```

当查询的 `WHERE` clause **包含**投影的 `WHERE` clause (即投影过滤器中的每个条件也都出现在查询的过滤器中) 时，优化器会在判断这样做有利时自动使用该投影：

```sql
-- This query implies the projection's WHERE, so the projection may be used:
SELECT time, message FROM events WHERE event_type = 'pageview';

-- A stricter query also implies the projection's WHERE:
SELECT time, message FROM events WHERE event_type = 'pageview' AND time > '2024-01-01';

-- This query does NOT imply the projection, so the base table is scanned:
SELECT time, message FROM events WHERE event_type = 'click';
```

蕴含检查采用保守策略——它会基于规范化后的表达式形式，对合取项进行精确匹配。它可能会漏掉一些有效的优化机会 (例如范围蕴含) ，但绝不会产生错误结果。

<div id="manipulating-projections">
  ## 操作投影
</div>

可以对[投影](/zh/engines/table-engines/mergetree-family/mergetree.md/#projections)执行以下操作：

<div id="add-projection">
  ### ADD PROJECTION
</div>

使用下面的语句向表的元数据中添加投影定义：

```sql
-- Normal projection (supports WHERE)
ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [WHERE <expr>] [ORDER BY] ) [WITH SETTINGS ( setting_name1 = setting_value1, setting_name2 = setting_value2, ...)]

-- Aggregate projection (supports WHERE)
ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [WHERE <expr>] [GROUP BY] ) [WITH SETTINGS ( setting_name1 = setting_value1, setting_name2 = setting_value2, ...)]
```

:::note
当投影定义了 `WHERE` 子句时，只有匹配该谓词的行才会被物化。只有当查询的 `WHERE` 在逻辑上蕴含该投影的 `WHERE`，且该投影有利于查询计划时，优化器才会使用这样的投影。这同时适用于普通投影和聚合投影。
:::

<div id="with-settings">
  #### `WITH SETTINGS` 子句
</div>

`WITH SETTINGS` 定义**投影级设置**，用于自定义投影存储数据的方式 (例如 `index_granularity` 或 `index_granularity_bytes`) 。
这些设置与 **MergeTree 表设置** 直接对应，但**仅对当前投影生效**。

示例：

```sql
ALTER TABLE t
ADD PROJECTION p (
    SELECT x ORDER BY x
) WITH SETTINGS (
    index_granularity = 4096,
    index_granularity_bytes = 1048576
);
```

投影设置会覆盖该投影实际生效的表设置，但仍需遵循验证规则 (例如，无效或不兼容的覆盖会被拒绝) 。

<div id="drop-projection">
  ### DROP PROJECTION
</div>

使用下面的语句可从表的元数据中移除投影描述，并从磁盘中删除投影文件。
该操作通过 [变更](/zh/sql-reference/statements/alter/index.md#mutations) 实现。

```sql
ALTER TABLE [db.]name [ON CLUSTER cluster] DROP PROJECTION [IF EXISTS] name
```

<div id="materialize-projection">
  ### MATERIALIZE PROJECTION
</div>

使用以下语句重建分区 `partition_name` 中的投影 `name`。
这是通过[变更](/zh/sql-reference/statements/alter/index.md#mutations)实现的。

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] MATERIALIZE PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
```

<div id="clear-projection">
  ### CLEAR PROJECTION
</div>

使用以下语句可从磁盘中删除投影文件，而不会移除其描述。
这是通过[变更](/zh/sql-reference/statements/alter/index.md#mutations)实现的。

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] CLEAR PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
```

命令 `ADD`、`DROP` 和 `CLEAR` 属于轻量级操作，因为它们只会更改元数据或删除文件。
此外，这些命令也会被复制，并通过 ClickHouse Keeper 或 ZooKeeper 同步投影元数据。

:::note
仅带有 [`*MergeTree`](/zh/engines/table-engines/mergetree-family/mergetree.md) 引擎的表 (包括 [Replicated](/zh/engines/table-engines/mergetree-family/replication.md) 变体) 支持投影操作。
:::

<div id="control-projections-merges">
  ### 控制投影合并行为
</div>

执行查询时，ClickHouse 会在原始表及其某个投影之间选择读取来源。
这一决策会针对每个表分区片段单独做出，判断是读取原始表还是读取其某个投影。
ClickHouse 通常会尽量减少读取的数据量，并采用一些技巧来识别最佳读取对象，例如对某个分区片段的主键进行采样。

在某些情况下，源表的数据分区片段没有对应的投影分区片段。
例如，这可能是因为在 SQL 中为表创建投影默认是“惰性”的——它只影响新插入的数据，而不会更改现有 parts。

由于某个投影已经包含预先计算好的聚合值，ClickHouse 会尽量从对应的投影分区片段中读取，以避免在查询运行时再次聚合。如果某个特定分区片段缺少对应的投影分区片段，查询执行就会回退到原始分区片段。

但如果原始表中的行因为较复杂的后台数据分区片段合并而发生了非平凡变化，会怎样呢？
例如，假设该表使用 `ReplacingMergeTree` 表引擎存储。
如果在合并期间多个输入分区片段中检测到同一行，则只会保留最新的行版本 (来自最近插入的分区片段) ，而所有较旧版本都会被丢弃。

类似地，如果表使用 `AggregatingMergeTree` 表引擎存储，则合并操作可能会将输入分区片段中的相同行 (基于主键值) 折叠为单独一行，以更新部分聚合状态。

在 ClickHouse v24.8 之前，投影分区片段要么会在无提示的情况下与主数据失去同步，要么某些操作 (如更新和删除) 根本无法执行，因为如果表存在投影，数据库会自动抛出异常。

从 v24.8 开始，新增了一个表级设置 [`deduplicate_merge_projection_mode`](/zh/operations/settings/merge-tree-settings#deduplicate_merge_projection_mode)，用于控制当上述非平凡后台合并操作发生在原始表分区片段中时的行为。

删除变更是 part merge 操作的另一个例子，这类操作会删除原始表分区片段中的行。从 v24.7 起，我们还提供了一个设置，用于控制由轻量级删除触发的删除变更行为：[`lightweight_mutation_projection_mode`](/zh/operations/settings/merge-tree-settings#deduplicate_merge_projection_mode)。

下面是 `deduplicate_merge_projection_mode` 和 `lightweight_mutation_projection_mode` 的可能值：

* `throw` (默认) ：抛出异常，防止投影分区片段与主数据失去同步。
* `drop`：删除受影响的投影表分区片段。对于受影响的投影分区片段，查询将回退到原始表分区片段。
* `rebuild`：重建受影响的投影分区片段，以与原始表分区片段中的数据保持一致。

<div id="limitations">
  ## 限制
</div>

无法在投影的 `ORDER BY` 子句中使用 `ALIAS` 列。例如：

```sql
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    ab_sum UInt64 ALIAS a + 1,
--highlight-next-line
    PROJECTION p (SELECT a ORDER BY ab_sum)
)
ENGINE = MergeTree ORDER BY id;
-- Fails with UNKNOWN_IDENTIFIER
```

`ALIAS` 列不会物理存储，而是在查询时动态计算，因此在对排序表达式求值时，projection part 的写入路径中无法使用这些列。

请改用 `MATERIALIZED` 列，或将该表达式直接内联：

```sql
-- using MATERIALIZED column
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    ab_sum UInt64 MATERIALIZED a + 1,
    PROJECTION p (SELECT a ORDER BY ab_sum)
)
ENGINE = MergeTree ORDER BY id;

-- using an inline expression
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    PROJECTION p (SELECT a ORDER BY a + 1)
)
ENGINE = MergeTree ORDER BY id;
```

<div id="see-also">
  ## 另请参见
</div>

* [&quot;合并期间对投影的控制&quot; (博客文章) ](https://clickhouse.com/blog/clickhouse-release-24-08#control-of-projections-during-merges)
* [&quot;投影&quot; (指南) ](/zh/data-modeling/projections#using-projections-to-speed-up-UK-price-paid)
* [&quot;materialized views 与投影&quot;](https://clickhouse.com/docs/managing-data/materialized-views-versus-projections)