---
description: '与 MergeTree 的不同之处在于，它会移除具有相同排序键值（表定义中的 `ORDER BY` 部分，而不是 `PRIMARY KEY`）的重复条目。'
sidebar_label: 'ReplacingMergeTree'
sidebar_position: 40
slug: /engines/table-engines/mergetree-family/replacingmergetree
title: 'ReplacingMergeTree 表引擎'
doc_type: 'reference'
---

该引擎与 [MergeTree](/zh/engines/table-engines/mergetree-family/mergetree) 的不同之处在于，它会移除具有相同[排序键](../../../engines/table-engines/mergetree-family/mergetree.md)值 (表定义中的 `ORDER BY` 部分，而不是 `PRIMARY KEY`) 的重复条目。

数据去重只会在 合并 期间发生。合并会在后台于未知时间进行，因此无法提前规划。部分数据可能仍未被处理。虽然你可以使用 `OPTIMIZE` 查询执行一次计划外的 合并，但不要依赖这种方式，因为 `OPTIMIZE` 查询会读写大量数据。

因此，`ReplacingMergeTree` 适合在后台清理重复数据以节省空间，但它并不能保证完全没有重复数据。

:::note
有关 ReplacingMergeTree 的详细指南 (包括最佳实践以及如何优化性能) ，可在[此处](/zh/guides/replacing-merge-tree)查看。
:::

<div id="creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = ReplacingMergeTree([ver [, is_deleted]])
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

有关参数的说明，请参见[语句说明](../../../sql-reference/statements/create/table.md)。

:::note
行的唯一性由表定义中的 `ORDER BY` 部分决定，而不是 `PRIMARY KEY`。
:::

<div id="replacingmergetree-parameters">
  ## ReplacingMergeTree 参数
</div>

<div id="ver">
  ### `ver`
</div>

`ver` — 带有版本号的列。类型为 `UInt*`、`Date`、`DateTime` 或 `DateTime64`。可选参数。

合并时，`ReplacingMergeTree` 会在所有具有相同排序键的行中只保留一行：

* 如果未设置 `ver`，则保留选集中的最后一行。选集是指参与合并的一组 parts 中的一组行。最近创建的 part (即最后一次插入生成的 part) 会是选集中的最后一个。因此，去重后，对于每个唯一的排序键，都会保留最近一次插入中的最后一行。
* 如果指定了 `ver`，则保留版本值最大的行。如果多行的 `ver` 相同，则对这些行应用“未指定 `ver`”时的规则，也就是说，会保留最近插入的那一行。

示例：

```sql
-- without ver - the last inserted 'wins'
CREATE TABLE myFirstReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime
)
ENGINE = ReplacingMergeTree
ORDER BY key;

INSERT INTO myFirstReplacingMT Values (1, 'first', '2020-01-01 01:01:01');
INSERT INTO myFirstReplacingMT Values (1, 'second', '2020-01-01 00:00:00');

SELECT * FROM myFirstReplacingMT FINAL;

┌─key─┬─someCol─┬───────────eventTime─┐
│   1 │ second  │ 2020-01-01 00:00:00 │
└─────┴─────────┴─────────────────────┘


-- with ver - the row with the biggest ver 'wins'
CREATE TABLE mySecondReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime
)
ENGINE = ReplacingMergeTree(eventTime)
ORDER BY key;

INSERT INTO mySecondReplacingMT Values (1, 'first', '2020-01-01 01:01:01');
INSERT INTO mySecondReplacingMT Values (1, 'second', '2020-01-01 00:00:00');

SELECT * FROM mySecondReplacingMT FINAL;

┌─key─┬─someCol─┬───────────eventTime─┐
│   1 │ first   │ 2020-01-01 01:01:01 │
└─────┴─────────┴─────────────────────┘
```

<div id="is_deleted">
  ### `is_deleted`
</div>

`is_deleted` — 在 合并 期间使用的列名，用于确定该行中的数据表示状态，还是应被删除；`1` 表示“已删除”行，`0` 表示“状态”行。

列的数据类型 — `UInt8`。

:::note
`is_deleted` 只能在使用 `ver` 时启用。

无论对数据执行什么操作，版本号都应递增。如果插入的两行具有相同的版本号，则保留最后插入的那一行。

默认情况下，即使某个键的最后一行是删除行，ClickHouse 也会保留这最后一行。这是为了确保今后即使插入版本更低的行，
删除行仍然会生效。

要永久删除这类删除行，请启用表设置 `allow_experimental_replacing_merge_with_cleanup`，并执行以下任一操作：

1. 设置表设置 `enable_replacing_merge_with_cleanup_for_min_age_to_force_merge`、`min_age_to_force_merge_on_partition_only` 和 `min_age_to_force_merge_seconds`。如果某个分区中的所有 parts 的存在时间都超过 `min_age_to_force_merge_seconds`，ClickHouse 会将它们
   全部 合并 成一个单独的 part，并删除所有删除行。

2. 手动运行 `OPTIMIZE TABLE table [PARTITION partition | PARTITION ID 'partition_id'] FINAL CLEANUP`。
   :::

示例：

```sql
-- with ver and is_deleted
CREATE OR REPLACE TABLE myThirdReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime,
    `is_deleted` UInt8
)
ENGINE = ReplacingMergeTree(eventTime, is_deleted)
ORDER BY key
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 01:01:01', 0);
INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 01:01:01', 1);

select * from myThirdReplacingMT final;

0 rows in set. Elapsed: 0.003 sec.

-- delete rows with is_deleted
OPTIMIZE TABLE myThirdReplacingMT FINAL CLEANUP;

INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 00:00:00', 0);

select * from myThirdReplacingMT final;

┌─key─┬─someCol─┬───────────eventTime─┬─is_deleted─┐
│   1 │ first   │ 2020-01-01 00:00:00 │          0 │
└─────┴─────────┴─────────────────────┴────────────┘
```

<div id="query-clauses">
  ## 查询子句
</div>

创建 `ReplacingMergeTree` 表时，所需的[子句](../../../engines/table-engines/mergetree-family/mergetree.md)与创建 `MergeTree` 表时相同。

<details markdown="1">
  <summary>已弃用的建表方法</summary>

  :::note
  请勿在新项目中使用此方法；如果可能，请将旧项目切换为上文所述的方法。
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] ReplacingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [ver])
  ```

  除 `ver` 外，其他所有参数的含义都与 `MergeTree` 中相同。

  * `ver` - 版本列。可选参数。有关说明，请参见上文。
</details>

<div id="query-time-de-duplication--final">
  ## 查询时去重 &amp; FINAL
</div>

在合并时，ReplacingMergeTree 会以 `ORDER BY` 列的值 (用于创建表) 作为唯一标识来识别重复行，并只保留版本最高的那一行。不过，这只能实现最终一致的正确性——并不能保证这些行一定会被去重，因此不应依赖它。因此，由于查询时会将更新行和删除行也计算在内，查询结果可能不正确。

为了获得正确结果，用户需要在后台合并之外，再结合查询时去重和删除标记移除。这可以通过使用 `FINAL` 运算符来实现。例如，考虑以下示例：

```sql
CREATE TABLE rmt_example
(
    `number` UInt16
)
ENGINE = ReplacingMergeTree
ORDER BY number

INSERT INTO rmt_example SELECT floor(randUniform(0, 100)) AS number
FROM numbers(1000000000)

0 rows in set. Elapsed: 19.958 sec. Processed 1.00 billion rows, 8.00 GB (50.11 million rows/s., 400.84 MB/s.)
```

不使用 `FINAL` 查询会得到不正确的计数 (确切结果会因合并操作而异) ：

```sql
SELECT count()
FROM rmt_example

┌─count()─┐
│     200 │
└─────────┘

1 row in set. Elapsed: 0.002 sec.
```

加上 FINAL 后会得到正确结果：

```sql
SELECT count()
FROM rmt_example
FINAL

┌─count()─┐
│     100 │
└─────────┘

1 row in set. Elapsed: 0.002 sec.
```

有关 `FINAL` 的更多信息，包括如何优化 `FINAL` 的性能，建议阅读我们的[ReplacingMergeTree 详细指南](/zh/guides/replacing-merge-tree)。