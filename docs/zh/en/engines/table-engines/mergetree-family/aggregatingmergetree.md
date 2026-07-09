---
description: '将具有相同主键的所有行（更准确地说，具有相同[排序键](../../../engines/table-engines/mergetree-family/mergetree.md)的所有行）
  替换为单行（在单个数据分区片段内），该行存储聚合函数状态的组合。'
sidebar_label: 'AggregatingMergeTree'
sidebar_position: 60
slug: /engines/table-engines/mergetree-family/aggregatingmergetree
title: 'AggregatingMergeTree 表引擎'
doc_type: 'reference'
---

该引擎继承自 [MergeTree](/zh/engines/table-engines/mergetree-family/mergetree)，但修改了数据分区片段合并的逻辑。ClickHouse 会将具有相同主键的所有行 (更准确地说，具有相同[排序键](../../../engines/table-engines/mergetree-family/mergetree.md)的所有行) 替换为单行 (在单个数据分区片段内) ，该行存储聚合函数状态的组合。

你可以将 `AggregatingMergeTree` 表用于增量数据聚合，包括聚合型 materialized view。

你可以在下面的视频中查看如何使用 AggregatingMergeTree 和聚合函数的示例：

<div class="vimeo-container">
  <iframe width="1030" height="579" src="https://www.youtube.com/embed/pryhI4F_zqQ" title="ClickHouse 中的聚合状态" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />
</div>

该引擎会处理以下类型的所有列：

* [`AggregateFunction`](../../../sql-reference/data-types/aggregatefunction.md)
* [`SimpleAggregateFunction`](../../../sql-reference/data-types/simpleaggregatefunction.md)

如果 `AggregatingMergeTree` 能将行数减少几个数量级，那么它就是合适的选择。

<div id="creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = AggregatingMergeTree()
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[TTL expr]
[SETTINGS name=value, ...]
```

有关请求参数的说明，请参见[请求说明](../../../sql-reference/statements/create/table.md)。

**查询子句**

创建 `AggregatingMergeTree` 表时，所需的[子句](../../../engines/table-engines/mergetree-family/mergetree.md)与创建 `MergeTree` 表时相同。

<details markdown="1">
  <summary>已弃用的建表方法</summary>

  :::note
  请勿在新项目中使用此方法；如有可能，请将旧项目切换为上述方法。
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] AggregatingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
  ```

  所有参数的含义与 `MergeTree` 中相同。
</details>

<div id="select-and-insert">
  ## SELECT 和 INSERT
</div>

要插入数据，请使用带有 aggregate `-State-` 函数的 [INSERT SELECT](../../../sql-reference/statements/insert-into.md) 查询。
从 `AggregatingMergeTree` 表中选择数据时，请使用 `GROUP BY` 子句，并使用与插入数据时相同的 聚合函数，但要加上 `-Merge` 后缀。

在 `SELECT` 查询结果中，`AggregateFunction` 类型的值在所有 ClickHouse output formats 中都采用特定于实现的二进制表示。例如，如果使用 `SELECT` 查询将数据转储为 `TabSeparated` format，则可以使用 `INSERT` 查询将该转储重新导入。

<div id="example-of-an-aggregated-materialized-view">
  ## 聚合 materialized view 示例
</div>

以下示例假设您有一个名为 `test` 的数据库。如果尚不存在，请使用以下命令创建：

```sql
CREATE DATABASE test;
```

现在创建包含原始数据的 `test.visits` 表：

```sql
CREATE TABLE test.visits
 (
    StartDate DateTime64 NOT NULL,
    CounterID UInt64,
    Sign Nullable(Int32),
    UserID Nullable(Int32)
) ENGINE = MergeTree ORDER BY (StartDate, CounterID);
```

接下来，你需要一个 `AggregatingMergeTree` 表，用于存储 `AggregationFunction`，以跟踪访问总次数和唯一用户数。

创建一个 `AggregatingMergeTree` materialized view，用于监控 `test.visits` 表，并使用 [`AggregateFunction`](/zh/sql-reference/data-types/aggregatefunction) 类型：

```sql
CREATE TABLE test.agg_visits (
    StartDate DateTime64 NOT NULL,
    CounterID UInt64,
    Visits AggregateFunction(sum, Nullable(Int32)),
    Users AggregateFunction(uniq, Nullable(Int32))
)
ENGINE = AggregatingMergeTree() ORDER BY (StartDate, CounterID);
```

创建一个 materialized view，将 `test.visits` 中的数据写入 `test.agg_visits`：

```sql
CREATE MATERIALIZED VIEW test.visits_mv TO test.agg_visits
AS SELECT
    StartDate,
    CounterID,
    sumState(Sign) AS Visits,
    uniqState(UserID) AS Users
FROM test.visits
GROUP BY StartDate, CounterID;
```

向 `test.visits` 表中插入数据：

```sql
INSERT INTO test.visits (StartDate, CounterID, Sign, UserID)
 VALUES (1667446031000, 1, 3, 4), (1667446031000, 1, 6, 3);
```

数据会同时插入到 `test.visits` 和 `test.agg_visits` 中。

要获取聚合后的数据，请从 materialized view `test.visits_mv` 执行类似 `SELECT ... GROUP BY ...` 的查询：

```sql
SELECT
    StartDate,
    sumMerge(Visits) AS Visits,
    uniqMerge(Users) AS Users
FROM test.visits_mv
GROUP BY StartDate
ORDER BY StartDate;
```

```text
┌───────────────StartDate─┬─Visits─┬─Users─┐
│ 2022-11-03 03:27:11.000 │      9 │     2 │
└─────────────────────────┴────────┴───────┘
```

向 `test.visits` 再添加两条记录，不过这次请尝试让其中一条记录使用不同的时间戳：

```sql
INSERT INTO test.visits (StartDate, CounterID, Sign, UserID)
 VALUES (1669446031000, 2, 5, 10), (1667446031000, 3, 7, 5);
```

再次运行 `SELECT` 查询，返回以下输出：

```text
┌───────────────StartDate─┬─Visits─┬─Users─┐
│ 2022-11-03 03:27:11.000 │     16 │     3 │
│ 2022-11-26 07:00:31.000 │      5 │     1 │
└─────────────────────────┴────────┴───────┘
```

在某些情况下，你可能希望避免在写入时对行进行预聚合，从而将聚合的开销从写入时
转移到合并时。通常，为了避免报错，必须在 materialized view 定义的 `GROUP BY`
子句中包含那些不属于聚合部分的列。不过，你也可以通过使用 [`initializeAggregation`](/zh/sql-reference/functions/other-functions#initializeAggregation)
函数，并将设置 `optimize_on_insert = 0` (默认开启) ，来实现这一点。在这种情况下，不再需要使用 `GROUP BY`：

```sql
CREATE MATERIALIZED VIEW test.visits_mv TO test.agg_visits
AS SELECT
    StartDate,
    CounterID,
    initializeAggregation('sumState', Sign) AS Visits,
    initializeAggregation('uniqState', UserID) AS Users
FROM test.visits;
```

:::note
使用 `initializeAggregation` 时，会为每一行单独创建一个 aggregate state，而不进行分组。
每个源行都会在 materialized view 中生成一行，实际的 aggregation 会在后续 `AggregatingMergeTree` 合并 parts 时发生。只有当 `optimize_on_insert = 0` 时才会如此。
:::

<div id="tuple-element-aggregation">
  ## Tuple 元素聚合
</div>

启用 `allow_tuple_element_aggregation` 设置后，`Tuple` 列会被递归地展平，使每个叶子元素都能独立参与聚合。这意味着，`Tuple` 中的 `AggregateFunction` 或 `SimpleAggregateFunction` 子列会按照各自对应的函数进行聚合，就像它们是顶层列一样。

`Tuple` 中属于排序键的子列不会参与聚合。非聚合子列会被视为普通列 (保留其第一个值) 。

:::note
此设置不可变，必须在创建表时指定。
:::

```sql
CREATE TABLE agg_tuples
(
    key UInt32,
    metrics Tuple(
        total_visits SimpleAggregateFunction(sum, UInt64),
        unique_users SimpleAggregateFunction(max, UInt64)
    )
) ENGINE = AggregatingMergeTree()
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO agg_tuples VALUES (1, (100, 5));
INSERT INTO agg_tuples VALUES (1, (200, 8));
INSERT INTO agg_tuples VALUES (2, (50, 3));

OPTIMIZE TABLE agg_tuples FINAL;

SELECT key, metrics.total_visits, metrics.unique_users FROM agg_tuples ORDER BY key;
```

```text
┌─key─┬─metrics.total_visits─┬─metrics.unique_users─┐
│   1 │                  300 │                    8 │
│   2 │                   50 │                    3 │
└─────┴──────────────────────┴──────────────────────┘
```

`total_visits` 通过 `sum` 进行聚合 (100 + 200 = 300) ，而 `unique_users` 通过 `max` 进行聚合 (max(5, 8) = 8) 。

<div id="related-content">
  ## 相关内容
</div>

* 博客：[在 ClickHouse 中使用适用于数组、Map 和状态的聚合组合器](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)