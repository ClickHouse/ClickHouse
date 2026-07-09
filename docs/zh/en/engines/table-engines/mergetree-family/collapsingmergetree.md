---
description: '继承自 MergeTree，并增加了在合并过程中对行进行折叠的逻辑。'
keywords: ['更新', '折叠']
sidebar_label: 'CollapsingMergeTree'
sidebar_position: 70
slug: /engines/table-engines/mergetree-family/collapsingmergetree
title: 'CollapsingMergeTree 表引擎'
doc_type: 'guide'
---

<div id="description">
  ## 描述
</div>

`CollapsingMergeTree` 引擎继承自 [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md)，
并增加了在合并过程中折叠行的逻辑。
`CollapsingMergeTree` 表引擎会异步删除 (折叠)
成对的行，前提是排序键 (`ORDER BY`) 中的所有字段都相同，特殊字段 `Sign` 除外；
该字段的值只能是 `1` 或 `-1`。
没有与取值相反的 `Sign` 配对的行会被保留。

更多信息，请参见本文档中的 [Collapsing](#table_engine-collapsingmergetree-collapsing) 部分。

:::note
该引擎可以显著减少存储占用，
从而提高 `SELECT` 查询的效率。
:::

<div id="parameters">
  ## 参数
</div>

此表引擎的所有参数 (`Sign` 参数除外) ，
其含义都与 [`MergeTree`](/zh/engines/table-engines/mergetree-family/mergetree) 中的参数相同。

* `Sign` — 为一列指定的名称，该列用于表示行的类型，其中 `1` 表示“状态”行，`-1` 表示“取消”行。类型：[Int8](/zh/sql-reference/data-types/int-uint)。

<div id="creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) 
ENGINE = CollapsingMergeTree(Sign)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

<details markdown="1">
  <summary>已弃用的建表方法</summary>

  :::note
  不建议在新项目中使用以下方法。
  如有可能，建议将旧项目更新为使用新方法。
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) 
  ENGINE [=] CollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, Sign)
  ```

  `Sign` — 为某一列指定的名称，该列用于表示行的类型，其中 `1` 表示“状态”行，`-1` 表示“取消”行。[Int8](/zh/sql-reference/data-types/int-uint)。
</details>

* 有关查询参数的说明，请参见[查询说明](../../../sql-reference/statements/create/table.md)。
* 创建 `CollapsingMergeTree` 表时，需要使用与创建 `MergeTree` 表时相同的[查询子句](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)。

<div id="table_engine-collapsingmergetree-collapsing">
  ## 折叠
</div>

<div id="data">
  ### 数据
</div>

设想这样一种情况：你需要为某个对象保存持续变化的数据。
乍看之下，似乎为每个对象只保留一行，并在数据发生变化时随时更新，是很合理的做法；
但对于 DBMS 来说，更新操作成本高且速度慢，因为它们需要重写存储中的数据。
如果我们需要快速写入数据，那么执行大量更新就不是一种可接受的方案；
不过，我们始终可以按顺序写入对象的变更。
为此，我们使用特殊的列 `Sign`。

* 如果 `Sign` = `1`，表示该行是“状态”行：*包含表示当前有效状态的字段的行*。
* 如果 `Sign` = `-1`，表示该行是“取消”行：*用于取消具有相同属性的对象状态的行*。

例如，我们想统计用户在某个网站上查看了多少页面，以及在这些页面上停留了多长时间。
在某个时刻，我们写入下面这一行来表示用户活动的状态：

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

在后续某个时刻，我们记录用户活动的变化，并将其写入以下两行：

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

第一行会抵消该对象先前的状态 (本例中表示一个用户) 。
除 `Sign` 外，它应复制 &quot;canceled&quot; 行的所有排序键字段。
上面的第二行包含当前状态。

由于我们只需要用户活动的最后一个状态，因此原始的 &quot;state&quot; 行和我们插入的 &quot;cancel&quot;
行可以按如下所示删除，从而折叠对象的无效 (旧) 状态：

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │ -- old "state" row can be deleted
│ 4324182021466249494 │         5 │      146 │   -1 │ -- "cancel" row can be deleted
│ 4324182021466249494 │         6 │      185 │    1 │ -- new "state" row remains
└─────────────────────┴───────────┴──────────┴──────┘
```

`CollapsingMergeTree` 会在 parts 发生合并时精确执行这种 *折叠* 行为。

:::note
为什么每次变更都需要两行，
将在 [Algorithm](#table_engine-collapsingmergetree-collapsing-algorithm) 段落中进一步讨论。
:::

**这种方法的特点**

1. 写入数据的程序应当记住对象的状态，以便能够将其抵消。&quot;cancel&quot; 行应包含 &quot;state&quot; 的排序键字段副本，以及相反的 `Sign`。这会增加初始存储大小，但能让我们快速写入数据。
2. 列中不断增长的长数组会因写入负载增加而降低引擎效率。数据越简单，效率越高。
3. `SELECT` 结果高度依赖对象变更历史的一致性。准备插入数据时务必谨慎。数据不一致时，可能会得到不可预测的结果。例如，像会话深度这样的非负指标出现负值。

<div id="table_engine-collapsingmergetree-collapsing-algorithm">
  ### 算法
</div>

当 ClickHouse 合并数据 [parts](/zh/concepts/glossary#parts) 时，
对于每一组具有相同排序键 (`ORDER BY`) 的连续行，最终会被缩减为不超过两行，
即 `Sign` = `1` 的“状态行”和 `Sign` = `-1` 的“取消行”。
换句话说，在 ClickHouse 中，这些条目会发生折叠。

对于每个生成的 parts，ClickHouse 会保存：

|    |                                                                        |
| -- | ---------------------------------------------------------------------- |
| 1. | 如果“状态行”和“取消行”的数量相同，且最后一行是“状态行”，则保留第一条“取消行”和最后一条“状态行”。 |
| 2. | 如果“状态行”多于“取消行”，则保留最后一条“状态行”。                                 |
| 3. | 如果“取消行”多于“状态行”，则保留第一条“取消行”。                                 |
| 4. | 在其他所有情况下，不保留任何行。                                                       |

此外，当“状态行”至少比“取消行”多两行，
或者“取消行”至少比“状态行”多两行时，合并会继续进行。
不过，ClickHouse 会将这种情况视为逻辑错误，并将其记录到服务器日志中。
如果同一份数据被重复插入，就可能出现此错误。
因此，折叠不应改变统计计算的结果。
这些变更会逐步折叠，最终几乎每个对象都只会保留最后一个状态。

之所以需要 `Sign` 列，是因为合并算法无法保证
具有相同排序键的所有行都会出现在同一个生成的 parts 中，甚至不能保证它们位于同一台物理服务器上。
ClickHouse 使用多个线程处理 `SELECT` 查询，因此无法预测结果中各行的顺序。

如果需要从 `CollapsingMergeTree` 表中获取完全“折叠”后的数据，就必须进行聚合。
要完成最终折叠，请编写包含 `GROUP BY` 子句以及考虑符号的聚合函数的查询。
例如，要计算数量，请使用 `sum(Sign)` 而不是 `count()`。
要计算某个值的总和，请使用 `sum(Sign * x)` 并结合 `HAVING sum(Sign) > 0`，而不是使用 `sum(x)`
如下方的[示例](#example-of-use)所示。

聚合 `count`、`sum` 和 `avg` 都可以通过这种方式计算。
如果某个对象至少有一个未折叠的状态，则聚合 `uniq` 也可以这样计算。
而聚合 `min` 和 `max` 无法计算，
因为 `CollapsingMergeTree` 不会保存已折叠状态的历史记录。

:::note
如果你需要在不进行聚合的情况下提取数据
(例如，检查最新值满足特定条件的行是否存在) ，
可以在 `FROM` 子句中使用 [`FINAL`](../../../sql-reference/statements/select/from.md#final-modifier) 修饰符。它会在返回结果之前先合并数据。
对于 CollapsingMergeTree，每个键只会返回最新的状态行。
:::

<div id="examples">
  ## 示例
</div>

<div id="example-of-use">
  ### 使用示例
</div>

以下是示例数据：

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

使用 `CollapsingMergeTree` 创建一个 `UAct` 表：

```sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY UserID
```

接下来，我们将插入一些数据：

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1)
```

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1),(4324182021466249494, 6, 185, 1)
```

我们使用两条 `INSERT` 查询来创建两个不同的 parts。

:::note
如果通过单条查询插入数据，ClickHouse 只会创建一个 parts，且之后不会再执行任何合并。
:::

我们可以使用以下方式查询数据：

```sql
SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

我们来看看上面返回的数据，确认是否发生了折叠……

通过两条 `INSERT` 查询，我们创建了两个 parts。
`SELECT` 查询由两个线程执行，因此得到的行顺序是随机的。
不过，**并没有发生折叠**，因为这些 parts 此时还未发生合并，
而 ClickHouse 会在后台于某个我们无法预测的时间点合并 parts。

因此，我们需要进行聚合，
这可以通过 [`sum`](/zh/sql-reference/aggregate-functions/reference/sum)
聚合函数和 [`HAVING`](/zh/sql-reference/statements/select/having) 子句来完成：

```sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration
FROM UAct
GROUP BY UserID
HAVING sum(Sign) > 0
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

如果不需要聚合并希望强制执行折叠，也可以在 `FROM` 子句中使用 `FINAL` 修饰符。

```sql
SELECT * FROM UAct FINAL
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

:::note
这种数据选择方式效率较低，不建议用于扫描大量数据 (数百万行) 的场景。
:::

<div id="example-of-another-approach">
  ### 另一种方法示例
</div>

这种方法的思路是，合并时只考虑键字段。
因此，在 &quot;取消&quot;行中，我们可以指定负值，
这样在求和时即使不使用 `Sign` 列，也能抵消该行的上一版本。

在本示例中，我们将使用下面的样本数据：

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │        -5 │     -146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

对于这种方法，需要将 `PageViews` 和 `Duration` 的数据类型修改为可存储负值。
因此，在使用
`collapsingMergeTree` 创建表 `UAct` 时，我们将这些列的数据类型从 `UInt8` 改为 `Int16`：

```sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews Int16,
    Duration Int16,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY UserID
```

我们通过向表中插入数据来验证这种方法。

不过，对于示例或小型表，这样做也是可以接受的：

```sql
INSERT INTO UAct VALUES(4324182021466249494,  5,  146,  1);
INSERT INTO UAct VALUES(4324182021466249494, -5, -146, -1);
INSERT INTO UAct VALUES(4324182021466249494,  6,  185,  1);

SELECT * FROM UAct FINAL;
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

```sql
SELECT
    UserID,
    sum(PageViews) AS PageViews,
    sum(Duration) AS Duration
FROM UAct
GROUP BY UserID
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

```sql
SELECT COUNT() FROM UAct
```

```text
┌─count()─┐
│       3 │
└─────────┘
```

```sql
OPTIMIZE TABLE UAct FINAL;

SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```