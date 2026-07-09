---
description: '支持快速写入持续变化的对象状态，
  并在后台删除旧的对象状态。'
sidebar_label: 'VersionedCollapsingMergeTree'
sidebar_position: 80
slug: /engines/table-engines/mergetree-family/versionedcollapsingmergetree
title: 'VersionedCollapsingMergeTree 表引擎'
doc_type: 'reference'
---

此引擎：

* 支持快速写入持续变化的对象状态。
* 在后台删除旧的对象状态。这会显著减少存储占用。

有关详细信息，请参见 [Collapsing](#table_engines_versionedcollapsingmergetree) 一节。

该引擎继承自 [MergeTree](/zh/engines/table-engines/mergetree-family/mergetree)，并在合并数据分区片段的算法中加入了行折叠逻辑。`VersionedCollapsingMergeTree` 与 [CollapsingMergeTree](../../../engines/table-engines/mergetree-family/collapsingmergetree.md) 的用途相同，但它采用了不同的折叠算法，因此可以通过多个线程以任意顺序插入数据。尤其是，`Version` 列有助于正确折叠这些行，即使它们的插入顺序不正确也没问题。相比之下，`CollapsingMergeTree` 只允许严格连续地插入。

<div id="creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = VersionedCollapsingMergeTree(sign, version)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

有关查询参数的说明，请参阅[查询描述](../../../sql-reference/statements/create/table.md)。

<div id="engine-parameters">
  ### 引擎参数
</div>

```sql
VersionedCollapsingMergeTree(sign, version)
```

| 参数        | 描述                                                         | 类型                                                                                                                                                                                                                                                                                           |
| --------- | ---------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sign`    | 用于表示行类型的列名：`1` 表示 &quot;状态&quot;行，`-1` 表示 &quot;抵消&quot;行。 | [`Int8`](/zh/sql-reference/data-types/int-uint)                                                                                                                                                                                                                                                 |
| `version` | 用于表示对象状态版本的列名。                                             | [`Int*`](/zh/sql-reference/data-types/int-uint), [`UInt*`](/zh/sql-reference/data-types/int-uint), [`Date`](/zh/sql-reference/data-types/date), [`Date32`](/zh/sql-reference/data-types/date32), [`DateTime`](/zh/sql-reference/data-types/datetime) 或 [`DateTime64`](/zh/sql-reference/data-types/datetime64) |

<div id="query-clauses">
  ### 查询子句
</div>

创建 `VersionedCollapsingMergeTree` 表时，需要使用与创建 `MergeTree` 表时相同的[子句](../../../engines/table-engines/mergetree-family/mergetree.md)。

<details markdown="1">
  <summary>已弃用的建表方法</summary>

  :::note
  请勿在新项目中使用此方法。如有可能，请将旧项目切换为上文所述的方法。
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] VersionedCollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, sign, version)
  ```

  除 `sign` 和 `version` 外，其他所有参数的含义都与 `MergeTree` 中相同。

  * `sign` — 表示行类型的列名：`1` 表示“状态”行，`-1` 表示“抵消”行。

    列的数据类型为 `Int8`。

  * `version` — 表示对象状态版本的列名。

    该列的数据类型应为 `UInt*`。
</details>

<div id="table_engines_versionedcollapsingmergetree">
  ## 折叠
</div>

<div id="data">
  ### Data
</div>

设想这样一种情况：你需要保存某个对象持续变化的数据。比较合理的做法是为每个对象保留一行，并在发生变化时更新这一行。然而，对 DBMS 而言，更新操作代价高、速度慢，因为它需要重写存储中的数据。如果你需要快速写入数据，更新就不可取；但你可以按顺序写入对象的变更，如下所示。

写入行时使用 `Sign` 列。如果 `Sign = 1`，表示该行是对象的一个状态 (我们称之为“状态”行) 。如果 `Sign = -1`，则表示抵消具有相同属性的对象状态 (我们称之为“抵消”行) 。此外还要使用 `Version` 列，它应当用单独的编号来标识对象的每个状态。

例如，我们想统计用户访问某个站点的页面数量以及停留时长。在某个时刻，我们会写入下面这一行来记录用户活动状态：

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

在后续某个时刻，我们会记录用户活动的变更，并将其写入如下两行记录。

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

第一行用于抵消该对象 (用户) 的上一条状态。除 `Sign` 外，它应复制被抵消状态中的所有字段。

第二行包含当前状态。

由于我们只需要用户活动的最后一个状态，这些行

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

可以删除，从而折叠对象的无效 (旧) 状态。`VersionedCollapsingMergeTree` 会在合并数据分区片段时执行此操作。

要了解为什么每次变更都需要两行，请参见[Algorithm](#table_engines-versionedcollapsingmergetree-algorithm)。

**使用说明**

1. 写入数据的程序应记住对象的状态，以便能够将其抵消。&quot;抵消&quot; 行应包含主键字段、&quot;状态&quot; 行的版本，以及相反的 `Sign` 的副本。这会增加初始存储占用，但可以实现快速写入。
2. 列中持续增长的长数组会因写入负载而降低引擎效率。数据越简单，效率越高。
3. `SELECT` 结果在很大程度上依赖于对象变更历史的一致性。准备插入数据时务必保证准确。如果数据不一致，可能会得到不可预测的结果，例如会为会话深度这类非负指标得到负值。

<div id="table_engines-versionedcollapsingmergetree-algorithm">
  ### 算法
</div>

当 ClickHouse 合并数据分区片段时，会删除每一对主键和版本相同但 `Sign` 不同的行。行的顺序无关紧要。

当 ClickHouse 插入数据时，会按主键对行排序。如果 `Version` 列不在主键中，ClickHouse 会将其隐式添加为主键中的最后一个字段，并据此排序。

<div id="selecting-data">
  ## 选择数据
</div>

ClickHouse 不保证具有相同主键的所有行都会落在同一个结果数据分区片段中，甚至不保证位于同一台物理服务器上。无论是写入数据时，还是后续合并数据分区片段时，都是如此。此外，ClickHouse 会使用多线程处理 `SELECT` 查询，因此无法预测结果中各行的顺序。这意味着，如果需要从 `VersionedCollapsingMergeTree` 表中获取完全“折叠”后的数据，就必须进行聚合。

要完成折叠，请编写包含 `GROUP BY` 子句以及将 Sign 考虑在内的聚合函数的查询。例如，要计算数量，应使用 `sum(Sign)` 而不是 `count()`。要计算某个值的总和，应使用 `sum(Sign * x)` 而不是 `sum(x)`，并添加 `HAVING sum(Sign) > 0`。

聚合 `count`、`sum` 和 `avg` 可以通过这种方式计算。如果某个对象至少有一个未折叠的状态，则聚合 `uniq` 也可以计算。聚合 `min` 和 `max` 无法计算，因为 `VersionedCollapsingMergeTree` 不会保存已折叠状态的值历史。

如果你需要提取经过“折叠”但不做聚合的数据 (例如，检查是否存在最新值满足特定条件的行) ，可以在 `FROM` 子句中使用 `FINAL` modifier。这种方法效率较低，不应在大型表上使用。

<div id="example-of-use">
  ## 使用示例
</div>

示例数据：

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

创建表：

```sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Sign Int8,
    Version UInt8
)
ENGINE = VersionedCollapsingMergeTree(Sign, Version)
ORDER BY UserID
```

插入数据：

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1, 1)
```

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1, 1),(4324182021466249494, 6, 185, 1, 2)
```

我们使用两条 `INSERT` 查询来创建两个不同的数据分区片段。如果只用一条查询插入数据，ClickHouse 只会创建一个数据分区片段，因此永远不会执行任何合并。

获取数据：

```sql
SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 │
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

这里看到了什么，折叠后的 parts 在哪里？
我们通过两条 `INSERT` 查询创建了两个数据分区片段。`SELECT` 查询由两个线程执行，因此结果中的行顺序是随机的。
没有发生折叠，是因为这些数据分区片段尚未合并。ClickHouse 会在某个我们无法预测的时间点合并数据分区片段。

这就是为什么我们需要聚合：

```sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration,
    Version
FROM UAct
GROUP BY UserID, Version
HAVING sum(Sign) > 0
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │       2 │
└─────────────────────┴───────────┴──────────┴─────────┘
```

如果不需要聚合而想强制进行折叠，可以在 `FROM` 子句中使用 `FINAL` 修饰符。

```sql
SELECT * FROM UAct FINAL
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

这是一种非常低效的数据选择方式。不要在大型表上使用它。