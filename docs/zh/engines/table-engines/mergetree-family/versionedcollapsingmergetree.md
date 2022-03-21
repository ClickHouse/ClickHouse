---
toc_priority: 37
toc_title: "版本折叠MergeTree"
---

# VersionedCollapsingMergeTree {#versionedcollapsingmergetree}

这个引擎:

-   允许快速写入不断变化的对象状态。
-   删除后台中的旧对象状态。 这显着降低了存储体积。

请参阅部分 [崩溃](#table_engines_versionedcollapsingmergetree) 有关详细信息。

引擎继承自 [MergeTree](mergetree.md#table_engines-mergetree) 并将折叠行的逻辑添加到合并数据部分的算法中。 `VersionedCollapsingMergeTree` 用于相同的目的 [折叠树](collapsingmergetree.md) 但使用不同的折叠算法，允许以多个线程的任何顺序插入数据。 特别是， `Version` 列有助于正确折叠行，即使它们以错误的顺序插入。 相比之下, `CollapsingMergeTree` 只允许严格连续插入。

## 创建表 {#creating-a-table}

``` sql
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

有关查询参数的说明，请参阅 [查询说明](../../../sql-reference/statements/create.md).

**引擎参数**

``` sql
VersionedCollapsingMergeTree(sign, version)
```

-   `sign` — 指定行类型的列名: `1` 是一个 “state” 行, `-1` 是一个 “cancel” 行

    列数据类型应为 `Int8`.

-   `version` — 指定对象状态版本的列名。

    列数据类型应为 `UInt*`.

**查询 Clauses**

当创建一个 `VersionedCollapsingMergeTree` 表时，跟创建一个 `MergeTree`表的时候需要相同 [Clause](mergetree.md)

<details markdown="1">

<summary>不推荐使用的创建表的方法</summary>

!!! attention "注意"
    不要在新项目中使用此方法。 如果可能，请将旧项目切换到上述方法。

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] VersionedCollapsingMergeTree(date-column [, samp#table_engines_versionedcollapsingmergetreeling_expression], (primary, key), index_granularity, sign, version)
```

所有的参数，除了 `sign` 和 `version` 具有相同的含义 `MergeTree`.

-   `sign` — 指定行类型的列名: `1` 是一个 “state” 行, `-1` 是一个 “cancel” 划

    Column Data Type — `Int8`.

-   `version` — 指定对象状态版本的列名。

    列数据类型应为 `UInt*`.

</details>

## 折叠 {#table_engines_versionedcollapsingmergetree}

### 数据 {#data}

考虑一种情况，您需要为某个对象保存不断变化的数据。 对于一个对象有一行，并在发生更改时更新该行是合理的。 但是，对于数据库管理系统来说，更新操作非常昂贵且速度很慢，因为它需要重写存储中的数据。 如果需要快速写入数据，则不能接受更新，但可以按如下顺序将更改写入对象。

使用 `Sign` 列写入行时。 如果 `Sign = 1` 这意味着该行是一个对象的状态（让我们把它称为 “state” 行）。 如果 `Sign = -1` 它指示具有相同属性的对象的状态的取消（让我们称之为 “cancel” 行）。 还可以使用 `Version` 列，它应该用单独的数字标识对象的每个状态。

例如，我们要计算用户在某个网站上访问了多少页面以及他们在那里的时间。 在某个时间点，我们用用户活动的状态写下面的行:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

在稍后的某个时候，我们注册用户活动的变化，并用以下两行写入它。

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

第一行取消对象（用户）的先前状态。 它应该复制已取消状态的所有字段，除了 `Sign`.

第二行包含当前状态。

因为我们只需要用户活动的最后一个状态，行

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

可以删除，折叠对象的无效（旧）状态。 `VersionedCollapsingMergeTree` 在合并数据部分时执行此操作。

要了解为什么每次更改都需要两行，请参阅 [算法](#table_engines-versionedcollapsingmergetree-algorithm).

**使用注意事项**

1.  写入数据的程序应该记住对象的状态以取消它。 该 “cancel” 字符串应该是 “state” 与相反的字符串 `Sign`. 这增加了存储的初始大小，但允许快速写入数据。
2.  列中长时间增长的数组由于写入负载而降低了引擎的效率。 数据越简单，效率就越高。
3.  `SELECT` 结果很大程度上取决于对象变化历史的一致性。 准备插入数据时要准确。 不一致的数据将导致不可预测的结果，例如会话深度等非负指标的负值。

### 算法 {#table_engines-versionedcollapsingmergetree-algorithm}

当ClickHouse合并数据部分时，它会删除具有相同主键和版本但 `Sign`值不同的一对行. 行的顺序并不重要。

当ClickHouse插入数据时，它会按主键对行进行排序。 如果 `Version` 列不在主键中，ClickHouse将其隐式添加到主键作为最后一个字段并使用它进行排序。

## 选择数据 {#selecting-data}

ClickHouse不保证具有相同主键的所有行都将位于相同的结果数据部分中，甚至位于相同的物理服务器上。 对于写入数据和随后合并数据部分都是如此。 此外，ClickHouse流程 `SELECT` 具有多个线程的查询，并且无法预测结果中的行顺序。 这意味着，如果有必要从`VersionedCollapsingMergeTree` 表中得到完全 “collapsed” 的数据，聚合是必需的。

要完成折叠，请使用 `GROUP BY` 考虑符号的子句和聚合函数。 例如，要计算数量，请使用 `sum(Sign)` 而不是 `count()`. 要计算的东西的总和，使用 `sum(Sign * x)` 而不是 `sum(x)`，并添加 `HAVING sum(Sign) > 0`.

聚合 `count`, `sum` 和 `avg` 可以这样计算。 聚合 `uniq` 如果对象至少具有一个非折叠状态，则可以计算。 聚合 `min` 和 `max` 无法计算是因为 `VersionedCollapsingMergeTree` 不保存折叠状态值的历史记录。

如果您需要提取数据 “collapsing” 但是，如果没有聚合（例如，要检查是否存在其最新值与某些条件匹配的行），则可以使用 `FINAL` 修饰 `FROM` 条件这种方法效率低下，不应与大型表一起使用。

## 使用示例 {#example-of-use}

示例数据:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

创建表:

``` sql
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

插入数据:

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1, 1)
```

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1, 1),(4324182021466249494, 6, 185, 1, 2)
```

我们用两个 `INSERT` 查询以创建两个不同的数据部分。 如果我们使用单个查询插入数据，ClickHouse将创建一个数据部分，并且永远不会执行任何合并。

获取数据:

``` sql
SELECT * FROM UAct
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 │
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

我们在这里看到了什么，折叠的部分在哪里？
我们使用两个创建了两个数据部分 `INSERT` 查询。 该 `SELECT` 查询是在两个线程中执行的，结果是行的随机顺序。
由于数据部分尚未合并，因此未发生折叠。 ClickHouse在我们无法预测的未知时间点合并数据部分。

这就是为什么我们需要聚合:

``` sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration,
    Version
FROM UAct
GROUP BY UserID, Version
HAVING sum(Sign) > 0
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │       2 │
└─────────────────────┴───────────┴──────────┴─────────┘
```

如果我们不需要聚合，并希望强制折叠，我们可以使用 `FINAL` 修饰符 `FROM` 条款

``` sql
SELECT * FROM UAct FINAL
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

这是一个非常低效的方式来选择数据。 不要把它用于数据量大的表。

[原始文章](https://clickhouse.com/docs/en/operations/table_engines/versionedcollapsingmergetree/) <!--hide-->
