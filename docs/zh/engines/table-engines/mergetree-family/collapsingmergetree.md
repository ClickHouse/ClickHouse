# 折叠树 {#table_engine-collapsingmergetree}

该引擎继承于 [MergeTree](mergetree.md)，并在数据块合并算法中添加了折叠行的逻辑。

`CollapsingMergeTree` 会异步的删除（折叠）这些除了特定列 `Sign` 有 `1` 和 `-1` 的值以外，其余所有字段的值都相等的成对的行。没有成对的行会被保留。更多的细节请看本文的[折叠](#table_engine-collapsingmergetree-collapsing)部分。

因此，该引擎可以显著的降低存储量并提高 `SELECT` 查询效率。

## 建表 {#jian-biao}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = CollapsingMergeTree(sign)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

请求参数的描述，参考[请求参数](../../../engines/table-engines/mergetree-family/collapsingmergetree.md)。

**CollapsingMergeTree 参数**

-   `sign` — 类型列的名称： `1` 是«状态»行，`-1` 是«取消»行。

    列数据类型 — `Int8`。

**子句**

创建 `CollapsingMergeTree` 表时，需要与创建 `MergeTree` 表时相同的[子句](mergetree.md#table_engine-mergetree-creating-a-table)。

<details markdown="1">

<summary>已弃用的建表方法</summary>

!!! attention "注意"
    不要在新项目中使用该方法，可能的话，请将旧项目切换到上述方法。

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] CollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, sign)
```

除了 `sign` 的所有参数都与 `MergeTree` 中的含义相同。

-   `sign` — 类型列的名称： `1` 是«状态»行，`-1` 是«取消»行。

        列数据类型 — `Int8`。

</details>

## 折叠 {#table_engine-collapsingmergetree-collapsing}

### 数据 {#shu-ju}

考虑你需要为某个对象保存不断变化的数据的情景。似乎为一个对象保存一行记录并在其发生任何变化时更新记录是合乎逻辑的，但是更新操作对 DBMS 来说是昂贵且缓慢的，因为它需要重写存储中的数据。如果你需要快速的写入数据，则更新操作是不可接受的，但是你可以按下面的描述顺序地更新一个对象的变化。

在写入行的时候使用特定的列 `Sign`。如果 `Sign = 1` 则表示这一行是对象的状态，我们称之为«状态»行。如果 `Sign = -1` 则表示是对具有相同属性的状态行的取消，我们称之为«取消»行。

例如，我们想要计算用户在某个站点访问的页面页面数以及他们在那里停留的时间。在某个时候，我们将用户的活动状态写入下面这样的行。

    ┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
    │ 4324182021466249494 │         5 │      146 │    1 │
    └─────────────────────┴───────────┴──────────┴──────┘

一段时间后，我们写入下面的两行来记录用户活动的变化。

    ┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
    │ 4324182021466249494 │         5 │      146 │   -1 │
    │ 4324182021466249494 │         6 │      185 │    1 │
    └─────────────────────┴───────────┴──────────┴──────┘

第一行取消了这个对象（用户）的状态。它需要复制被取消的状态行的所有除了 `Sign` 的属性。

第二行包含了当前的状态。

因为我们只需要用户活动的最后状态，这些行

    ┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
    │ 4324182021466249494 │         5 │      146 │    1 │
    │ 4324182021466249494 │         5 │      146 │   -1 │
    └─────────────────────┴───────────┴──────────┴──────┘

可以在折叠对象的失效（老的）状态的时候被删除。`CollapsingMergeTree` 会在合并数据片段的时候做这件事。

为什么我们每次改变需要 2 行可以阅读[算法](#table_engine-collapsingmergetree-collapsing-algorithm)段。

**这种方法的特殊属性**

1.  写入的程序应该记住对象的状态从而可以取消它。«取消»字符串应该是«状态»字符串的复制，除了相反的 `Sign`。它增加了存储的初始数据的大小，但使得写入数据更快速。
2.  由于写入的负载，列中长的增长阵列会降低引擎的效率。数据越简单，效率越高。
3.  `SELECT` 的结果很大程度取决于对象变更历史的一致性。在准备插入数据时要准确。在不一致的数据中会得到不可预料的结果，例如，像会话深度这种非负指标的负值。

### 算法 {#table_engine-collapsingmergetree-collapsing-algorithm}

当 ClickHouse 合并数据片段时，每组具有相同主键的连续行被减少到不超过两行，一行 `Sign = 1`（«状态»行），另一行 `Sign = -1` （«取消»行），换句话说，数据项被折叠了。

对每个结果的数据部分 ClickHouse 保存：

    1. 第一个«取消»和最后一个«状态»行，如果«状态»和«取消»行的数量匹配和最后一个行是«状态»行
    2. 最后一个«状态»行，如果«状态»行比«取消»行多一个或一个以上。
    3. 第一个«取消»行，如果«取消»行比«状态»行多一个或一个以上。
    4. 没有行，在其他所有情况下。

        合并会继续，但是 ClickHouse 会把此情况视为逻辑错误并将其记录在服务日志中。这个错误会在相同的数据被插入超过一次时出现。

因此，折叠不应该改变统计数据的结果。
变化逐渐地被折叠，因此最终几乎每个对象都只剩下了最后的状态。

`Sign` 是必须的因为合并算法不保证所有有相同主键的行都会在同一个结果数据片段中，甚至是在同一台物理服务器上。ClickHouse 用多线程来处理 `SELECT` 请求，所以它不能预测结果中行的顺序。如果要从 `CollapsingMergeTree` 表中获取完全«折叠»后的数据，则需要聚合。

要完成折叠，请使用 `GROUP BY` 子句和用于处理符号的聚合函数编写请求。例如，要计算数量，使用 `sum(Sign)` 而不是 `count()`。要计算某物的总和，使用 `sum(Sign * x)` 而不是 `sum(x)`，并添加 `HAVING sum(Sign) > 0` 子句。

聚合体 `count`,`sum` 和 `avg` 可以用这种方式计算。如果一个对象至少有一个未被折叠的状态，则可以计算 `uniq` 聚合。`min` 和 `max` 聚合无法计算，因为 `CollaspingMergeTree` 不会保存折叠状态的值的历史记录。

如果你需要在不进行聚合的情况下获取数据（例如，要检查是否存在最新值与特定条件匹配的行），你可以在 `FROM` 从句中使用 `FINAL` 修饰符。这种方法显然是更低效的。

## 示例 {#shi-li}

示例数据:

    ┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
    │ 4324182021466249494 │         5 │      146 │    1 │
    │ 4324182021466249494 │         5 │      146 │   -1 │
    │ 4324182021466249494 │         6 │      185 │    1 │
    └─────────────────────┴───────────┴──────────┴──────┘

建表:

``` sql
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

插入数据:

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1)
```

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1),(4324182021466249494, 6, 185, 1)
```

我们使用两次 `INSERT` 请求来创建两个不同的数据片段。如果我们使用一个请求插入数据，ClickHouse 只会创建一个数据片段且不会执行任何合并操作。

获取数据：

    SELECT * FROM UAct

    ┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
    │ 4324182021466249494 │         5 │      146 │   -1 │
    │ 4324182021466249494 │         6 │      185 │    1 │
    └─────────────────────┴───────────┴──────────┴──────┘
    ┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
    │ 4324182021466249494 │         5 │      146 │    1 │
    └─────────────────────┴───────────┴──────────┴──────┘

我们看到了什么，哪里有折叠？

通过两个 `INSERT` 请求，我们创建了两个数据片段。`SELECT` 请求在两个线程中被执行，我们得到了随机顺序的行。没有发生折叠是因为还没有合并数据片段。ClickHouse 在一个我们无法预料的未知时刻合并数据片段。

因此我们需要聚合：

``` sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration
FROM UAct
GROUP BY UserID
HAVING sum(Sign) > 0
```

    ┌──────────────UserID─┬─PageViews─┬─Duration─┐
    │ 4324182021466249494 │         6 │      185 │
    └─────────────────────┴───────────┴──────────┘

如果我们不需要聚合并想要强制进行折叠，我们可以在 `FROM` 从句中使用 `FINAL` 修饰语。

``` sql
SELECT * FROM UAct FINAL
```

    ┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
    │ 4324182021466249494 │         6 │      185 │    1 │
    └─────────────────────┴───────────┴──────────┴──────┘

这种查询数据的方法是非常低效的。不要在大表中使用它。

[来源文章](https://clickhouse.tech/docs/en/operations/table_engines/collapsingmergetree/) <!--hide-->
