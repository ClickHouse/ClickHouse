---
machine_translated: true
machine_translated_rev: 0f7ef7704d018700049223525bad4a63911b6e70
toc_priority: 33
toc_title: SELECT
---

# 选择查询语法 {#select-queries-syntax}

`SELECT` 执行数据检索。

``` sql
[WITH expr_list|(subquery)]
SELECT [DISTINCT] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] [ANY|ALL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER] JOIN (subquery)|table USING columns_list
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH TOTALS]
[HAVING expr]
[ORDER BY expr_list]
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m]
[UNION ALL ...]
[INTO OUTFILE filename]
[FORMAT format]
```

所有子句都是可选的，除了紧接在SELECT之后的必需表达式列表。
以下子句的描述顺序几乎与查询执行传送器中的顺序相同。

如果查询省略 `DISTINCT`, `GROUP BY` 和 `ORDER BY` 条款和 `IN` 和 `JOIN` 子查询，查询将被完全流处理，使用O(1)量的RAM。
否则，如果未指定适当的限制，则查询可能会消耗大量RAM: `max_memory_usage`, `max_rows_to_group_by`, `max_rows_to_sort`, `max_rows_in_distinct`, `max_bytes_in_distinct`, `max_rows_in_set`, `max_bytes_in_set`, `max_rows_in_join`, `max_bytes_in_join`, `max_bytes_before_external_sort`, `max_bytes_before_external_group_by`. 有关详细信息，请参阅部分 “Settings”. 可以使用外部排序（将临时表保存到磁盘）和外部聚合。 `The system does not have "merge join"`.

### WITH条款 {#with-clause}

本节提供对公共表表达式的支持 ([CTE](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL)），有一些限制:
1. 不支持递归查询
2. 当在section中使用子查询时，它的结果应该是只有一行的标量
3. Expression的结果在子查询中不可用
WITH子句表达式的结果可以在SELECT子句中使用。

示例1：使用常量表达式作为 “variable”

``` sql
WITH '2019-08-01 15:23:00' as ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound
```

示例2：从SELECT子句列表中逐出sum(bytes)表达式结果

``` sql
WITH sum(bytes) as s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s
```

示例3：使用标量子查询的结果

``` sql
/* this example would return TOP 10 of most huge tables */
WITH
    (
        SELECT sum(bytes)
        FROM system.parts
        WHERE active
    ) AS total_disk_usage
SELECT
    (sum(bytes) / total_disk_usage) * 100 AS table_disk_usage,
    table
FROM system.parts
GROUP BY table
ORDER BY table_disk_usage DESC
LIMIT 10
```

示例4：在子查询中重用表达式
作为子查询中表达式使用的当前限制的解决方法，您可以复制它。

``` sql
WITH ['hello'] AS hello
SELECT
    hello,
    *
FROM
(
    WITH ['hello'] AS hello
    SELECT hello
)
```

``` text
┌─hello─────┬─hello─────┐
│ ['hello'] │ ['hello'] │
└───────────┴───────────┘
```

### FROM条款 {#select-from}

如果FROM子句被省略，数据将从读取 `system.one` 桌子
该 `system.one` 表只包含一行（此表满足与其他Dbms中找到的双表相同的目的）。

该 `FROM` 子句指定从中读取数据的源:

-   表
-   子查询
-   [表函数](../table-functions/index.md)

`ARRAY JOIN` 和常规 `JOIN` 也可以包括在内（见下文）。

而不是一个表，该 `SELECT` 子查询可以在括号中指定。
与标准SQL相比，不需要在子查询后指定同义词。

若要执行查询，将从相应的表中提取查询中列出的所有列。 外部查询不需要的任何列都将从子查询中抛出。
如果查询未列出任何列（例如, `SELECT count() FROM t`），无论如何都会从表中提取一些列（最小的列是首选），以便计算行数。

#### 最终修饰符 {#select-from-final}

从表中选择数据时适用 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-发动机系列比其他 `GraphiteMergeTree`. 当 `FINAL` 如果指定，ClickHouse会在返回结果之前完全合并数据，从而执行给定表引擎合并期间发生的所有数据转换。

还支持:
- [复制](../../engines/table-engines/mergetree-family/replication.md) 版本 `MergeTree` 引擎
- [查看](../../engines/table-engines/special/view.md), [缓冲区](../../engines/table-engines/special/buffer.md), [分布](../../engines/table-engines/special/distributed.md)，和 [MaterializedView](../../engines/table-engines/special/materializedview.md) 在其他引擎上运行的引擎，只要它们是在创建 `MergeTree`-发动机表。

使用的查询 `FINAL` 执行速度不如类似的查询那么快，因为:

-   查询在单个线程中执行，并在查询执行期间合并数据。
-   查询与 `FINAL` 除了读取查询中指定的列之外，还读取主键列。

在大多数情况下，避免使用 `FINAL`.

### 示例子句 {#select-sample-clause}

该 `SAMPLE` 子句允许近似查询处理。

启用数据采样时，不会对所有数据执行查询，而只对特定部分数据（样本）执行查询。 例如，如果您需要计算所有访问的统计信息，只需对所有访问的1/10分数执行查询，然后将结果乘以10即可。

近似查询处理在以下情况下可能很有用:

-   当你有严格的时间requirements（如\<100ms），但你不能证明额外的硬件资源来满足他们的成本。
-   当您的原始数据不准确时，所以近似不会明显降低质量。
-   业务需求的目标是近似结果（为了成本效益，或者为了向高级用户推销确切的结果）。

!!! note "注"
    您只能使用采样中的表 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 家庭，并且只有在表创建过程中指定了采样表达式（请参阅 [MergeTree引擎](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)).

下面列出了数据采样的功能:

-   数据采样是一种确定性机制。 同样的结果 `SELECT .. SAMPLE` 查询始终是相同的。
-   对于不同的表，采样工作始终如一。 对于具有单个采样键的表，具有相同系数的采样总是选择相同的可能数据子集。 例如，用户Id的示例采用来自不同表的所有可能的用户Id的相同子集的行。 这意味着您可以在子查询中使用示例 [IN](#select-in-operators) 条款 此外，您可以使用 [JOIN](#select-join) 条款
-   采样允许从磁盘读取更少的数据。 请注意，您必须正确指定采样键。 有关详细信息，请参阅 [创建MergeTree表](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table).

为 `SAMPLE` 子句支持以下语法:

| SAMPLE Clause Syntax | 产品描述                                                                                                                                                                          |
|----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SAMPLE k`           | 这里 `k` 是从0到1的数字。</br>查询执行于 `k` 数据的分数。 例如, `SAMPLE 0.1` 对10%的数据运行查询。 [碌莽禄more拢more](#select-sample-k)                                           |
| `SAMPLE n`           | 这里 `n` 是足够大的整数。</br>该查询是在至少一个样本上执行的 `n` 行（但不超过这个）。 例如, `SAMPLE 10000000` 在至少10,000,000行上运行查询。 [碌莽禄more拢more](#select-sample-n) |
| `SAMPLE k OFFSET m`  | 这里 `k` 和 `m` 是从0到1的数字。</br>查询在以下示例上执行 `k` 数据的分数。 用于采样的数据由以下偏移 `m` 分数。 [碌莽禄more拢more](#select-sample-offset)                          |

#### SAMPLE K {#select-sample-k}

这里 `k` 从0到1的数字（支持小数和小数表示法）。 例如, `SAMPLE 1/2` 或 `SAMPLE 0.5`.

在一个 `SAMPLE k` 子句，样品是从 `k` 数据的分数。 示例如下所示:

``` sql
SELECT
    Title,
    count() * 10 AS PageViews
FROM hits_distributed
SAMPLE 0.1
WHERE
    CounterID = 34
GROUP BY Title
ORDER BY PageViews DESC LIMIT 1000
```

在此示例中，对0.1(10%)数据的样本执行查询。 聚合函数的值不会自动修正，因此要获得近似结果，值 `count()` 手动乘以10。

#### SAMPLE N {#select-sample-n}

这里 `n` 是足够大的整数。 例如, `SAMPLE 10000000`.

在这种情况下，查询在至少一个样本上执行 `n` 行（但不超过这个）。 例如, `SAMPLE 10000000` 在至少10,000,000行上运行查询。

由于数据读取的最小单位是一个颗粒（其大小由 `index_granularity` 设置），是有意义的设置一个样品，其大小远大于颗粒。

使用时 `SAMPLE n` 子句，你不知道处理了哪些数据的相对百分比。 所以你不知道聚合函数应该乘以的系数。 使用 `_sample_factor` 虚拟列得到近似结果。

该 `_sample_factor` 列包含动态计算的相对系数。 当您执行以下操作时，将自动创建此列 [创建](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) 具有指定采样键的表。 的使用示例 `_sample_factor` 列如下所示。

让我们考虑表 `visits`，其中包含有关网站访问的统计信息。 第一个示例演示如何计算页面浏览量:

``` sql
SELECT sum(PageViews * _sample_factor)
FROM visits
SAMPLE 10000000
```

下一个示例演示如何计算访问总数:

``` sql
SELECT sum(_sample_factor)
FROM visits
SAMPLE 10000000
```

下面的示例显示了如何计算平均会话持续时间。 请注意，您不需要使用相对系数来计算平均值。

``` sql
SELECT avg(Duration)
FROM visits
SAMPLE 10000000
```

#### SAMPLE K OFFSET M {#select-sample-offset}

这里 `k` 和 `m` 是从0到1的数字。 示例如下所示。

**示例1**

``` sql
SAMPLE 1/10
```

在此示例中，示例是所有数据的十分之一:

`[++------------]`

**示例2**

``` sql
SAMPLE 1/10 OFFSET 1/2
```

这里，从数据的后半部分取出10％的样本。

`[------++------]`

### ARRAY JOIN子句 {#select-array-join-clause}

允许执行 `JOIN` 具有数组或嵌套数据结构。 意图类似于 [arrayJoin](../functions/array-join.md#functions_arrayjoin) 功能，但其功能更广泛。

``` sql
SELECT <expr_list>
FROM <left_subquery>
[LEFT] ARRAY JOIN <array>
[WHERE|PREWHERE <expr>]
...
```

您只能指定一个 `ARRAY JOIN` 查询中的子句。

运行时优化查询执行顺序 `ARRAY JOIN`. 虽然 `ARRAY JOIN` 必须始终之前指定 `WHERE/PREWHERE` 子句，它可以执行之前 `WHERE/PREWHERE` （如果结果是需要在这个子句），或完成后（以减少计算量）。 处理顺序由查询优化器控制。

支持的类型 `ARRAY JOIN` 下面列出:

-   `ARRAY JOIN` -在这种情况下，空数组不包括在结果中 `JOIN`.
-   `LEFT ARRAY JOIN` -的结果 `JOIN` 包含具有空数组的行。 空数组的值设置为数组元素类型的默认值（通常为0、空字符串或NULL）。

下面的例子演示的用法 `ARRAY JOIN` 和 `LEFT ARRAY JOIN` 条款 让我们创建一个表 [阵列](../../sql-reference/data-types/array.md) 键入column并在其中插入值:

``` sql
CREATE TABLE arrays_test
(
    s String,
    arr Array(UInt8)
) ENGINE = Memory;

INSERT INTO arrays_test
VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);
```

``` text
┌─s───────────┬─arr─────┐
│ Hello       │ [1,2]   │
│ World       │ [3,4,5] │
│ Goodbye     │ []      │
└─────────────┴─────────┘
```

下面的例子使用 `ARRAY JOIN` 条款:

``` sql
SELECT s, arr
FROM arrays_test
ARRAY JOIN arr;
```

``` text
┌─s─────┬─arr─┐
│ Hello │   1 │
│ Hello │   2 │
│ World │   3 │
│ World │   4 │
│ World │   5 │
└───────┴─────┘
```

下一个示例使用 `LEFT ARRAY JOIN` 条款:

``` sql
SELECT s, arr
FROM arrays_test
LEFT ARRAY JOIN arr;
```

``` text
┌─s───────────┬─arr─┐
│ Hello       │   1 │
│ Hello       │   2 │
│ World       │   3 │
│ World       │   4 │
│ World       │   5 │
│ Goodbye     │   0 │
└─────────────┴─────┘
```

#### 使用别名 {#using-aliases}

可以为数组中的别名指定 `ARRAY JOIN` 条款 在这种情况下，数组项目可以通过此别名访问，但数组本身可以通过原始名称访问。 示例:

``` sql
SELECT s, arr, a
FROM arrays_test
ARRAY JOIN arr AS a;
```

``` text
┌─s─────┬─arr─────┬─a─┐
│ Hello │ [1,2]   │ 1 │
│ Hello │ [1,2]   │ 2 │
│ World │ [3,4,5] │ 3 │
│ World │ [3,4,5] │ 4 │
│ World │ [3,4,5] │ 5 │
└───────┴─────────┴───┘
```

使用别名，您可以执行 `ARRAY JOIN` 与外部阵列。 例如:

``` sql
SELECT s, arr_external
FROM arrays_test
ARRAY JOIN [1, 2, 3] AS arr_external;
```

``` text
┌─s───────────┬─arr_external─┐
│ Hello       │            1 │
│ Hello       │            2 │
│ Hello       │            3 │
│ World       │            1 │
│ World       │            2 │
│ World       │            3 │
│ Goodbye     │            1 │
│ Goodbye     │            2 │
│ Goodbye     │            3 │
└─────────────┴──────────────┘
```

多个数组可以在逗号分隔 `ARRAY JOIN` 条款 在这种情况下, `JOIN` 与它们同时执行（直接和，而不是笛卡尔积）。 请注意，所有数组必须具有相同的大小。 示例:

``` sql
SELECT s, arr, a, num, mapped
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num, arrayMap(x -> x + 1, arr) AS mapped;
```

``` text
┌─s─────┬─arr─────┬─a─┬─num─┬─mapped─┐
│ Hello │ [1,2]   │ 1 │   1 │      2 │
│ Hello │ [1,2]   │ 2 │   2 │      3 │
│ World │ [3,4,5] │ 3 │   1 │      4 │
│ World │ [3,4,5] │ 4 │   2 │      5 │
│ World │ [3,4,5] │ 5 │   3 │      6 │
└───────┴─────────┴───┴─────┴────────┘
```

下面的例子使用 [arrayEnumerate](../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate) 功能:

``` sql
SELECT s, arr, a, num, arrayEnumerate(arr)
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num;
```

``` text
┌─s─────┬─arr─────┬─a─┬─num─┬─arrayEnumerate(arr)─┐
│ Hello │ [1,2]   │ 1 │   1 │ [1,2]               │
│ Hello │ [1,2]   │ 2 │   2 │ [1,2]               │
│ World │ [3,4,5] │ 3 │   1 │ [1,2,3]             │
│ World │ [3,4,5] │ 4 │   2 │ [1,2,3]             │
│ World │ [3,4,5] │ 5 │   3 │ [1,2,3]             │
└───────┴─────────┴───┴─────┴─────────────────────┘
```

#### 具有嵌套数据结构的数组连接 {#array-join-with-nested-data-structure}

`ARRAY`加入"也适用于 [嵌套数据结构](../../sql-reference/data-types/nested-data-structures/nested.md). 示例:

``` sql
CREATE TABLE nested_test
(
    s String,
    nest Nested(
    x UInt8,
    y UInt32)
) ENGINE = Memory;

INSERT INTO nested_test
VALUES ('Hello', [1,2], [10,20]), ('World', [3,4,5], [30,40,50]), ('Goodbye', [], []);
```

``` text
┌─s───────┬─nest.x──┬─nest.y─────┐
│ Hello   │ [1,2]   │ [10,20]    │
│ World   │ [3,4,5] │ [30,40,50] │
│ Goodbye │ []      │ []         │
└─────────┴─────────┴────────────┘
```

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

当指定嵌套数据结构的名称 `ARRAY JOIN`，意思是一样的 `ARRAY JOIN` 它包含的所有数组元素。 下面列出了示例:

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`, `nest.y`;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

这种变化也是有道理的:

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─────┐
│ Hello │      1 │ [10,20]    │
│ Hello │      2 │ [10,20]    │
│ World │      3 │ [30,40,50] │
│ World │      4 │ [30,40,50] │
│ World │      5 │ [30,40,50] │
└───────┴────────┴────────────┘
```

可以将别名用于嵌套数据结构，以便选择 `JOIN` 结果或源数组。 示例:

``` sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest AS n;
```

``` text
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │
└───────┴─────┴─────┴─────────┴────────────┘
```

使用的例子 [arrayEnumerate](../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate) 功能:

``` sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`, num
FROM nested_test
ARRAY JOIN nest AS n, arrayEnumerate(`nest.x`) AS num;
```

``` text
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┬─num─┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │   1 │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │   2 │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │   1 │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │   2 │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │   3 │
└───────┴─────┴─────┴─────────┴────────────┴─────┘
```

### JOIN子句 {#select-join}

加入正常的数据 [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) 感觉

!!! info "注"
    不相关的 [ARRAY JOIN](#select-array-join-clause).

``` sql
SELECT <expr_list>
FROM <left_subquery>
[GLOBAL] [ANY|ALL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER] JOIN <right_subquery>
(ON <expr_list>)|(USING <column_list>) ...
```

可以指定表名，而不是 `<left_subquery>` 和 `<right_subquery>`. 这相当于 `SELECT * FROM table` 子查询，除了在特殊情况下，当表具有 [加入我们](../../engines/table-engines/special/join.md) engine – an array prepared for joining.

#### 支持的类型 `JOIN` {#select-join-types}

-   `INNER JOIN` （或 `JOIN`)
-   `LEFT JOIN` （或 `LEFT OUTER JOIN`)
-   `RIGHT JOIN` （或 `RIGHT OUTER JOIN`)
-   `FULL JOIN` （或 `FULL OUTER JOIN`)
-   `CROSS JOIN` （或 `,` )

查看标准 [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) 描述。

#### 多联接 {#multiple-join}

执行查询时，ClickHouse将多表联接重写为双表联接的序列。 例如，如果有四个连接表ClickHouse连接第一个和第二个，然后将结果与第三个表连接，并在最后一步，它连接第四个表。

如果查询包含 `WHERE` 子句，ClickHouse尝试通过中间联接从此子句推下过滤器。 如果无法将筛选器应用于每个中间联接，ClickHouse将在所有联接完成后应用筛选器。

我们建议 `JOIN ON` 或 `JOIN USING` 用于创建查询的语法。 例如:

``` sql
SELECT * FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t1.a = t3.a
```

您可以使用逗号分隔的列表中的表 `FROM` 条款 例如:

``` sql
SELECT * FROM t1, t2, t3 WHERE t1.a = t2.a AND t1.a = t3.a
```

不要混合使用这些语法。

ClickHouse不直接支持使用逗号的语法，所以我们不建议使用它们。 该算法尝试重写查询 `CROSS JOIN` 和 `INNER JOIN` 子句，然后继续进行查询处理。 重写查询时，ClickHouse会尝试优化性能和内存消耗。 默认情况下，ClickHouse将逗号视为 `INNER JOIN` 子句和转换 `INNER JOIN` 到 `CROSS JOIN` 当算法不能保证 `INNER JOIN` 返回所需的数据。

#### 严格 {#select-join-strictness}

-   `ALL` — If the right table has several matching rows, ClickHouse creates a [笛卡尔积](https://en.wikipedia.org/wiki/Cartesian_product) 从匹配的行。 这是标准 `JOIN` SQL中的行为。
-   `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of queries with `ANY` 和 `ALL` 关键字是相同的。
-   `ASOF` — For joining sequences with a non-exact match. `ASOF JOIN` 用法描述如下。

**ASOF加入使用**

`ASOF JOIN` 当您需要连接没有完全匹配的记录时非常有用。

表 `ASOF JOIN` 必须具有有序序列列。 此列不能单独存在于表中，并且应该是其中一种数据类型: `UInt32`, `UInt64`, `Float32`, `Float64`, `Date`，和 `DateTime`.

语法 `ASOF JOIN ... ON`:

``` sql
SELECT expressions_list
FROM table_1
ASOF LEFT JOIN table_2
ON equi_cond AND closest_match_cond
```

您可以使用任意数量的相等条件和恰好一个最接近的匹配条件。 例如, `SELECT count() FROM table_1 ASOF LEFT JOIN table_2 ON table_1.a == table_2.b AND table_2.t <= table_1.t`.

支持最接近匹配的条件: `>`, `>=`, `<`, `<=`.

语法 `ASOF JOIN ... USING`:

``` sql
SELECT expressions_list
FROM table_1
ASOF JOIN table_2
USING (equi_column1, ... equi_columnN, asof_column)
```

`ASOF JOIN` 用途 `equi_columnX` 对于加入平等和 `asof_column` 用于加入与最接近的比赛 `table_1.asof_column >= table_2.asof_column` 条件。 该 `asof_column` 列总是在最后一个 `USING` 条款

例如，请考虑下表:

         table_1                           table_2
      event   | ev_time | user_id       event   | ev_time | user_id
    ----------|---------|----------   ----------|---------|----------
                  ...                               ...
    event_1_1 |  12:00  |  42         event_2_1 |  11:59  |   42
                  ...                 event_2_2 |  12:30  |   42
    event_1_2 |  13:00  |  42         event_2_3 |  13:00  |   42
                  ...                               ...

`ASOF JOIN` 可以从用户事件的时间戳 `table_1` 并找到一个事件 `table_2` 其中时间戳最接近事件的时间戳 `table_1` 对应于最接近的匹配条件。 如果可用，则相等的时间戳值是最接近的值。 在这里，该 `user_id` 列可用于连接相等和 `ev_time` 列可用于在最接近的匹配加入。 在我们的例子中, `event_1_1` 可以加入 `event_2_1` 和 `event_1_2` 可以加入 `event_2_3`，但是 `event_2_2` 不能加入。

!!! note "注"
    `ASOF` 加入是 **不** 支持在 [加入我们](../../engines/table-engines/special/join.md) 表引擎。

若要设置默认严格性值，请使用session configuration参数 [join\_default\_strictness](../../operations/settings/settings.md#settings-join_default_strictness).

#### GLOBAL JOIN {#global-join}

当使用正常 `JOIN`，将查询发送到远程服务器。 为了创建正确的表，在每个子查询上运行子查询，并使用此表执行联接。 换句话说，在每个服务器上单独形成右表。

使用时 `GLOBAL ... JOIN`，首先请求者服务器运行一个子查询来计算正确的表。 此临时表将传递到每个远程服务器，并使用传输的临时数据对其运行查询。

使用时要小心 `GLOBAL`. 有关详细信息，请参阅部分 [分布式子查询](#select-distributed-subqueries).

#### 使用建议 {#usage-recommendations}

当运行 `JOIN`，与查询的其他阶段相关的执行顺序没有优化。 连接（在右表中搜索）在过滤之前运行 `WHERE` 和聚集之前。 为了明确设置处理顺序，我们建议运行 `JOIN` 具有子查询的子查询。

示例:

``` sql
SELECT
    CounterID,
    hits,
    visits
FROM
(
    SELECT
        CounterID,
        count() AS hits
    FROM test.hits
    GROUP BY CounterID
) ANY LEFT JOIN
(
    SELECT
        CounterID,
        sum(Sign) AS visits
    FROM test.visits
    GROUP BY CounterID
) USING CounterID
ORDER BY hits DESC
LIMIT 10
```

``` text
┌─CounterID─┬───hits─┬─visits─┐
│   1143050 │ 523264 │  13665 │
│    731962 │ 475698 │ 102716 │
│    722545 │ 337212 │ 108187 │
│    722889 │ 252197 │  10547 │
│   2237260 │ 196036 │   9522 │
│  23057320 │ 147211 │   7689 │
│    722818 │  90109 │  17847 │
│     48221 │  85379 │   4652 │
│  19762435 │  77807 │   7026 │
│    722884 │  77492 │  11056 │
└───────────┴────────┴────────┘
```

子查询不允许您设置名称或使用它们从特定子查询引用列。
在指定的列 `USING` 两个子查询中必须具有相同的名称，并且其他列必须以不同的方式命名。 您可以使用别名更改子查询中的列名（此示例使用别名 `hits` 和 `visits`).

该 `USING` 子句指定一个或多个要联接的列，这将建立这些列的相等性。 列的列表设置不带括号。 不支持更复杂的连接条件。

正确的表（子查询结果）驻留在RAM中。 如果没有足够的内存，则无法运行 `JOIN`.

每次使用相同的查询运行 `JOIN`，子查询再次运行，因为结果未缓存。 为了避免这种情况，使用特殊的 [加入我们](../../engines/table-engines/special/join.md) 表引擎，它是一个用于连接的准备好的数组，总是在RAM中。

在某些情况下，使用效率更高 `IN` 而不是 `JOIN`.
在各种类型的 `JOIN`，最有效的是 `ANY LEFT JOIN`，然后 `ANY INNER JOIN`. 效率最低的是 `ALL LEFT JOIN` 和 `ALL INNER JOIN`.

如果你需要一个 `JOIN` 对于连接维度表（这些是包含维度属性的相对较小的表，例如广告活动的名称）， `JOIN` 由于每个查询都会重新访问正确的表，因此可能不太方便。 对于这种情况下，有一个 “external dictionaries” 您应该使用的功能 `JOIN`. 有关详细信息，请参阅部分 [外部字典](../dictionaries/external-dictionaries/external-dicts.md).

**内存限制**

ClickHouse使用 [哈希联接](https://en.wikipedia.org/wiki/Hash_join) 算法。 ClickHouse采取 `<right_subquery>` 并在RAM中为其创建哈希表。 如果需要限制联接操作内存消耗，请使用以下设置:

-   [max\_rows\_in\_join](../../operations/settings/query-complexity.md#settings-max_rows_in_join) — Limits number of rows in the hash table.
-   [max\_bytes\_in\_join](../../operations/settings/query-complexity.md#settings-max_bytes_in_join) — Limits size of the hash table.

当任何这些限制达到，ClickHouse作为 [join\_overflow\_mode](../../operations/settings/query-complexity.md#settings-join_overflow_mode) 设置指示。

#### 处理空单元格或空单元格 {#processing-of-empty-or-null-cells}

在连接表时，可能会出现空单元格。 设置 [join\_use\_nulls](../../operations/settings/settings.md#join_use_nulls) 定义ClickHouse如何填充这些单元格。

如果 `JOIN` 键是 [可为空](../data-types/nullable.md) 字段，其中至少有一个键具有值的行 [NULL](../syntax.md#null-literal) 没有加入。

#### 语法限制 {#syntax-limitations}

对于多个 `JOIN` 单个子句 `SELECT` 查询:

-   通过以所有列 `*` 仅在联接表时才可用，而不是子查询。
-   该 `PREWHERE` 条款不可用。

为 `ON`, `WHERE`，和 `GROUP BY` 条款:

-   任意表达式不能用于 `ON`, `WHERE`，和 `GROUP BY` 子句，但你可以定义一个表达式 `SELECT` 子句，然后通过别名在这些子句中使用它。

### WHERE条款 {#select-where}

如果存在WHERE子句，则必须包含具有UInt8类型的表达式。 这通常是一个带有比较和逻辑运算符的表达式。
此表达式将用于在所有其他转换之前过滤数据。

如果数据库表引擎支持索引，则根据使用索引的能力计算表达式。

### PREWHERE条款 {#prewhere-clause}

此条款与WHERE条款具有相同的含义。 区别在于从表中读取数据。
使用PREWHERE时，首先只读取执行PREWHERE所需的列。 然后读取运行查询所需的其他列，但只读取PREWHERE表达式为true的那些块。

如果查询中的少数列使用过滤条件，但提供强大的数据过滤，则使用PREWHERE是有意义的。 这减少了要读取的数据量。

例如，对于提取大量列但仅对少数列进行过滤的查询，编写PREWHERE非常有用。

PREWHERE仅由来自 `*MergeTree` 家人

查询可以同时指定PREWHERE和WHERE。 在这种情况下，PREWHERE先于WHERE。

如果 ‘optimize\_move\_to\_prewhere’ 设置设置为1并省略PREWHERE，系统使用启发式方法自动将部分表达式从哪里移动到哪里。

### GROUP BY子句 {#select-group-by-clause}

这是面向列的DBMS最重要的部分之一。

如果存在GROUP BY子句，则必须包含表达式列表。 每个表达式将在这里被称为 “key”.
SELECT、HAVING和ORDER BY子句中的所有表达式都必须从键或聚合函数计算。 换句话说，从表中选择的每个列必须在键或聚合函数内使用。

如果查询仅包含聚合函数中的表列，则可以省略GROUP BY子句，并假定通过一组空键进行聚合。

示例:

``` sql
SELECT
    count(),
    median(FetchTiming > 60 ? 60 : FetchTiming),
    count() - sum(Refresh)
FROM hits
```

但是，与标准SQL相比，如果表没有任何行（根本没有任何行，或者在使用WHERE to filter之后没有任何行），则返回一个空结果，而不是来自包含聚合函数初始值的行之

相对于MySQL（并且符合标准SQL），您无法获取不在键或聚合函数（常量表达式除外）中的某些列的某些值。 要解决此问题，您可以使用 ‘any’ 聚合函数（获取第一个遇到的值）或 ‘min/max’.

示例:

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    count(),
    any(Title) AS title -- getting the first occurred page header for each domain.
FROM hits
GROUP BY domain
```

对于遇到的每个不同的键值，GROUP BY计算一组聚合函数值。

数组列不支持分组依据。

不能将常量指定为聚合函数的参数。 示例：sum(1)。 相反，你可以摆脱常数。 示例: `count()`.

#### 空处理 {#null-processing}

对于GROUP BY子句，ClickHouse将 [NULL](../syntax.md#null-literal) 解释为一个值，并且支持`NULL=NULL`。

这里有一个例子来说明这意味着什么。

假设你有这张桌子:

``` text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

查询 `SELECT sum(x), y FROM t_null_big GROUP BY y` 结果:

``` text
┌─sum(x)─┬────y─┐
│      4 │    2 │
│      3 │    3 │
│      5 │ ᴺᵁᴸᴸ │
└────────┴──────┘
```

你可以看到 `GROUP BY` 为 `y = NULL` 总结 `x`，仿佛 `NULL` 是这个值。

如果你通过几个键 `GROUP BY`，结果会给你选择的所有组合，就好像 `NULL` 是一个特定的值。

#### 使用总计修饰符 {#with-totals-modifier}

如果指定了WITH TOTALS修饰符，则将计算另一行。 此行将具有包含默认值（零或空行）的关键列，以及包含跨所有行计算值的聚合函数列（ “total” 值）。

这个额外的行以JSON\*，TabSeparated\*和Pretty\*格式输出，与其他行分开。 在其他格式中，此行不输出。

在JSON\*格式中，此行作为单独的输出 ‘totals’ 场。 在TabSeparated\*格式中，该行位于主结果之后，前面有一个空行（在其他数据之后）。 在Pretty\*格式中，该行在主结果之后作为单独的表输出。

`WITH TOTALS` 当有存在时，可以以不同的方式运行。 该行为取决于 ‘totals\_mode’ 设置。
默认情况下, `totals_mode = 'before_having'`. 在这种情况下, ‘totals’ 是跨所有行计算，包括那些不通过具有和 ‘max\_rows\_to\_group\_by’.

其他替代方案仅包括通过具有在 ‘totals’，并与设置不同的行为 `max_rows_to_group_by` 和 `group_by_overflow_mode = 'any'`.

`after_having_exclusive` – Don't include rows that didn't pass through `max_rows_to_group_by`. 换句话说, ‘totals’ 将有少于或相同数量的行，因为它会 `max_rows_to_group_by` 被省略。

`after_having_inclusive` – Include all the rows that didn't pass through ‘max\_rows\_to\_group\_by’ 在 ‘totals’. 换句话说, ‘totals’ 将有多个或相同数量的行，因为它会 `max_rows_to_group_by` 被省略。

`after_having_auto` – Count the number of rows that passed through HAVING. If it is more than a certain amount (by default, 50%), include all the rows that didn't pass through ‘max\_rows\_to\_group\_by’ 在 ‘totals’. 否则，不包括它们。

`totals_auto_threshold` – By default, 0.5. The coefficient for `after_having_auto`.

如果 `max_rows_to_group_by` 和 `group_by_overflow_mode = 'any'` 不使用，所有的变化 `after_having` 是相同的，你可以使用它们中的任何一个（例如, `after_having_auto`).

您可以在子查询中使用总计，包括在JOIN子句中的子查询（在这种情况下，将合并各自的总计值）。

#### 在外部存储器中分组 {#select-group-by-in-external-memory}

您可以启用将临时数据转储到磁盘以限制内存使用期间 `GROUP BY`.
该 [max\_bytes\_before\_external\_group\_by](../../operations/settings/settings.md#settings-max_bytes_before_external_group_by) 设置确定倾销的阈值RAM消耗 `GROUP BY` 临时数据到文件系统。 如果设置为0（默认值），它将被禁用。

使用时 `max_bytes_before_external_group_by`，我们建议您设置 `max_memory_usage` 大约两倍高。 这是必要的，因为聚合有两个阶段：读取日期和形成中间数据（1）和合并中间数据（2）。 将数据转储到文件系统只能在阶段1中发生。 如果未转储临时数据，则阶段2可能需要与阶段1相同的内存量。

例如，如果 [max\_memory\_usage](../../operations/settings/settings.md#settings_max_memory_usage) 设置为10000000000，你想使用外部聚合，这是有意义的设置 `max_bytes_before_external_group_by` 到10000000000，max\_memory\_usage到20000000000。 当触发外部聚合（如果至少有一个临时数据转储）时，RAM的最大消耗仅略高于 `max_bytes_before_external_group_by`.

通过分布式查询处理，在远程服务器上执行外部聚合。 为了使请求者服务器只使用少量的RAM，设置 `distributed_aggregation_memory_efficient` 到1。

当合并数据刷新到磁盘时，以及当合并来自远程服务器的结果时， `distributed_aggregation_memory_efficient` 设置被启用，消耗高达 `1/256 * the_number_of_threads` 从RAM的总量。

当启用外部聚合时，如果有小于 `max_bytes_before_external_group_by` of data (i.e. data was not flushed), the query runs just as fast as without external aggregation. If any temporary data was flushed, the run time will be several times longer (approximately three times).

如果你有一个 `ORDER BY` 用一个 `LIMIT` 后 `GROUP BY`，然后使用的RAM的量取决于数据的量 `LIMIT`，不是在整个表。 但如果 `ORDER BY` 没有 `LIMIT`，不要忘记启用外部排序 (`max_bytes_before_external_sort`).

### 限制条款 {#limit-by-clause}

与查询 `LIMIT n BY expressions` 子句选择第一个 `n` 每个不同值的行 `expressions`. 的关键 `LIMIT BY` 可以包含任意数量的 [表达式](../syntax.md#syntax-expressions).

ClickHouse支持以下语法:

-   `LIMIT [offset_value, ]n BY expressions`
-   `LIMIT n OFFSET offset_value BY expressions`

在查询处理过程中，ClickHouse会选择按排序键排序的数据。 排序键使用以下命令显式设置 [ORDER BY](#select-order-by) 子句或隐式作为表引擎的属性。 然后ClickHouse应用 `LIMIT n BY expressions` 并返回第一 `n` 每个不同组合的行 `expressions`. 如果 `OFFSET` 被指定，则对于每个数据块属于一个不同的组合 `expressions`，ClickHouse跳过 `offset_value` 从块开始的行数，并返回最大值 `n` 行的结果。 如果 `offset_value` 如果数据块中的行数大于数据块中的行数，ClickHouse将从该块返回零行。

`LIMIT BY` 是不相关的 `LIMIT`. 它们都可以在同一个查询中使用。

**例**

样品表:

``` sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by values(1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

查询:

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

该 `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` 查询返回相同的结果。

以下查询返回每个引用的前5个引用 `domain, device_type` 最多可与100行配对 (`LIMIT n BY + LIMIT`).

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100
```

### 有条款 {#having-clause}

允许筛选GROUP BY之后收到的结果，类似于WHERE子句。
WHERE和HAVING的不同之处在于WHERE是在聚合（GROUP BY）之前执行的，而HAVING是在聚合之后执行的。
如果不执行聚合，则不能使用HAVING。

### 按条款订购 {#select-order-by}

ORDER BY子句包含一个表达式列表，每个表达式都可以分配DESC或ASC（排序方向）。 如果未指定方向，则假定ASC。 ASC按升序排序，DESC按降序排序。 排序方向适用于单个表达式，而不适用于整个列表。 示例: `ORDER BY Visits DESC, SearchPhrase`

对于按字符串值排序，可以指定排序规则（比较）。 示例: `ORDER BY SearchPhrase COLLATE 'tr'` -对于按关键字升序排序，使用土耳其字母，不区分大小写，假设字符串是UTF-8编码。 COLLATE可以按顺序独立地为每个表达式指定或不指定。 如果指定了ASC或DESC，则在其后指定COLLATE。 使用COLLATE时，排序始终不区分大小写。

我们只建议使用COLLATE对少量行进行最终排序，因为使用COLLATE进行排序的效率低于正常按字节进行排序的效率。

对于排序表达式列表具有相同值的行，将以任意顺序输出，也可以是不确定的（每次都不同）。
如果省略ORDER BY子句，则行的顺序也是未定义的，并且可能也是不确定的。

`NaN` 和 `NULL` 排序顺序:

-   使用修饰符 `NULLS FIRST` — First `NULL`，然后 `NaN`，然后其他值。
-   使用修饰符 `NULLS LAST` — First the values, then `NaN`，然后 `NULL`.
-   Default — The same as with the `NULLS LAST` 修饰符。

示例:

对于表

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    2 │
│ 1 │  nan │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │  nan │
│ 7 │ ᴺᵁᴸᴸ │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

运行查询 `SELECT * FROM t_null_nan ORDER BY y NULLS FIRST` 获得:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 7 │ ᴺᵁᴸᴸ │
│ 1 │  nan │
│ 6 │  nan │
│ 2 │    2 │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

当对浮点数进行排序时，Nan与其他值是分开的。 无论排序顺序如何，Nan都在最后。 换句话说，对于升序排序，它们被放置为好像它们比所有其他数字大，而对于降序排序，它们被放置为好像它们比其他数字小。

如果除了ORDER BY之外指定了足够小的限制，则使用较少的RAM。 否则，所花费的内存量与用于排序的数据量成正比。 对于分布式查询处理，如果省略GROUP BY，则在远程服务器上部分完成排序，并在请求者服务器上合并结果。 这意味着对于分布式排序，要排序的数据量可以大于单个服务器上的内存量。

如果没有足够的RAM，则可以在外部存储器中执行排序（在磁盘上创建临时文件）。 使用设置 `max_bytes_before_external_sort` 为此目的。 如果将其设置为0（默认值），则禁用外部排序。 如果启用，则当要排序的数据量达到指定的字节数时，将对收集的数据进行排序并转储到临时文件中。 读取所有数据后，将合并所有已排序的文件并输出结果。 文件被写入配置中的/var/lib/clickhouse/tmp/目录（默认情况下，但您可以使用 ‘tmp\_path’ 参数来更改此设置）。

运行查询可能占用的内存比 ‘max\_bytes\_before\_external\_sort’. 因此，此设置的值必须大大小于 ‘max\_memory\_usage’. 例如，如果您的服务器有128GB的RAM，并且您需要运行单个查询，请设置 ‘max\_memory\_usage’ 到100GB，和 ‘max\_bytes\_before\_external\_sort’ 至80GB。

外部排序的工作效率远远低于在RAM中进行排序。

### SELECT子句 {#select-select}

[表达式](../syntax.md#syntax-expressions) 在指定 `SELECT` 子句是在上述子句中的所有操作完成后计算的。 这些表达式的工作方式就好像它们应用于结果中的单独行一样。 如果在表达式 `SELECT` 子句包含聚合函数，然后ClickHouse处理过程中用作其参数的聚合函数和表达式 [GROUP BY](#select-group-by-clause) 聚合。

如果要在结果中包含所有列，请使用星号 (`*`）符号。 例如, `SELECT * FROM ...`.

将结果中的某些列与 [re2](https://en.wikipedia.org/wiki/RE2_(software)) 正则表达式，您可以使用 `COLUMNS` 表达。

``` sql
COLUMNS('regexp')
```

例如，考虑表:

``` sql
CREATE TABLE default.col_names (aa Int8, ab Int8, bc Int8) ENGINE = TinyLog
```

以下查询从包含以下内容的所有列中选择数据 `a` 在他们的名字符号。

``` sql
SELECT COLUMNS('a') FROM col_names
```

``` text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

所选列不按字母顺序返回。

您可以使用多个 `COLUMNS` 查询中的表达式并将函数应用于它们。

例如:

``` sql
SELECT COLUMNS('a'), COLUMNS('c'), toTypeName(COLUMNS('c')) FROM col_names
```

``` text
┌─aa─┬─ab─┬─bc─┬─toTypeName(bc)─┐
│  1 │  1 │  1 │ Int8           │
└────┴────┴────┴────────────────┘
```

由返回的每一列 `COLUMNS` 表达式作为单独的参数传递给函数。 如果函数支持其他参数，您也可以将其他参数传递给函数。 使用函数时要小心。 如果函数不支持您传递给它的参数数，ClickHouse将引发异常。

例如:

``` sql
SELECT COLUMNS('a') + COLUMNS('c') FROM col_names
```

``` text
Received exception from server (version 19.14.1):
Code: 42. DB::Exception: Received from localhost:9000. DB::Exception: Number of arguments for function plus doesn't match: passed 3, should be 2.
```

在这个例子中, `COLUMNS('a')` 返回两列: `aa` 和 `ab`. `COLUMNS('c')` 返回 `bc` 列。 该 `+` 运算符不能应用于3个参数，因此ClickHouse引发一个带有相关消息的异常。

匹配的列 `COLUMNS` 表达式可以具有不同的数据类型。 如果 `COLUMNS` 不匹配任何列，并且是唯一的表达式 `SELECT`，ClickHouse抛出异常。

### DISTINCT子句 {#select-distinct}

如果指定了DISTINCT，则在结果中所有完全匹配的行集中，只有一行将保留。
结果将与在没有聚合函数的情况下在SELECT中指定的所有字段中指定GROUP BY一样。 但有几个区别，从组通过:

-   DISTINCT可以与GROUP BY一起应用。
-   如果省略ORDER BY并定义LIMIT，则在读取所需数量的不同行后，查询立即停止运行。
-   数据块在处理时输出，而无需等待整个查询完成运行。

如果SELECT至少有一个数组列，则不支持DISTINCT。

`DISTINCT` 适用于 [NULL](../syntax.md) 就好像 `NULL` 是一个特定的值，并且 `NULL=NULL`. 换句话说，在 `DISTINCT` 结果，不同的组合 `NULL` 只发生一次。

ClickHouse支持使用 `DISTINCT` 和 `ORDER BY` 一个查询中不同列的子句。 该 `DISTINCT` 子句之前执行 `ORDER BY` 条款

示例表:

``` text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 1 │ 2 │
│ 3 │ 3 │
│ 2 │ 4 │
└───┴───┘
```

`DISTINCT`可以与 [NULL](../syntax.md#null-literal)一起工作，就好像`NULL`仅是一个特殊的值一样，并且`NULL=NULL`。换而言之，在`DISTINCT`的结果中，与`NULL`不同的组合仅能出现一次。

``` text
┌─a─┐
│ 2 │
│ 1 │
│ 3 │
└───┘
```

如果我们改变排序方向 `SELECT DISTINCT a FROM t1 ORDER BY b DESC`，我们得到以下结果:

``` text
┌─a─┐
│ 3 │
│ 1 │
│ 2 │
└───┘
```

行 `2, 4` 分拣前被切割。

在编程查询时考虑这种实现特异性。

### 限制条款 {#limit-clause}

`LIMIT m` 允许您选择第一个 `m` 结果中的行。

`LIMIT n, m` 允许您选择第一个 `m` 跳过第一个结果后的行 `n` 行。 该 `LIMIT m OFFSET n` 也支持语法。

`n` 和 `m` 必须是非负整数。

如果没有 `ORDER BY` 明确排序结果的子句，结果可能是任意的和不确定的。

### UNION ALL条款 {#union-all-clause}

您可以使用UNION ALL来组合任意数量的查询。 示例:

``` sql
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

只支持UNION ALL。 不支持常规联合（UNION DISTINCT）。 如果您需要UNION DISTINCT，则可以从包含UNION ALL的子查询中编写SELECT DISTINCT。

作为UNION ALL部分的查询可以同时运行，并且它们的结果可以混合在一起。

结果的结构（列的数量和类型）必须与查询匹配。 但列名可能不同。 在这种情况下，最终结果的列名将从第一个查询中获取。 对联合执行类型转换。 例如，如果合并的两个查询具有相同的字段与非-`Nullable` 和 `Nullable` 从兼容类型的类型，由此产生的 `UNION ALL` 有一个 `Nullable` 类型字段。

作为UNION ALL部分的查询不能括在括号中。 ORDER BY和LIMIT应用于单独的查询，而不是最终结果。 如果您需要将转换应用于最终结果，则可以将所有带有UNION ALL的查询放在FROM子句的子查询中。

### INTO OUTFILE条款 {#into-outfile-clause}

添加 `INTO OUTFILE filename` 子句（其中filename是字符串文字），用于将查询输出重定向到指定的文件。
与MySQL相比，该文件是在客户端创建的。 如果具有相同文件名的文件已经存在，则查询将失败。
此功能在命令行客户端和clickhouse-local中可用（通过HTTP接口发送的查询将失败）。

默认输出格式为TabSeparated（与命令行客户端批处理模式相同）。

### 格式子句 {#format-clause}

指定 ‘FORMAT format’ 获取任何指定格式的数据。
为了方便起见，您可以使用它或创建转储。
有关详细信息，请参阅部分 “Formats”.
如果省略FORMAT子句，则使用默认格式，这取决于用于访问数据库的设置和接口。 对于http接口和批处理模式下的命令行客户端，默认格式为TabSeparated。 对于交互模式下的命令行客户端，默认格式为PrettyCompact（它具有吸引力和紧凑的表）。

使用命令行客户端时，数据以内部高效格式传递给客户端。 客户端独立解释查询的FORMAT子句并格式化数据本身（从而减轻网络和服务器的负载）。

### 在运营商 {#select-in-operators}

该 `IN`, `NOT IN`, `GLOBAL IN`，和 `GLOBAL NOT IN` 运算符是单独复盖的，因为它们的功能相当丰富。

运算符的左侧是单列或元组。

例:

``` sql
SELECT UserID IN (123, 456) FROM ...
SELECT (CounterID, UserID) IN ((34, 123), (101500, 456)) FROM ...
```

如果左侧是索引中的单列，而右侧是一组常量，则系统将使用索引处理查询。

Don't list too many values explicitly (i.e. millions). If a data set is large, put it in a temporary table (for example, see the section “External data for query processing”），然后使用子查询。

运算符的右侧可以是一组常量表达式、一组带有常量表达式的元组（如上面的示例所示），或括号中的数据库表或SELECT子查询的名称。

如果运算符的右侧是表的名称（例如, `UserID IN users`），这相当于子查询 `UserID IN (SELECT * FROM users)`. 使用与查询一起发送的外部数据时，请使用此选项。 例如，查询可以与一组用户Id一起发送到 ‘users’ 应过滤的临时表。

如果运算符的右侧是具有Set引擎的表名（始终位于RAM中的准备好的数据集），则不会为每个查询重新创建数据集。

子查询可以指定多个用于筛选元组的列。
示例:

``` sql
SELECT (CounterID, UserID) IN (SELECT CounterID, UserID FROM ...) FROM ...
```

IN运算符左侧和右侧的列应具有相同的类型。

IN运算符和子查询可能出现在查询的任何部分，包括聚合函数和lambda函数。
示例:

``` sql
SELECT
    EventDate,
    avg(UserID IN
    (
        SELECT UserID
        FROM test.hits
        WHERE EventDate = toDate('2014-03-17')
    )) AS ratio
FROM test.hits
GROUP BY EventDate
ORDER BY EventDate ASC
```

``` text
┌──EventDate─┬────ratio─┐
│ 2014-03-17 │        1 │
│ 2014-03-18 │ 0.807696 │
│ 2014-03-19 │ 0.755406 │
│ 2014-03-20 │ 0.723218 │
│ 2014-03-21 │ 0.697021 │
│ 2014-03-22 │ 0.647851 │
│ 2014-03-23 │ 0.648416 │
└────────────┴──────────┘
```

对于3月17日后的每一天，计算3月17日访问该网站的用户所做的浏览量百分比。
IN子句中的子查询始终只在单个服务器上运行一次。 没有依赖子查询。

#### 空处理 {#null-processing-1}

在处理中，IN操作符总是假定 [NULL](../syntax.md#null-literal) 值的操作结果总是等于`0`，而不管`NULL`位于左侧还是右侧。`NULL`值不应该包含在任何数据集中，它们彼此不能够对应，并且不能够比较。

下面是一个例子 `t_null` 表:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

运行查询 `SELECT x FROM t_null WHERE y IN (NULL,3)` 为您提供以下结果:

``` text
┌─x─┐
│ 2 │
└───┘
```

你可以看到，在其中的行 `y = NULL` 被抛出的查询结果。 这是因为ClickHouse无法决定是否 `NULL` 包含在 `(NULL,3)` 设置，返回 `0` 作为操作的结果，和 `SELECT` 从最终输出中排除此行。

``` sql
SELECT y IN (NULL, 3)
FROM t_null
```

``` text
┌─in(y, tuple(NULL, 3))─┐
│                     0 │
│                     1 │
└───────────────────────┘
```

#### 分布式子查询 {#select-distributed-subqueries}

带子查询的IN-s有两个选项（类似于连接）：normal `IN` / `JOIN` 和 `GLOBAL IN` / `GLOBAL JOIN`. 它们在分布式查询处理的运行方式上有所不同。

!!! attention "注意"
    请记住，下面描述的算法可能会有不同的工作方式取决于 [设置](../../operations/settings/settings.md) `distributed_product_mode` 设置。

当使用常规IN时，查询被发送到远程服务器，并且它们中的每个服务器都在运行子查询 `IN` 或 `JOIN` 条款

使用时 `GLOBAL IN` / `GLOBAL JOINs`，首先所有的子查询都运行 `GLOBAL IN` / `GLOBAL JOINs`，并将结果收集在临时表中。 然后将临时表发送到每个远程服务器，其中使用此临时数据运行查询。

对于非分布式查询，请使用常规 `IN` / `JOIN`.

在使用子查询时要小心 `IN` / `JOIN` 用于分布式查询处理的子句。

让我们来看看一些例子。 假设集群中的每个服务器都有一个正常的 **local\_table**. 每个服务器还具有 **distributed\_table** 表与 **分布** 类型，它查看群集中的所有服务器。

对于查询 **distributed\_table**，查询将被发送到所有远程服务器，并使用以下命令在其上运行 **local\_table**.

例如，查询

``` sql
SELECT uniq(UserID) FROM distributed_table
```

将被发送到所有远程服务器

``` sql
SELECT uniq(UserID) FROM local_table
```

并且并行运行它们中的每一个，直到达到可以结合中间结果的阶段。 然后将中间结果返回给请求者服务器并在其上合并，并将最终结果发送给客户端。

现在让我们检查一个查询IN:

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

-   计算两个网站的受众的交集。

此查询将以下列方式发送到所有远程服务器

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

换句话说，IN子句中的数据集将在每台服务器上独立收集，仅在每台服务器上本地存储的数据中收集。

如果您已经为此情况做好准备，并且已经将数据分散到群集服务器上，以便单个用户Id的数据完全驻留在单个服务器上，则这将正常和最佳地工作。 在这种情况下，所有必要的数据将在每台服务器上本地提供。 否则，结果将是不准确的。 我们将查询的这种变体称为 “local IN”.

若要更正数据在群集服务器上随机传播时查询的工作方式，可以指定 **distributed\_table** 在子查询中。 查询如下所示:

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

此查询将以下列方式发送到所有远程服务器

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

子查询将开始在每个远程服务器上运行。 由于子查询使用分布式表，因此每个远程服务器上的子查询将重新发送到每个远程服务器

``` sql
SELECT UserID FROM local_table WHERE CounterID = 34
```

例如，如果您有100台服务器的集群，则执行整个查询将需要10,000个基本请求，这通常被认为是不可接受的。

在这种情况下，应始终使用GLOBAL IN而不是IN。 让我们来看看它是如何工作的查询

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID GLOBAL IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

请求者服务器将运行子查询

``` sql
SELECT UserID FROM distributed_table WHERE CounterID = 34
```

结果将被放在RAM中的临时表中。 然后请求将被发送到每个远程服务器

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID GLOBAL IN _data1
```

和临时表 `_data1` 将通过查询发送到每个远程服务器（临时表的名称是实现定义的）。

这比使用正常IN更优化。 但是，请记住以下几点:

1.  创建临时表时，数据不是唯一的。 要减少通过网络传输的数据量，请在子查询中指定DISTINCT。 （你不需要为正常人做这个。)
2.  临时表将被发送到所有远程服务器。 传输不考虑网络拓扑。 例如，如果10个远程服务器驻留在与请求者服务器非常远程的数据中心中，则数据将通过通道发送10次到远程数据中心。 使用GLOBAL IN时尽量避免使用大型数据集。
3.  将数据传输到远程服务器时，无法配置网络带宽限制。 您可能会使网络过载。
4.  尝试跨服务器分发数据，以便您不需要定期使用GLOBAL IN。
5.  如果您需要经常使用GLOBAL IN，请规划ClickHouse集群的位置，以便单个副本组驻留在不超过一个数据中心中，并且它们之间具有快速网络，以便可以完全在单个数据中心内处理查询。

这也是有意义的，在指定一个本地表 `GLOBAL IN` 子句，以防此本地表仅在请求者服务器上可用，并且您希望在远程服务器上使用来自它的数据。

### 极端值 {#extreme-values}

除了结果之外，还可以获取结果列的最小值和最大值。 要做到这一点，设置 **极端** 设置为1。 最小值和最大值是针对数字类型、日期和带有时间的日期计算的。 对于其他列，默认值为输出。

An extra two rows are calculated – the minimums and maximums, respectively. These extra two rows are output in `JSON*`, `TabSeparated*`，和 `Pretty*` [格式](../../interfaces/formats.md)，与其他行分开。 它们不是其他格式的输出。

在 `JSON*` 格式时，极端值在一个单独的输出 ‘extremes’ 场。 在 `TabSeparated*` 格式中，该行来的主要结果之后，和之后 ‘totals’ 如果存在。 它前面有一个空行（在其他数据之后）。 在 `Pretty*` 格式中，该行被输出为一个单独的表之后的主结果，和之后 `totals` 如果存在。

极值计算之前的行 `LIMIT`，但之后 `LIMIT BY`. 但是，使用时 `LIMIT offset, size`，之前的行 `offset` 都包含在 `extremes`. 在流请求中，结果还可能包括少量通过的行 `LIMIT`.

### 注 {#notes}

该 `GROUP BY` 和 `ORDER BY` 子句不支持位置参数。 这与MySQL相矛盾，但符合标准SQL。
例如, `GROUP BY 1, 2` will be interpreted as grouping by constants (i.e. aggregation of all rows into one).

您可以使用同义词 (`AS` 别名）在查询的任何部分。

您可以在查询的任何部分而不是表达式中添加星号。 分析查询时，星号将展开为所有表列的列表（不包括 `MATERIALIZED` 和 `ALIAS` 列）。 只有少数情况下使用星号是合理的:

-   创建表转储时。
-   对于只包含几列的表，例如系统表。
-   获取有关表中哪些列的信息。 在这种情况下，设置 `LIMIT 1`. 但最好使用 `DESC TABLE` 查询。
-   当对少量柱进行强过滤时，使用 `PREWHERE`.
-   在子查询中（因为外部查询不需要的列从子查询中排除）。

在所有其他情况下，我们不建议使用星号，因为它只给你一个列DBMS的缺点，而不是优点。 换句话说，不建议使用星号。

[原始文章](https://clickhouse.tech/docs/en/query_language/select/) <!--hide-->
