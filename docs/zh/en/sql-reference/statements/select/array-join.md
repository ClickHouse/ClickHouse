---
description: 'ARRAY JOIN 子句文档'
sidebar_label: 'ARRAY JOIN'
slug: /sql-reference/statements/select/array-join
title: 'ARRAY JOIN 子句'
doc_type: 'reference'
---

对于包含数组列的表，一种常见操作是生成一个新表：将初始列中的每个数组元素分别展开为一行，同时复制其他列的值。这就是 `ARRAY JOIN` 子句最基本的用法。

它之所以叫这个名字，是因为可以将其看作对数组或嵌套数据结构执行 `JOIN`。它的用途与 [arrayJoin](/zh/sql-reference/functions/array-join) 函数类似，但子句的功能范围更广。

语法：

```sql
SELECT <expr_list>
FROM <left_subquery>
[LEFT] ARRAY JOIN <array>
[WHERE|PREWHERE <expr>]
...
```

支持的 `ARRAY JOIN` 类型如下：

* `ARRAY JOIN` - 在默认情况下，空数组不会出现在 `JOIN` 结果中。
* `LEFT ARRAY JOIN` - `JOIN` 结果中会包含空数组对应的行。对于空数组，其值会被设置为数组元素类型的默认值 (通常为 0、空字符串或 NULL) 。

<div id="basic-array-join-examples">
  ## ARRAY JOIN 基础示例
</div>

<div id="array-join-left-array-join-examples">
  ### ARRAY JOIN 和 LEFT ARRAY JOIN
</div>

下面的示例展示了 `ARRAY JOIN` 和 `LEFT ARRAY JOIN` 子句的用法。我们先创建一个包含 [Array](../../../sql-reference/data-types/array.md) 类型列的表，并向其中插入值：

```sql
CREATE TABLE arrays_test
(
    s String,
    arr Array(UInt8)
) ENGINE = Memory;

INSERT INTO arrays_test
VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);
```

```response
┌─s───────────┬─arr─────┐
│ Hello       │ [1,2]   │
│ World       │ [3,4,5] │
│ Goodbye     │ []      │
└─────────────┴─────────┘
```

以下示例使用了 `ARRAY JOIN` 子句：

```sql
SELECT s, arr
FROM arrays_test
ARRAY JOIN arr;
```

```response
┌─s─────┬─arr─┐
│ Hello │   1 │
│ Hello │   2 │
│ World │   3 │
│ World │   4 │
│ World │   5 │
└───────┴─────┘
```

下面的示例使用 `LEFT ARRAY JOIN` 子句：

```sql
SELECT s, arr
FROM arrays_test
LEFT ARRAY JOIN arr;
```

```response
┌─s───────────┬─arr─┐
│ Hello       │   1 │
│ Hello       │   2 │
│ World       │   3 │
│ World       │   4 │
│ World       │   5 │
│ Goodbye     │   0 │
└─────────────┴─────┘
```

<div id="array-join-arrayEnumerate">
  ### ARRAY JOIN 与 arrayEnumerate 函数
</div>

此函数通常与 `ARRAY JOIN` 搭配使用。它可以在应用 `ARRAY JOIN` 后，对每个数组只将某项内容计数一次。例如：

```sql
SELECT
    count() AS Reaches,
    countIf(num = 1) AS Hits
FROM test.hits
ARRAY JOIN
    GoalsReached,
    arrayEnumerate(GoalsReached) AS num
WHERE CounterID = 160656
LIMIT 10
```

```text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

在此示例中，Reaches 是转化次数 (执行 `ARRAY JOIN` 后得到的字符串数量) ，而 Hits 是页面浏览量 (执行 `ARRAY JOIN` 前的字符串数量) 。在这个特定场景下，你可以用更简单的方法得到相同的结果：

```sql
SELECT
    sum(length(GoalsReached)) AS Reaches,
    count() AS Hits
FROM test.hits
WHERE (CounterID = 160656) AND notEmpty(GoalsReached)
```

```text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

<div id="array_join_arrayEnumerateUniq">
  ### ARRAY JOIN 和 arrayEnumerateUniq
</div>

此函数在使用 `ARRAY JOIN` 并对数组元素进行聚合时非常有用。

在此示例中，会计算每个目标 ID 的转化次数 (`Goals` 嵌套数据结构中的每个元素都是一个已达成的目标，我们称之为一次转化) 以及会话数。如果没有 `ARRAY JOIN`，我们会将会话数统计为 sum(Sign)。但在这个特定场景下，行会因嵌套的 `Goals` 结构而成倍增加，因此，为了确保之后每个会话只统计一次，我们需要根据 `arrayEnumerateUniq(Goals.ID)` 函数的值应用一个条件。

```sql
SELECT
    Goals.ID AS GoalID,
    sum(Sign) AS Reaches,
    sumIf(Sign, num = 1) AS Visits
FROM test.visits
ARRAY JOIN
    Goals,
    arrayEnumerateUniq(Goals.ID) AS num
WHERE CounterID = 160656
GROUP BY GoalID
ORDER BY Reaches DESC
LIMIT 10
```

```text
┌──GoalID─┬─Reaches─┬─Visits─┐
│   53225 │    3214 │   1097 │
│ 2825062 │    3188 │   1097 │
│   56600 │    2803 │    488 │
│ 1989037 │    2401 │    365 │
│ 2830064 │    2396 │    910 │
│ 1113562 │    2372 │    373 │
│ 3270895 │    2262 │    812 │
│ 1084657 │    2262 │    345 │
│   56599 │    2260 │    799 │
│ 3271094 │    2256 │    812 │
└─────────┴─────────┴────────┘
```

<div id="using-aliases">
  ## 使用别名
</div>

可以在 `ARRAY JOIN` 子句中为数组指定别名。在这种情况下，可以通过该别名访问数组元素，而数组本身仍通过原始名称访问。示例：

```sql
SELECT s, arr, a
FROM arrays_test
ARRAY JOIN arr AS a;
```

```response
┌─s─────┬─arr─────┬─a─┐
│ Hello │ [1,2]   │ 1 │
│ Hello │ [1,2]   │ 2 │
│ World │ [3,4,5] │ 3 │
│ World │ [3,4,5] │ 4 │
│ World │ [3,4,5] │ 5 │
└───────┴─────────┴───┘
```

使用别名，可以对外部数组进行 `ARRAY JOIN`。例如：

```sql
SELECT s, arr_external
FROM arrays_test
ARRAY JOIN [1, 2, 3] AS arr_external;
```

```response
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

多个数组可以在 `ARRAY JOIN` 子句中用逗号分隔。在这种情况下，会同时对它们执行 `JOIN` (即直和，而不是笛卡尔积) 。请注意，默认情况下，所有数组的大小必须相同。示例：

```sql
SELECT s, arr, a, num, mapped
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num, arrayMap(x -> x + 1, arr) AS mapped;
```

```response
┌─s─────┬─arr─────┬─a─┬─num─┬─mapped─┐
│ Hello │ [1,2]   │ 1 │   1 │      2 │
│ Hello │ [1,2]   │ 2 │   2 │      3 │
│ World │ [3,4,5] │ 3 │   1 │      4 │
│ World │ [3,4,5] │ 4 │   2 │      5 │
│ World │ [3,4,5] │ 5 │   3 │      6 │
└───────┴─────────┴───┴─────┴────────┘
```

以下示例使用了 [arrayEnumerate](/zh/sql-reference/functions/array-functions#arrayEnumerate) 函数：

```sql
SELECT s, arr, a, num, arrayEnumerate(arr)
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num;
```

```response
┌─s─────┬─arr─────┬─a─┬─num─┬─arrayEnumerate(arr)─┐
│ Hello │ [1,2]   │ 1 │   1 │ [1,2]               │
│ Hello │ [1,2]   │ 2 │   2 │ [1,2]               │
│ World │ [3,4,5] │ 3 │   1 │ [1,2,3]             │
│ World │ [3,4,5] │ 4 │   2 │ [1,2,3]             │
│ World │ [3,4,5] │ 5 │   3 │ [1,2,3]             │
└───────┴─────────┴───┴─────┴─────────────────────┘
```

通过使用 `SETTINGS enable_unaligned_array_join = 1`，可以对多个大小不同的数组进行 join。Example：

```sql
SELECT s, arr, a, b
FROM arrays_test ARRAY JOIN arr AS a, [['a','b'],['c']] AS b
SETTINGS enable_unaligned_array_join = 1;
```

```response
┌─s───────┬─arr─────┬─a─┬─b─────────┐
│ Hello   │ [1,2]   │ 1 │ ['a','b'] │
│ Hello   │ [1,2]   │ 2 │ ['c']     │
│ World   │ [3,4,5] │ 3 │ ['a','b'] │
│ World   │ [3,4,5] │ 4 │ ['c']     │
│ World   │ [3,4,5] │ 5 │ []        │
│ Goodbye │ []      │ 0 │ ['a','b'] │
│ Goodbye │ []      │ 0 │ ['c']     │
└─────────┴─────────┴───┴───────────┘
```

<div id="array-join-with-nested-data-structure">
  ## ARRAY JOIN 与嵌套数据结构
</div>

`ARRAY JOIN` 也可用于[嵌套数据结构](../../../sql-reference/data-types/nested-data-structures/index.md)：

```sql
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

```response
┌─s───────┬─nest.x──┬─nest.y─────┐
│ Hello   │ [1,2]   │ [10,20]    │
│ World   │ [3,4,5] │ [30,40,50] │
│ Goodbye │ []      │ []         │
└─────────┴─────────┴────────────┘
```

```sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest;
```

```response
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

当在 `ARRAY JOIN` 中指定嵌套数据结构的名称时，其含义等同于对它所包含的所有数组元素执行 `ARRAY JOIN`。示例如下：

```sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`, `nest.y`;
```

```response
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

这种写法也说得通：

```sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`;
```

```response
┌─s─────┬─nest.x─┬─nest.y─────┐
│ Hello │      1 │ [10,20]    │
│ Hello │      2 │ [10,20]    │
│ World │      3 │ [30,40,50] │
│ World │      4 │ [30,40,50] │
│ World │      5 │ [30,40,50] │
└───────┴────────┴────────────┘
```

可以为嵌套数据结构指定别名，以便选择 `JOIN` 结果或源数组。示例：

```sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest AS n;
```

```response
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │
└───────┴─────┴─────┴─────────┴────────────┘
```

[arrayEnumerate](/zh/sql-reference/functions/array-functions#arrayEnumerate) 函数使用示例：

```sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`, num
FROM nested_test
ARRAY JOIN nest AS n, arrayEnumerate(`nest.x`) AS num;
```

```response
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┬─num─┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │   1 │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │   2 │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │   1 │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │   2 │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │   3 │
└───────┴─────┴─────┴─────────┴────────────┴─────┘
```

<div id="implementation-details">
  ## 实现细节
</div>

运行 `ARRAY JOIN` 时，查询的执行顺序会经过优化。虽然在查询中，`ARRAY JOIN` 必须始终写在 [WHERE](../../../sql-reference/statements/select/where.md)/[PREWHERE](../../../sql-reference/statements/select/prewhere.md) 子句之前，但从技术角度讲，除非 `ARRAY JOIN` 的结果会用于过滤，否则它们在技术上可以按任意顺序执行。具体的执行顺序由查询优化器控制。

<div id="incompatibility-with-short-circuit-function-evaluation">
  ### 与短路函数求值不兼容
</div>

[短路函数求值](/zh/operations/settings/settings#short_circuit_function_evaluation) 是一项用于优化 `if`、`multiIf`、`and` 和 `or` 等特定函数中复杂表达式执行过程的功能。它可以避免在执行这些函数时出现潜在异常，例如除以零。

`arrayJoin` 总是会执行，不支持短路函数求值。这是因为它是一种特殊函数，在查询分析和执行期间会与其他函数分开处理，并且需要额外的逻辑，而这套逻辑无法与短路函数执行机制兼容。原因在于，结果中的行数取决于 `arrayJoin` 的结果，而为 `arrayJoin` 实现惰性执行过于复杂且成本过高。

<div id="related-content">
  ## 相关内容
</div>

* 博客：[在 ClickHouse 中处理时间序列数据](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)