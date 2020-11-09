---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
toc_title: ARRAY JOIN
---

# ARRAY JOIN子句 {#select-array-join-clause}

对于包含数组列的表来说，这是一种常见的操作，用于生成一个新表，该表具有包含该初始列的每个单独数组元素的列，而其他列的值将被重复。 这是什么基本情况 `ARRAY JOIN` 子句有

它的名字来自这样一个事实，即它可以被视为执行 `JOIN` 具有数组或嵌套数据结构。 意图类似于 [arrayJoin](../../../sql-reference/functions/array-join.md#functions_arrayjoin) 功能，但该子句功能更广泛。

语法:

``` sql
SELECT <expr_list>
FROM <left_subquery>
[LEFT] ARRAY JOIN <array>
[WHERE|PREWHERE <expr>]
...
```

您只能指定一个 `ARRAY JOIN` a中的条款 `SELECT` 查询。

支持的类型 `ARRAY JOIN` 下面列出:

-   `ARRAY JOIN` -在基本情况下，空数组不包括在结果中 `JOIN`.
-   `LEFT ARRAY JOIN` -的结果 `JOIN` 包含具有空数组的行。 空数组的值设置为数组元素类型的默认值（通常为0、空字符串或NULL）。

## 基本数组连接示例 {#basic-array-join-examples}

下面的例子演示的用法 `ARRAY JOIN` 和 `LEFT ARRAY JOIN` 条款 让我们创建一个表 [阵列](../../../sql-reference/data-types/array.md) 键入column并在其中插入值:

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

## 使用别名 {#using-aliases}

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

下面的例子使用 [arrayEnumerate](../../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate) 功能:

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

## 具有嵌套数据结构的数组连接 {#array-join-with-nested-data-structure}

`ARRAY JOIN` 也适用于 [嵌套数据结构](../../../sql-reference/data-types/nested-data-structures/nested.md):

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

使用的例子 [arrayEnumerate](../../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate) 功能:

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

## 实施细节 {#implementation-details}

运行时优化查询执行顺序 `ARRAY JOIN`. 虽然 `ARRAY JOIN` 必须始终之前指定 [WHERE](../../../sql-reference/statements/select/where.md)/[PREWHERE](../../../sql-reference/statements/select/prewhere.md) 子句中的查询，从技术上讲，它们可以以任何顺序执行，除非结果 `ARRAY JOIN` 用于过滤。 处理顺序由查询优化器控制。
