---
description: 'arrayJoin 函数文档'
sidebar_label: 'arrayJoin'
slug: /sql-reference/functions/array-join
title: 'arrayJoin 函数'
doc_type: 'reference'
---

这是一个非常特殊的函数。

普通函数不会改变一组行，只会改变每一行中的值 (map) 。
聚合函数会将一组行压缩 (fold 或 reduce) 。
`arrayJoin` 函数则会将每一行展开为一组行 (unfold) 。

该函数接受一个数组作为参数，并根据数组中的元素个数，将源行扩展为多行。
所有列中的值都会被直接复制，只有应用了该函数的那一列中的值除外；该值会被替换为对应的数组元素值。

:::note
如果数组为空，`arrayJoin` 不会生成任何行。
若要返回一行，其中包含该数组类型的默认值，可以用 [emptyArrayToSingle](./array-functions.md#emptyArrayToSingle) 将其包装起来，例如：`arrayJoin(emptyArrayToSingle(...))`。
:::

例如：

```sql title="Query"
SELECT arrayJoin([1, 2, 3] AS src) AS dst, 'Hello', src
```

```text title="Response"
┌─dst─┬─\'Hello\'─┬─src─────┐
│   1 │ Hello     │ [1,2,3] │
│   2 │ Hello     │ [1,2,3] │
│   3 │ Hello     │ [1,2,3] │
└─────┴───────────┴─────────┘
```

`arrayJoin` 函数会影响查询的所有部分，包括 `WHERE` 子句。请注意，下面这个查询的结果是 `2`，尽管子查询只返回了 1 行。

```sql title="Query"
SELECT sum(1) AS impressions
FROM
(
    SELECT ['Istanbul', 'Berlin', 'Babruysk'] AS cities
)
WHERE arrayJoin(cities) IN ['Istanbul', 'Berlin'];
```

```text title="Response"
┌─impressions─┐
│           2 │
└─────────────┘
```

一个查询可以使用多个 `arrayJoin` 函数。在这种情况下，转换会执行多次，行会成倍增加。
例如：

```sql title="Query"
SELECT
    sum(1) AS impressions,
    arrayJoin(cities) AS city,
    arrayJoin(browsers) AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
```

```text title="Response"
┌─impressions─┬─city─────┬─browser─┐
│           2 │ Istanbul │ Chrome  │
│           1 │ Istanbul │ Firefox │
│           2 │ Berlin   │ Chrome  │
│           1 │ Berlin   │ Firefox │
│           2 │ Babruysk │ Chrome  │
│           1 │ Babruysk │ Firefox │
└─────────────┴──────────┴─────────┘
```

<div id="important-note">
  ### 最佳实践
</div>

由于公共子表达式会被消除，对同一表达式多次使用 `arrayJoin` 可能得不到预期结果。
在这种情况下，可考虑给重复的数组表达式添加一些不会影响 JOIN 结果的额外操作。例如，`arrayJoin(arraySort(arr))`、`arrayJoin(arrayConcat(arr, []))`

示例：

```sql title="Query"
SELECT
    arrayJoin(dice) AS first_throw,
    /* arrayJoin(dice) as second_throw */ -- is technically correct, but will annihilate result set
    arrayJoin(arrayConcat(dice, [])) AS second_throw -- intentionally changed expression to force re-evaluation
FROM (
    SELECT [1, 2, 3, 4, 5, 6] AS dice
);
```

请注意 SELECT 查询中的 [`ARRAY JOIN`](../statements/select/array-join.md) 语法，它提供了更丰富的用法。
`ARRAY JOIN` 允许你一次同时转换多个元素个数相同的数组。

示例：

```sql title="Query"
SELECT
    sum(1) AS impressions,
    city,
    browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
ARRAY JOIN
    cities AS city,
    browsers AS browser
GROUP BY
    2,
    3
```

```text title="Response"
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Babruysk │ Chrome  │
└─────────────┴──────────┴─────────┘
```

或者也可以使用 [`Tuple`](../data-types/tuple.md)

示例：

```sql title="Query"
SELECT
    sum(1) AS impressions,
    (arrayJoin(arrayZip(cities, browsers)) AS t).1 AS city,
    t.2 AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
```

```text title="Row"
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Babruysk │ Chrome  │
└─────────────┴──────────┴─────────┘
```

在 ClickHouse 中，`arrayJoin` 这个名称源于它在概念上与 JOIN 操作相似，但作用于单行中的数组。传统 JOIN 会合并来自不同表的行，而 `arrayJoin` 则会把一行中数组里的每个元素分别“连接”出来，生成多行——每个数组元素对应一行——同时复制其他列的值。ClickHouse 还提供了 [`ARRAY JOIN`](/zh/sql-reference/statements/select/array-join) 子句语法，通过使用熟悉的 SQL JOIN 术语，使它与传统 JOIN 操作之间的关系更加直观。这个过程也称为数组的“展开” (“unfolding”) ，但函数名和子句中之所以都使用 “join” 一词，是因为它类似于将表与数组元素进行连接，从而以类似 JOIN 操作的方式有效扩展数据集。