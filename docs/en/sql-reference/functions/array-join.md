---
description: 'Documentation for arrayJoin function'
sidebar_label: 'arrayJoin'
sidebar_position: 15
slug: /sql-reference/functions/array-join
title: 'arrayJoin function'
---

# arrayJoin function

This is a very unusual function.

Normal functions do not change a set of rows, but just change the values in each row (map).
Aggregate functions compress a set of rows (fold or reduce).
The `arrayJoin` function takes each row and generates a set of rows (unfold).

This function takes an array as an argument, and propagates the source row to multiple rows for the number of elements in the array.
All the values in columns are simply copied, except the values in the column where this function is applied; it is replaced with the corresponding array value.

For example:

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

The `arrayJoin` function affects all sections of the query, including the `WHERE` section. Notice in that the result of the query below is `2`, even though the subquery returned 1 row.

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

A query can use multiple `arrayJoin` functions. In this case, the transformation is performed multiple times and the rows are multiplied.
For example:

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

### Best practice {#important-note}

Using multiple `arrayJoin` with same expression may not produce expected results due to the elimination of common subexpressions.
In those cases, consider modifying repeated array expressions with extra operations that do not affect the join result. For example,  `arrayJoin(arraySort(arr))`, `arrayJoin(arrayConcat(arr, []))`

Example:

```sql
SELECT
    arrayJoin(dice) AS first_throw,
    /* arrayJoin(dice) as second_throw */ -- is technically correct, but will annihilate result set
    arrayJoin(arrayConcat(dice, [])) AS second_throw -- intentionally changed expression to force re-evaluation
FROM (
    SELECT [1, 2, 3, 4, 5, 6] AS dice
);
```

Note the [`ARRAY JOIN`](../statements/select/array-join.md) syntax in the SELECT query, which provides broader possibilities.
`ARRAY JOIN` allows you to convert multiple arrays with the same number of elements at a time.

Example:

```sql
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

```text
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Babruysk │ Chrome  │
└─────────────┴──────────┴─────────┘
```

Or you can use [`Tuple`](../data-types/tuple.md)

Example:

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

The name `arrayJoin` in ClickHouse comes from its conceptual similarity to the JOIN operation, but applied to arrays within a single row. While traditional JOINs combine rows from different tables, `arrayJoin` "joins" each element of an array in a row, producing multiple rows - one for each array element - while duplicating the other column values. ClickHouse also provides the [`ARRAY JOIN`](/sql-reference/statements/select/array-join) clause syntax, which makes this relationship to traditional JOIN operations even more explicit by using familiar SQL JOIN terminology. This process is also referred to as "unfolding" the array, but the term "join" is used in both the function name and clause because it resembles joining the table with the array elements, effectively expanding the dataset in a way similar to a JOIN operation.
