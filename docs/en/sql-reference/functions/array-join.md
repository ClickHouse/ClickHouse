---
sidebar_position: 61
sidebar_label: arrayJoin
---

# arrayJoin function

This is a very unusual function.

Normal functions do not change a set of rows, but just change the values in each row (map).
Aggregate functions compress a set of rows (fold or reduce).
The `arrayJoin` function takes each row and generates a set of rows (unfold).

This function takes an array as an argument, and propagates the source row to multiple rows for the number of elements in the array.
All the values in columns are simply copied, except the values in the column where this function is applied; it is replaced with the corresponding array value.

Example:

``` sql
SELECT arrayJoin([1, 2, 3] AS src) AS dst, 'Hello', src
```

``` text
┌─dst─┬─\'Hello\'─┬─src─────┐
│   1 │ Hello     │ [1,2,3] │
│   2 │ Hello     │ [1,2,3] │
│   3 │ Hello     │ [1,2,3] │
└─────┴───────────┴─────────┘
```

The `arrayJoin` function affects all sections of the query, including the `WHERE` section. Notice the result 2, even though the subquery returned 1 row.

Example:

```sql
SELECT sum(1) AS impressions
FROM
(
    SELECT ['Istambul', 'Berlin', 'Bobruisk'] AS cities
)
WHERE arrayJoin(cities) IN ['Istambul', 'Berlin'];
```

``` text
┌─impressions─┐
│           2 │
└─────────────┘
```

A query can use multiple `arrayJoin` functions. In this case, the transformation is performed multiple times and the rows are multiplied.

Example:

```sql
SELECT
    sum(1) AS impressions,
    arrayJoin(cities) AS city,
    arrayJoin(browsers) AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Bobruisk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
```

``` text
┌─impressions─┬─city─────┬─browser─┐
│           2 │ Istanbul │ Chrome  │
│           1 │ Istanbul │ Firefox │
│           2 │ Berlin   │ Chrome  │
│           1 │ Berlin   │ Firefox │
│           2 │ Bobruisk │ Chrome  │
│           1 │ Bobruisk │ Firefox │
└─────────────┴──────────┴─────────┘
```

Note the [ARRAY JOIN](../statements/select/array-join.md) syntax in the SELECT query, which provides broader possibilities.
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
        ['Istanbul', 'Berlin', 'Bobruisk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
ARRAY JOIN
    cities AS city,
    browsers AS browser
GROUP BY
    2,
    3
```

``` text
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Bobruisk │ Chrome  │
└─────────────┴──────────┴─────────┘
```

Or you can use [Tuple](../data-types/tuple.md)

Example:

```sql
SELECT
    sum(1) AS impressions,
    (arrayJoin(arrayZip(cities, browsers)) AS t).1 AS city,
    t.2 AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Bobruisk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
```

``` text
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Bobruisk │ Chrome  │
└─────────────┴──────────┴─────────┘
```
