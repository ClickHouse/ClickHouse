---
description: 'Counts unique values in one or more `value` arrays according to the keys specified in the `key`
  array. Returns a tuple of arrays: keys in sorted order, followed by counts of unique values
  for the corresponding keys.'
sidebar_position: 217
slug: /sql-reference/aggregate-functions/reference/uniqmap
title: 'uniqMap'
doc_type: 'reference'
---

# uniqMap

Counts the number of unique (distinct) values in one or more `value` arrays according to the keys specified in the `key` array. Returns a tuple of arrays: keys in sorted order, followed by counts of unique values for the corresponding keys.

**Syntax**

```sql
uniqMap(key, value)
uniqMap(key, value1[, value2, ...])
uniqMap(Tuple(key, value))
uniqMap(Tuple(key, value1[, value2, ...]))
```

**Arguments**

- `key`: [Array](../../data-types/array.md) of keys.
- `value`, `value1`, `value2`, ...: [Array](../../data-types/array.md) of values to count unique occurrences for each key.

Passing a tuple of key and value arrays is a synonym to passing separately an array of keys and arrays of values.

:::note
The number of elements in `key` and all `value` arrays must be the same for each row that is aggregated.
:::

**Returned Value**

- Returns a tuple of arrays: the first array contains the keys in sorted order, followed by arrays containing counts of unique values for the corresponding keys. The count arrays are always of type `UInt64`.

**Example**

First we create a table called `uniq_map` and insert some data into it. Arrays of keys and values are stored separately as a column called `statusMap` of [Nested](../../data-types/nested-data-structures/index.md) type, and together as a column called `statusMapTuple` of [tuple](../../data-types/tuple.md) type to illustrate the use of the two different syntaxes of this function described above.

Query:

```sql
CREATE TABLE uniq_map(
    date Date,
    timeslot DateTime,
    statusMap Nested(
        status UInt16,
        requests UInt64
    ),
    statusMapTuple Tuple(Array(Int32), Array(Int32))
) ENGINE = Log;
```

```sql
INSERT INTO uniq_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10], ([1, 2, 3], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10], ([3, 4, 5], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10], ([4, 5, 6], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10], ([6, 7, 8], [10, 10, 10]));
```

Next, we query the table using the `uniqMap` function, making use of both array and tuple type syntaxes:

Query:

```sql
SELECT
    timeslot,
    uniqMap(statusMap.status, statusMap.requests),
    uniqMap(statusMapTuple)
FROM uniq_map
GROUP BY timeslot
```

Result:

```text
┌────────────timeslot─┬─uniqMap(statusMap.status, statusMap.requests)─┬─uniqMap(statusMapTuple)───┐
│ 2000-01-01 00:00:00 │ ([1,2,3,4,5],[1,1,1,1,1])                     │ ([1,2,3,4,5],[1,1,1,1,1]) │
│ 2000-01-01 00:01:00 │ ([4,5,6,7,8],[1,1,1,1,1])                     │ ([4,5,6,7,8],[1,1,1,1,1]) │
└─────────────────────┴───────────────────────────────────────────────┴───────────────────────────┘
```

**Example with Counting Unique Values**

This example demonstrates how `uniqMap` counts unique values when the same value appears multiple times:

```sql title="Query"
CREATE TABLE uniq_map_duplicates(
    keys Array(UInt64),
    values Array(UInt64)
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO uniq_map_duplicates VALUES
    ([1, 2, 3], [100, 200, 300]),
    ([1, 2, 3], [100, 200, 300]),
    ([1, 2, 3], [100, 250, 300]),
    ([4, 5], [400, 500]);

SELECT uniqMap(keys, values) FROM uniq_map_duplicates;
```

```text title="Response"
┌─uniqMap(keys, values)────┐
│ ([1,2,3,4,5],[1,2,1,1,1]) │
└──────────────────────────┘
```

In this example:
- Key 1: 1 unique value (100)
- Key 2: 2 unique values (200 and 250)
- Key 3: 1 unique value (300)
- Key 4: 1 unique value (400)
- Key 5: 1 unique value (500)

**Example with Multiple Value Arrays**

`uniqMap` also supports counting unique values across multiple value arrays simultaneously.
This is useful when you have related metrics that share the same keys.

```sql title="Query"
CREATE TABLE multi_metrics(
    date Date,
    browser_metrics Nested(
        browser String,
        impressions UInt32,
        clicks UInt32
    )
)
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO multi_metrics VALUES
    ('2000-01-01', ['Firefox', 'Chrome'], [100, 200], [10, 25]),
    ('2000-01-01', ['Chrome', 'Safari'], [200, 50], [25, 5]),
    ('2000-01-01', ['Firefox', 'Edge'], [100, 40], [15, 4]);

SELECT
    uniqMap(browser_metrics.browser, browser_metrics.impressions, browser_metrics.clicks) AS result
FROM multi_metrics;
```

```text title="Response"
┌─result──────────────────────────────────────────────────┐
│ (['Chrome','Edge','Firefox','Safari'],[1,1,1,1],[1,1,2,1]) │
└─────────────────────────────────────────────────────────┘
```

In this example:
- The result tuple contains three arrays
- First array: keys (browser names) in sorted order
- Second array: count of unique impressions for each browser
- Third array: count of unique clicks for each browser

**See Also**

- [Map combinator for Map datatype](../combinators.md#-map)
- [sumMap](../reference/summap.md)
