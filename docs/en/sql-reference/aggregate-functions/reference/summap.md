---
description: 'Totals one or more `value` arrays according to the keys specified in the `key`
  array. Returns a tuple of arrays: keys in sorted order, followed by values summed for
  the corresponding keys without overflow.'
sidebar_position: 198
slug: /sql-reference/aggregate-functions/reference/summap
title: 'sumMap'
doc_type: 'reference'
---

# sumMap

Totals one or more `value` arrays according to the keys specified in the `key` array. Returns a tuple of arrays: keys in sorted order, followed by values summed for the corresponding keys without overflow.

**Syntax**

- `sumMap(key <Array>, value1 <Array>[, value2 <Array>, ...])` [Array type](../../data-types/array.md).
- `sumMap(Tuple(key <Array>[, value1 <Array>, value2 <Array>, ...]))` [Tuple type](../../data-types/tuple.md).

Alias: `sumMappedArrays`.

**Arguments** 

- `key`: [Array](../../data-types/array.md) of keys.
- `value1`, `value2`, ...: [Array](../../data-types/array.md) of values to sum for each key.

Passing a tuple of key and value arrays is a synonym to passing separately an array of keys and arrays of values.

:::note 
The number of elements in `key` and all `value` arrays must be the same for each row that is totaled.
:::

**Returned Value** 

- Returns a tuple of arrays: the first array contains keys in sorted order, followed by arrays containing values summed for the corresponding keys.

**Example**

First we create a table called `sum_map`, and insert some data into it. Arrays of keys and values are stored separately as a column called `statusMap` of [Nested](../../data-types/nested-data-structures/index.md) type, and together as a column called `statusMapTuple` of [tuple](../../data-types/tuple.md) type to illustrate the use of the two different syntaxes of this function described above.

Query:

```sql
CREATE TABLE sum_map(
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
INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10], ([1, 2, 3], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10], ([3, 4, 5], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10], ([4, 5, 6], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10], ([6, 7, 8], [10, 10, 10]));
```

Next, we query the table using the `sumMap` function, making use of both array and tuple type syntaxes:

Query:

```sql
SELECT
    timeslot,
    sumMap(statusMap.status, statusMap.requests),
    sumMap(statusMapTuple)
FROM sum_map
GROUP BY timeslot
```

Result:

```text
┌────────────timeslot─┬─sumMap(statusMap.status, statusMap.requests)─┬─sumMap(statusMapTuple)─────────┐
│ 2000-01-01 00:00:00 │ ([1,2,3,4,5],[10,10,20,10,10])               │ ([1,2,3,4,5],[10,10,20,10,10]) │
│ 2000-01-01 00:01:00 │ ([4,5,6,7,8],[10,10,20,10,10])               │ ([4,5,6,7,8],[10,10,20,10,10]) │
└─────────────────────┴──────────────────────────────────────────────┴────────────────────────────────┘
```

**Example with Multiple Value Arrays**

`sumMap` also supports aggregating multiple value arrays simultaneously.
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
    ('2000-01-01', ['Chrome', 'Safari'], [150, 50], [20, 5]),
    ('2000-01-01', ['Firefox', 'Edge'], [80, 40], [8, 4]);

SELECT 
    sumMap(browser_metrics.browser, browser_metrics.impressions, browser_metrics.clicks) AS result
FROM multi_metrics;
```

```text title="Response"
┌─result────────────────────────────────────────────────────────────────────────┐
│ (['Chrome', 'Edge', 'Firefox', 'Safari'], [350, 40, 180, 50], [45, 4, 18, 5]) │
└───────────────────────────────────────────────────────────────────────────────┘
```

In this example:
- The result tuple contains three arrays
- First array: keys (browser names) in sorted order
- Second array: total impressions for each browser
- Third array: total clicks for each browser

**See Also**

- [Map combinator for Map datatype](../combinators.md#-map)
- [sumMapWithOverflow](../reference/summapwithoverflow.md)
