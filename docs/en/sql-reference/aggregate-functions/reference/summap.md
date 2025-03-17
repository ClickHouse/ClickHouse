---
slug: /en/sql-reference/aggregate-functions/reference/summap
sidebar_position: 198
---

# sumMap

Totals a `value` array according to the keys specified in the `key` array. Returns a tuple of two arrays: keys in sorted order, and values ​​summed for the corresponding keys without overflow.

**Syntax**

- `sumMap(key <Array>, value <Array>)` [Array type](../../data-types/array.md).
- `sumMap(Tuple(key <Array>, value <Array>))` [Tuple type](../../data-types/tuple.md).

Alias: `sumMappedArrays`.

**Arguments** 

- `key`: [Array](../../data-types/array.md) of keys.
- `value`: [Array](../../data-types/array.md) of values.

Passing a tuple of key and value arrays is a synonym to passing separately an array of keys and an array of values.

:::note 
The number of elements in `key` and `value` must be the same for each row that is totaled.
:::

**Returned Value** 

- Returns a tuple of two arrays: keys in sorted order, and values ​​summed for the corresponding keys.

**Example**

First we create a table called `sum_map`, and insert some data into it. Arrays of keys and values are stored separately as a column called `statusMap` of [Nested](../../data-types/nested-data-structures/index.md) type, and together as a column called `statusMapTuple` of [tuple](../../data-types/tuple.md) type to illustrate the use of the two different syntaxes of this function described above.

Query:

``` sql
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

``` sql
SELECT
    timeslot,
    sumMap(statusMap.status, statusMap.requests),
    sumMap(statusMapTuple)
FROM sum_map
GROUP BY timeslot
```

Result:

``` text
┌────────────timeslot─┬─sumMap(statusMap.status, statusMap.requests)─┬─sumMap(statusMapTuple)─────────┐
│ 2000-01-01 00:00:00 │ ([1,2,3,4,5],[10,10,20,10,10])               │ ([1,2,3,4,5],[10,10,20,10,10]) │
│ 2000-01-01 00:01:00 │ ([4,5,6,7,8],[10,10,20,10,10])               │ ([4,5,6,7,8],[10,10,20,10,10]) │
└─────────────────────┴──────────────────────────────────────────────┴────────────────────────────────┘
```

**See Also**

- [Map combinator for Map datatype](../combinators.md#-map)
- [sumMapWithOverflow](../reference/summapwithoverflow.md)
