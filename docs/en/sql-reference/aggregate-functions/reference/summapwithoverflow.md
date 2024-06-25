---
slug: /en/sql-reference/aggregate-functions/reference/summapwithoverflow
sidebar_position: 199
---

# sumMapWithOverflow

Totals a `value` array according to the keys specified in the `key` array. Returns a tuple of two arrays: keys in sorted order, and values ​​summed for the corresponding keys.
It differs from the [sumMap](../reference/summap.md) function in that it does summation with overflow - i.e. returns the same data type for the summation as the argument data type.

**Syntax**

- `sumMapWithOverflow(key <Array>, value <Array>)` [Array type](../../data-types/array.md).
- `sumMapWithOverflow(Tuple(key <Array>, value <Array>))` [Tuple type](../../data-types/tuple.md).

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
        status UInt8,
        requests UInt8
    ),
    statusMapTuple Tuple(Array(Int8), Array(Int8))
) ENGINE = Log;
```
```sql
INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10], ([1, 2, 3], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10], ([3, 4, 5], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10], ([4, 5, 6], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10], ([6, 7, 8], [10, 10, 10]));
```

If we query the table using the `sumMap`, `sumMapWithOverflow` with the array type syntax, and `toTypeName` functions then we can see that
for the `sumMapWithOverflow` function, the data type of the summed values array is the same as the argument type, both `UInt8` (i.e. summation was done with overflow). For `sumMap` the data type of the summed values arrays has changed from `UInt8` to `UInt64` such that overflow does not occur. 

Query:

``` sql
SELECT
    timeslot,
    toTypeName(sumMap(statusMap.status, statusMap.requests)),
    toTypeName(sumMapWithOverflow(statusMap.status, statusMap.requests)),
FROM sum_map
GROUP BY timeslot
```

Equivalently we could have used the tuple syntax with for the same result.

``` sql
SELECT
    timeslot,
    toTypeName(sumMap(statusMapTuple)),
    toTypeName(sumMapWithOverflow(statusMapTuple)),
FROM sum_map
GROUP BY timeslot
```

Result:

``` text
   ┌────────────timeslot─┬─toTypeName(sumMap(statusMap.status, statusMap.requests))─┬─toTypeName(sumMapWithOverflow(statusMap.status, statusMap.requests))─┐
1. │ 2000-01-01 00:01:00 │ Tuple(Array(UInt8), Array(UInt64))                       │ Tuple(Array(UInt8), Array(UInt8))                                    │
2. │ 2000-01-01 00:00:00 │ Tuple(Array(UInt8), Array(UInt64))                       │ Tuple(Array(UInt8), Array(UInt8))                                    │
   └─────────────────────┴──────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────────────────┘
```

**See Also**
    
- [sumMap](../reference/summap.md)
