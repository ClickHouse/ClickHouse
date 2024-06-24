---
slug: /en/sql-reference/aggregate-functions/reference/maxmap
sidebar_position: 165
---

# maxMap

Syntax: `maxMap(key, value)` or `maxMap(Tuple(key, value))`

Calculates the maximum from `value` array according to the keys specified in the `key` array.

Passing a tuple of keys and value arrays is identical to passing two arrays of keys and values.

The number of elements in `key` and `value` must be the same for each row that is totaled.

Returns a tuple of two arrays: keys and values calculated for the corresponding keys.

Example:

``` sql
SELECT maxMap(a, b)
FROM values('a Array(Char), b Array(Int64)', (['x', 'y'], [2, 2]), (['y', 'z'], [3, 1]))
```

``` text
┌─maxMap(a, b)───────────┐
│ [['x','y','z'],[2,3,1]]│
└────────────────────────┘
```
