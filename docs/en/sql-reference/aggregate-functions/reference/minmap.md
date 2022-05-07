---
sidebar_position: 142
---

# minMap {#agg_functions-minmap}

Syntax: `minMap(key, value)` or `minMap(Tuple(key, value))`

Calculates the minimum from `value` array according to the keys specified in the `key` array.

Passing a tuple of keys and value ​​arrays is identical to passing two arrays of keys and values.

The number of elements in `key` and `value` must be the same for each row that is totaled.

Returns a tuple of two arrays: keys in sorted order, and values calculated for the corresponding keys.

Example:

``` sql
SELECT minMap(a, b)
FROM values('a Array(Int32), b Array(Int64)', ([1, 2], [2, 2]), ([2, 3], [1, 1]))
```

``` text
┌─minMap(a, b)──────┐
│ ([1,2,3],[2,1,1]) │
└───────────────────┘
```
