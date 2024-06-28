---
slug: /en/sql-reference/aggregate-functions/reference/approxtopk
sidebar_position: 107
---

# approx_top_k

Returns an array of the approximately most frequent values and their counts in the specified column. The resulting array is sorted in descending order of approximate frequency of values (not by the values themselves).


``` sql
approx_top_k(N)(column)
approx_top_k(N, reserved)(column)
```

This function does not provide a guaranteed result. In certain situations, errors might occur and it might return frequent values that aren’t the most frequent values.

We recommend using the `N < 10` value; performance is reduced with large `N` values. Maximum value of `N = 65536`.

**Parameters**

- `N` — The number of elements to return. Optional. Default value: 10.
- `reserved` — Defines, how many cells reserved for values. If uniq(column) > reserved, result of topK function will be approximate. Optional. Default value: N * 3.
 
**Arguments**

- `column` — The value to calculate frequency.

**Example**

Query:

``` sql
SELECT approx_top_k(2)(k)
FROM VALUES('k Char, w UInt64', ('y', 1), ('y', 1), ('x', 5), ('y', 1), ('z', 10));
```

Result:

``` text
┌─approx_top_k(2)(k)────┐
│ [('y',3,0),('x',1,0)] │
└───────────────────────┘
```

# approx_top_count

Is an alias to `approx_top_k` function

**See Also**

- [topK](../../../sql-reference/aggregate-functions/reference/topk.md)
- [topKWeighted](../../../sql-reference/aggregate-functions/reference/topkweighted.md)
- [approx_top_sum](../../../sql-reference/aggregate-functions/reference/approxtopsum.md)

