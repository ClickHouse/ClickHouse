---
slug: /en/sql-reference/aggregate-functions/reference/topkweighted
sidebar_position: 203
---

# topKWeighted

Returns an array of the approximately most frequent values in the specified column. The resulting array is sorted in descending order of approximate frequency of values (not by the values themselves). Additionally, the weight of the value is taken into account.

**Syntax**

``` sql
topKWeighted(N)(column, weight)
topKWeighted(N, load_factor)(column, weight)
topKWeighted(N, load_factor, 'counts')(column, weight)
```

**Parameters**

- `N` — The number of elements to return. Optional. Default value: 10.
- `load_factor` — Defines, how many cells reserved for values. If uniq(column) > N * load_factor, result of topK function will be approximate. Optional. Default value: 3.
- `counts` — Defines, should result contain approximate count and error value.

**Arguments**

- `column` — The value.
- `weight` — The weight. Every value is accounted `weight` times for frequency calculation. [UInt64](../../../sql-reference/data-types/int-uint.md).

**Returned value**

Returns an array of the values with maximum approximate sum of weights.

**Example**

Query:

``` sql
SELECT topKWeighted(2)(k, w) FROM
VALUES('k Char, w UInt64', ('y', 1), ('y', 1), ('x', 5), ('y', 1), ('z', 10))
```

Result:

``` text
┌─topKWeighted(2)(k, w)──┐
│ ['z','x']              │
└────────────────────────┘
```

Query:

``` sql
SELECT topKWeighted(2, 10, 'counts')(k, w)
FROM VALUES('k Char, w UInt64', ('y', 1), ('y', 1), ('x', 5), ('y', 1), ('z', 10))
```

Result:

``` text
┌─topKWeighted(2, 10, 'counts')(k, w)─┐
│ [('z',10,0),('x',5,0)]              │
└─────────────────────────────────────┘
```

**See Also**

- [topK](../../../sql-reference/aggregate-functions/reference/topk.md)
- [approx_top_k](../../../sql-reference/aggregate-functions/reference/approxtopk.md)
- [approx_top_sum](../../../sql-reference/aggregate-functions/reference/approxtopsum.md)