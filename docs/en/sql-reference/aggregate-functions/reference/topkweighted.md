---
slug: /en/sql-reference/aggregate-functions/reference/topkweighted
sidebar_position: 109
---

# topKWeighted

Returns an array of the approximately most frequent values in the specified column. The resulting array is sorted in descending order of approximate frequency of values (not by the values themselves). Additionally, the weight of the value is taken into account.

**Syntax**

``` sql
topKWeighted(N)(x, weight)
```

**Arguments**

- `N` — The number of elements to return.
- `x` — The value.
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

**See Also**

- [topK](../../../sql-reference/aggregate-functions/reference/topk.md)
