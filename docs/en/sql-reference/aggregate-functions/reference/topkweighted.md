---
toc_priority: 109
---

# topKWeighted {#topkweighted}

Returns an array of the approximately most frequent values in the specified column. The resulting array is sorted in descending order of approximate frequency of values (not by the values themselves). Additionally, the weight of the value is taken into account.

**Syntax**

``` sql
topKWeighted(N)(x, weight)
```

**Arguments**

-   `N` — The number of elements to return.
-   `x` — The value.
-   `weight` — The weight. Every value is accounted `weight` times for frequency calculation. [UInt64](../../../sql-reference/data-types/int-uint.md).

**Returned value**

Returns an array of the values with maximum approximate sum of weights.

**Example**

Query:

``` sql
SELECT topKWeighted(10)(number, number) FROM numbers(1000)
```

Result:

``` text
┌─topKWeighted(10)(number, number)──────────┐
│ [999,998,997,996,995,994,993,992,991,990] │
└───────────────────────────────────────────┘
```

**See Also**

-   [topK](../../../sql-reference/aggregate-functions/reference/topk.md)
