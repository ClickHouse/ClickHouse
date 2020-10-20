---
toc_priority: 200
---

# countEqualRanges {#agg_function-countequalranges}

Calculates the number of segments with equal values in a column.

``` sql
countEqualRanges(x)
```

!!! note "Note"
    The result is depend on the order ClickHouse read data. In some cases, you can rely on the order (e.g. selecting from a subquery with `ORDER BY`).

This function can be used as a faster [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact) on sorted data.

**Parameters**

The function takes one parameter `x` of any type.

**Returned value**

-   A [UInt64](../../../sql-reference/data-types/int-uint.md)-type number.

**Examples**

Example 1:

``` sql
SELECT countEqualRanges(x) FROM (
    SELECT arrayJoin(['a', 'a', 'a', 'b', 'c', 'c', 'a', 'd', 'd']) AS x
)
```

``` text
┌─countEqualRanges(x)─┐
│                   5 │
└─────────────────────┘
```
