---
toc_priority: 141
---

# deltaSum {#agg_functions-deltasum}

Sums the arithmetic difference between consecutive rows. If the difference is negative, it is ignored.

!!! info "Note"
    The underlying data must be sorted for this function to work properly. If you would like to use this function in a [materialized view](../../../sql-reference/statements/create/view.md#materialized), you most likely want to use the [deltaSumTimestamp](../../../sql-reference/aggregate-functions/reference/deltasumtimestamp.md#agg_functions-deltasumtimestamp) method instead.

**Syntax**

``` sql
deltaSum(value)
```

**Arguments**

-   `value` — Input values, must be [Integer](../../data-types/int-uint.md) or [Float](../../data-types/float.md) type.

**Returned value**

- A gained arithmetic difference of the `Integer` or `Float` type.

**Examples**

Query:

``` sql
SELECT deltaSum(arrayJoin([1, 2, 3]));
```

Result:

``` text
┌─deltaSum(arrayJoin([1, 2, 3]))─┐
│                              2 │
└────────────────────────────────┘
```

Query:

``` sql
SELECT deltaSum(arrayJoin([1, 2, 3, 0, 3, 4, 2, 3]));
```

Result:

``` text
┌─deltaSum(arrayJoin([1, 2, 3, 0, 3, 4, 2, 3]))─┐
│                                             7 │
└───────────────────────────────────────────────┘
```

Query:

``` sql
SELECT deltaSum(arrayJoin([2.25, 3, 4.5]));
```

Result:

``` text
┌─deltaSum(arrayJoin([2.25, 3, 4.5]))─┐
│                                2.25 │
└─────────────────────────────────────┘
```

## See Also {#see-also}

-   [runningDifference](../../functions/other-functions.md#other_functions-runningdifference)
