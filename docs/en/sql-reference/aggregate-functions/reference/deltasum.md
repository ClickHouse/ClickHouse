---
slug: /sql-reference/aggregate-functions/reference/deltasum
sidebar_position: 129
title: "deltaSum"
description: "Sums the arithmetic difference between consecutive rows."
---

# deltaSum

Sums the arithmetic difference between consecutive rows. If the difference is negative, it is ignored.

:::note
The underlying data must be sorted for this function to work properly. If you would like to use this function in a [materialized view](/sql-reference/statements/create/view#materialized-view), you most likely want to use the [deltaSumTimestamp](../../../sql-reference/aggregate-functions/reference/deltasumtimestamp.md#agg_functions-deltasumtimestamp) method instead.
:::

**Syntax**

``` sql
deltaSum(value)
```

**Arguments**

- `value` — Input values, must be [Integer](../../data-types/int-uint.md) or [Float](../../data-types/float.md) type.

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

- [runningDifference](../../functions/other-functions.md#other_functions-runningdifference)

## Combinators

The following combinators can be applied to the `deltaSum` function:

### deltaSumIf
Sums the positive differences between consecutive rows only for rows that match the given condition.

### deltaSumArray
Sums the positive differences between consecutive elements in the array.

### deltaSumMap
Sums the positive differences between consecutive values for each key in the map separately.

### deltaSumSimpleState
Returns the delta sum value with SimpleAggregateFunction type.

### deltaSumState
Returns the intermediate state of delta sum calculation.

### deltaSumMerge
Combines intermediate delta sum states to get the final result.

### deltaSumMergeState
Combines intermediate delta sum states but returns an intermediate state.

### deltaSumForEach
Sums the positive differences between consecutive elements in multiple arrays.

### deltaSumDistinct
Sums the positive differences between consecutive distinct values only.

### deltaSumOrDefault
Returns 0 if there are no rows to calculate delta sum.

### deltaSumOrNull
Returns NULL if there are no rows to calculate delta sum.
