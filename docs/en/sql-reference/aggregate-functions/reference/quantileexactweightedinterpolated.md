---
slug: /en/sql-reference/aggregate-functions/reference/quantileExactWeightedInterpolated
sidebar_position: 176
---

# quantileExactWeightedInterpolated

Computes [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence using linear interpolation, taking into account the weight of each element.

To get the interpolated value, all the passed values are combined into an array, which are then sorted by their corresponding weights. Quantile interpolation is then performed using the [weighted percentile method](https://en.wikipedia.org/wiki/Percentile#The_weighted_percentile_method) by building a cumulative distribution based on weights and then a linear interpolation is performed using the weights and the values to compute the quantiles.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles) function.

We strongly recommend using `quantileExactWeightedInterpolated` instead of `quantileInterpolatedWeighted` because `quantileExactWeightedInterpolated` is more accurate than `quantileInterpolatedWeighted`. Here is an example:

``` sql
SELECT
    quantileExactWeightedInterpolated(0.99)(number, 1),
    quantile(0.99)(number),
    quantileInterpolatedWeighted(0.99)(number, 1)
FROM numbers(9)


┌─quantileExactWeightedInterpolated(0.99)(number, 1)─┬─quantile(0.99)(number)─┬─quantileInterpolatedWeighted(0.99)(number, 1)─┐
│                                               7.92 │                   7.92 │                                             8 │
└────────────────────────────────────────────────────┴────────────────────────┴───────────────────────────────────────────────┘
```

**Syntax**

``` sql
quantileExactWeightedInterpolated(level)(expr, weight)
```

Alias: `medianExactWeightedInterpolated`.

**Arguments**

- `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median).
- `expr` — Expression over the column values resulting in numeric [data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) or [DateTime](../../../sql-reference/data-types/datetime.md).
- `weight` — Column with weights of sequence members. Weight is a number of value occurrences with [Unsigned integer types](../../../sql-reference/data-types/int-uint.md).

**Returned value**

- Quantile of the specified level.

Type:

- [Float64](../../../sql-reference/data-types/float.md) for numeric data type input.
- [Date](../../../sql-reference/data-types/date.md) if input values have the `Date` type.
- [DateTime](../../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

Input table:

``` text
┌─n─┬─val─┐
│ 0 │   3 │
│ 1 │   2 │
│ 2 │   1 │
│ 5 │   4 │
└───┴─────┘
```

Result:

``` text
┌─quantileExactWeightedInterpolated(n, val)─┐
│                                       1.5 │
└───────────────────────────────────────────┘
```

**See Also**

- [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
- [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
