---
slug: /en/sql-reference/aggregate-functions/reference/quantileexactweighted
sidebar_position: 174
---

# quantileExactWeighted

Exactly computes the [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence, taking into account the weight of each element.

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Each value is counted with its weight, as if it is present `weight` times. A hash table is used in the algorithm. Because of this, if the passed values ​​are frequently repeated, the function consumes less RAM than [quantileExact](../../../sql-reference/aggregate-functions/reference/quantileexact.md#quantileexact). You can use this function instead of `quantileExact` and specify the weight 1.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles) function.

**Syntax**

``` sql
quantileExactWeighted(level)(expr, weight)
```

Alias: `medianExactWeighted`.

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

Query:

``` sql
SELECT quantileExactWeighted(n, val) FROM t
```

Result:

``` text
┌─quantileExactWeighted(n, val)─┐
│                             1 │
└───────────────────────────────┘
```

**See Also**

- [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
- [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
