---
toc_priority: 202
---

# quantileExact {#quantileexact}

Exactly computes the [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence.

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Therefore, the function consumes `O(n)` memory, where `n` is a number of values that were passed. However, for a small number of values, the function is very effective.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles) function.

**Syntax**

``` sql
quantileExact(level)(expr)
```

Alias: `medianExact`.

**Parameters**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) or [DateTime](../../../sql-reference/data-types/datetime.md).

**Returned value**

-   Quantile of the specified level.

Type:

-   [Float64](../../../sql-reference/data-types/float.md) for numeric data type input.
-   [Date](../../../sql-reference/data-types/date.md) if input values have the `Date` type.
-   [DateTime](../../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

Query:

``` sql
SELECT quantileExact(number) FROM numbers(10)
```

Result:

``` text
┌─quantileExact(number)─┐
│                     5 │
└───────────────────────┘
```

**See Also**

-   [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
-   [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
