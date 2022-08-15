---
sidebar_position: 204
---

# quantileTiming

With the determined precision computes the [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence.

The result is deterministic (it does not depend on the query processing order). The function is optimized for working with sequences which describe distributions like loading web pages times or backend response times.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles) function.

**Syntax**

``` sql
quantileTiming(level)(expr)
```

Alias: `medianTiming`.

**Arguments**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median).

-   `expr` — [Expression](../../../sql-reference/syntax.md#syntax-expressions) over a column values returning a [Float\*](../../../sql-reference/data-types/float.md)-type number.

    -   If negative values are passed to the function, the behavior is undefined.
    -   If the value is greater than 30,000 (a page loading time of more than 30 seconds), it is assumed to be 30,000.

**Accuracy**

The calculation is accurate if:

-   Total number of values does not exceed 5670.
-   Total number of values exceeds 5670, but the page loading time is less than 1024ms.

Otherwise, the result of the calculation is rounded to the nearest multiple of 16 ms.

:::note    
For calculating page loading time quantiles, this function is more effective and accurate than [quantile](../../../sql-reference/aggregate-functions/reference/quantile.md#quantile).
:::

**Returned value**

-   Quantile of the specified level.

Type: `Float32`.

:::note    
If no values are passed to the function (when using `quantileTimingIf`), [NaN](../../../sql-reference/data-types/float.md#data_type-float-nan-inf) is returned. The purpose of this is to differentiate these cases from cases that result in zero. See [ORDER BY clause](../../../sql-reference/statements/select/order-by.md#select-order-by) for notes on sorting `NaN` values.
:::

**Example**

Input table:

``` text
┌─response_time─┐
│            72 │
│           112 │
│           126 │
│           145 │
│           104 │
│           242 │
│           313 │
│           168 │
│           108 │
└───────────────┘
```

Query:

``` sql
SELECT quantileTiming(response_time) FROM t
```

Result:

``` text
┌─quantileTiming(response_time)─┐
│                           126 │
└───────────────────────────────┘
```

**See Also**

-   [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
-   [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
