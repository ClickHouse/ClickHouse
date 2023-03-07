---
slug: /en/sql-reference/aggregate-functions/reference/quantileGK
sidebar_position: 204
---

# quantileGK

Computes the [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence using the [Greenwald-Khanna](http://infolab.stanford.edu/~datar/courses/cs361a/papers/quantiles.pdf) algorithm.


**Syntax**

``` sql
quantileGK(accuracy, level)(expr)
```

Alias: `medianGK`.

**Arguments**

-   `accuracy` — Accuracy of quantile. Constant positive integer. The larger the better.

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. Default value: 0.5. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median).

-   `expr` — Expression over the column values resulting in numeric [data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) or [DateTime](../../../sql-reference/data-types/datetime.md).


**Returned value**

-   Quantile of the specified level and accuracy.


Type:

-   [Float64](../../../sql-reference/data-types/float.md) for numeric data type input.
-   [Date](../../../sql-reference/data-types/date.md) if input values have the `Date` type.
-   [DateTime](../../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

``` sql
WITH arrayJoin([0, 6, 7, 9, 10]) AS x
SELECT quantileGK(100, 0.5)(x)


┌─quantileGK(100, 0.5)(x)─┐
│                       7 │
└─────────────────────────┘
```


**See Also**

-   [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
-   [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
