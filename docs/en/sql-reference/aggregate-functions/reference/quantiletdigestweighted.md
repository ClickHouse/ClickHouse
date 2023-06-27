---
sidebar_position: 208
---

# quantileTDigestWeighted

Computes an approximate [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence using the [t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) algorithm. The function takes into account the weight of each sequence member. The maximum error is 1%. Memory consumption is `log(n)`, where `n` is a number of values.

The performance of the function is lower than performance of [quantile](../../../sql-reference/aggregate-functions/reference/quantile.md#quantile) or [quantileTiming](../../../sql-reference/aggregate-functions/reference/quantiletiming.md#quantiletiming). In terms of the ratio of State size to precision, this function is much better than `quantile`.

The result depends on the order of running the query, and is nondeterministic.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles) function.

:::note    
Using `quantileTDigestWeighted` [is not recommended for tiny data sets](https://github.com/tdunning/t-digest/issues/167#issuecomment-828650275) and can lead to significat error. In this case, consider possibility of using [`quantileTDigest`](../../../sql-reference/aggregate-functions/reference/quantiletdigest.md) instead.
:::

**Syntax**

``` sql
quantileTDigestWeighted(level)(expr, weight)
```

Alias: `medianTDigestWeighted`.

**Arguments**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) or [DateTime](../../../sql-reference/data-types/datetime.md).
-   `weight` — Column with weights of sequence elements. Weight is a number of value occurrences.

**Returned value**

-   Approximate quantile of the specified level.

Type:

-   [Float64](../../../sql-reference/data-types/float.md) for numeric data type input.
-   [Date](../../../sql-reference/data-types/date.md) if input values have the `Date` type.
-   [DateTime](../../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

Query:

``` sql
SELECT quantileTDigestWeighted(number, 1) FROM numbers(10)
```

Result:

``` text
┌─quantileTDigestWeighted(number, 1)─┐
│                                4.5 │
└────────────────────────────────────┘
```

**See Also**

-   [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
-   [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
