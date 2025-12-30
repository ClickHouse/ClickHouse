---
description: 'Computes quantile of a histogram using linear interpolation.'
sidebar_position: 364
slug: /sql-reference/aggregate-functions/reference/quantilePrometheusHistogram
title: 'quantilePrometheusHistogram'
doc_type: 'reference'
---

# quantilePrometheusHistogram

Computes [quantile](https://en.wikipedia.org/wiki/Quantile) of a histogram using linear interpolation, taking into account the cumulative value and upper bounds of each histogram bucket.

To get the interpolated value, all the passed values are combined into an array, which are then sorted by their corresponding bucket upper bound values. Quantile interpolation is then performed similarly to the PromQL [histogram_quantile()](https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile) function on a classic histogram, performing a linear interpolation using the lower and upper bound of the bucket in which the quantile position is found.

**Syntax**

```sql
quantilePrometheusHistogram(level)(bucket_upper_bound, cumulative_bucket_value)
```

**Arguments**

- `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: `0.5`. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median).

- `bucket_upper_bound` — Upper bounds of the histogram buckets.

  - The highest bucket must have an upper bound of `+Inf`.

- `cumulative_bucket_value` — Cumulative [UInt](../../../sql-reference/data-types/int-uint) or [Float64](../../../sql-reference/data-types/float.md) values of the histogram buckets.

  - Values must be monotonically increasing as the bucket upper bound increases.

**Returned value**

- Quantile of the specified level.

Type:

- `Float64`.

**Example**

Input table:

```text
   ┌─bucket_upper_bound─┬─cumulative_bucket_value─┐
1. │                  0 │                       6 │
2. │                0.5 │                      11 │
3. │                  1 │                      14 │
4. │                inf │                      19 │
   └────────────────────┴─────────────────────────┘
```

Result:

```text
   ┌─quantilePrometheusHistogram(bucket_upper_bound, cumulative_bucket_value)─┐
1. │                                                                     0.35 │
   └──────────────────────────────────────────────────────────────────────────┘
```

**See Also**

- [median](/sql-reference/aggregate-functions/reference/median)
- [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
