---
slug: /en/sql-reference/aggregate-functions/reference/welchttest
sidebar_position: 214
sidebar_label: welchTTest
---

# welchTTest

Applies Welch's t-test to samples from two populations.

**Syntax**

``` sql
welchTTest([confidence_level])(sample_data, sample_index)
```

Values of both samples are in the `sample_data` column. If `sample_index` equals to 0 then the value in that row belongs to the sample from the first population. Otherwise it belongs to the sample from the second population.
The null hypothesis is that means of populations are equal. Normal distribution is assumed. Populations may have unequal variance.

**Arguments**

- `sample_data` — Sample data. [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) or [Decimal](../../../sql-reference/data-types/decimal.md).
- `sample_index` — Sample index. [Integer](../../../sql-reference/data-types/int-uint.md).

**Parameters**

- `confidence_level` — Confidence level in order to calculate confidence intervals. [Float](../../../sql-reference/data-types/float.md).

**Returned values**

[Tuple](../../../sql-reference/data-types/tuple.md) with two or four elements (if the optional `confidence_level` is specified)

- calculated t-statistic. [Float64](../../../sql-reference/data-types/float.md).
- calculated p-value. [Float64](../../../sql-reference/data-types/float.md).
- calculated confidence-interval-low. [Float64](../../../sql-reference/data-types/float.md).
- calculated confidence-interval-high. [Float64](../../../sql-reference/data-types/float.md).


**Example**

Input table:

``` text
┌─sample_data─┬─sample_index─┐
│        20.3 │            0 │
│        22.1 │            0 │
│        21.9 │            0 │
│        18.9 │            1 │
│        20.3 │            1 │
│          19 │            1 │
└─────────────┴──────────────┘
```

Query:

``` sql
SELECT welchTTest(sample_data, sample_index) FROM welch_ttest;
```

Result:

``` text
┌─welchTTest(sample_data, sample_index)─────┐
│ (2.7988719532211235,0.051807360348581945) │
└───────────────────────────────────────────┘
```

**See Also**

- [Welch's t-test](https://en.wikipedia.org/wiki/Welch%27s_t-test)
- [studentTTest function](studentttest.md#studentttest)
