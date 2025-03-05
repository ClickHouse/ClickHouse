---
slug: /sql-reference/aggregate-functions/reference/meanztest
sidebar_position: 166
sidebar_label: meanZTest
title: "meanZTest"
description: "Applies mean z-test to samples from two populations."
---

# meanZTest

Applies mean z-test to samples from two populations.

**Syntax**

``` sql
meanZTest(population_variance_x, population_variance_y, confidence_level)(sample_data, sample_index)
```

Values of both samples are in the `sample_data` column. If `sample_index` equals to 0 then the value in that row belongs to the sample from the first population. Otherwise it belongs to the sample from the second population.
The null hypothesis is that means of populations are equal. Normal distribution is assumed. Populations may have unequal variance and the variances are known.

**Arguments**

- `sample_data` — Sample data. [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) or [Decimal](../../../sql-reference/data-types/decimal.md).
- `sample_index` — Sample index. [Integer](../../../sql-reference/data-types/int-uint.md).

**Parameters**

- `population_variance_x` — Variance for population x. [Float](../../../sql-reference/data-types/float.md).
- `population_variance_y` — Variance for population y. [Float](../../../sql-reference/data-types/float.md).
- `confidence_level` — Confidence level in order to calculate confidence intervals. [Float](../../../sql-reference/data-types/float.md).

**Returned values**

[Tuple](../../../sql-reference/data-types/tuple.md) with four elements:

- calculated t-statistic. [Float64](../../../sql-reference/data-types/float.md).
- calculated p-value. [Float64](../../../sql-reference/data-types/float.md).
- calculated confidence-interval-low. [Float64](../../../sql-reference/data-types/float.md).
- calculated confidence-interval-high. [Float64](../../../sql-reference/data-types/float.md).


**Example**

Input table:

``` text
┌─sample_data─┬─sample_index─┐
│        20.3 │            0 │
│        21.9 │            0 │
│        22.1 │            0 │
│        18.9 │            1 │
│          19 │            1 │
│        20.3 │            1 │
└─────────────┴──────────────┘
```

Query:

``` sql
SELECT meanZTest(0.7, 0.45, 0.95)(sample_data, sample_index) FROM mean_ztest
```

Result:

``` text
┌─meanZTest(0.7, 0.45, 0.95)(sample_data, sample_index)────────────────────────────┐
│ (3.2841296025548123,0.0010229786769086013,0.8198428246768334,3.2468238419898365) │
└──────────────────────────────────────────────────────────────────────────────────┘
```

## Combinators

The following combinators can be applied to the `meanZTest` function:

### meanZTestIf
Applies mean z-test only to rows that match the given condition.

### meanZTestArray
Applies mean z-test to elements in the array.

### meanZTestMap
Applies mean z-test for each key in the map separately.

### meanZTestSimpleState
Returns the test results with SimpleAggregateFunction type.

### meanZTestState
Returns the intermediate state of test calculation.

### meanZTestMerge
Combines intermediate test states to get the final results.

### meanZTestMergeState
Combines intermediate test states but returns an intermediate state.

### meanZTestForEach
Applies mean z-test to corresponding elements in multiple arrays.

### meanZTestDistinct
Applies mean z-test using distinct values only.

### meanZTestOrDefault
Returns (0, 1, 0, 0) if there are not enough samples to perform the test.

### meanZTestOrNull
Returns NULL if there are not enough samples to perform the test.
