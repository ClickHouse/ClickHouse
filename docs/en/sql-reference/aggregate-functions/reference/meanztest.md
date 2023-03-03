---
toc_priority: 303
toc_title: meanZTest
---

# meanZTest {#meanztest}

Applies mean z-test to samples from two populations.

**Syntax**

``` sql
meanZTest(population_variance_x, population_variance_y, confidence_level)(sample_data, sample_index)
```

Values of both samples are in the `sample_data` column. If `sample_index` equals to 0 then the value in that row belongs to the sample from the first population. Otherwise it belongs to the sample from the second population.
The null hypothesis is that means of populations are equal. Normal distribution is assumed. Populations may have unequal variance and the variances are known.

**Arguments**

-   `sample_data` — Sample data. [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) or [Decimal](../../../sql-reference/data-types/decimal.md).
-   `sample_index` — Sample index. [Integer](../../../sql-reference/data-types/int-uint.md).

**Parameters**

-   `population_variance_x` — Variance for population x. [Float](../../../sql-reference/data-types/float.md).
-   `population_variance_y` — Variance for population y. [Float](../../../sql-reference/data-types/float.md).
-   `confidence_level` — Confidence level in order to calculate confidence intervals. [Float](../../../sql-reference/data-types/float.md).

**Returned values**

[Tuple](../../../sql-reference/data-types/tuple.md) with four elements:

-   calculated t-statistic. [Float64](../../../sql-reference/data-types/float.md).
-   calculated p-value. [Float64](../../../sql-reference/data-types/float.md).
-   calculated confidence-interval-low. [Float64](../../../sql-reference/data-types/float.md).
-   calculated confidence-interval-high. [Float64](../../../sql-reference/data-types/float.md).


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


[Original article](https://clickhouse.com/docs/en/sql-reference/aggregate-functions/reference/meanZTest/) <!--hide-->
