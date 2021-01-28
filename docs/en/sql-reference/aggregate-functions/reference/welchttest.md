---
toc_priority: 301
toc_title: welchTTest
---

## welchTTest {#welchttest}

Applies Welch's t-test to samples. 
This is a test for the null hypothesis that two independent samples have identical average values. Samples may have unequal variances.

**Syntax**

``` sql
welchTTest(sample_data, sample_index)
```

Values of both samples are in the `sample_data` column. If `sample_index` equals to 0 then the value in that row belongs to the first sample. Otherwise it belongs to the second sample.

**Parameters**

-   `sample_data` — sample data. [Float64](../../../sql-reference/data-types/float.md).
-   `sample_index` — sample index. [Int8](../../../sql-reference/data-types/int-uint.md).

**Returned values**

-   t-statistic - calculated statistic;
-   p-value - calculated p-value.

Type: [Float64](../../../sql-reference/data-types/float.md).


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

-   [Welch's t-test](https://en.wikipedia.org/wiki/Welch%27s_t-test)
-   [studentTTest function](studentttest.md#studentttest)

[Original article](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/welchTTest/) <!--hide-->