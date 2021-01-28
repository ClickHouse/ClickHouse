---
toc_priority: 300
toc_title: studentTTest
---

## studentTTest {#studentttest}

Calculates the T-test for the means of two independent samples. 
This is a test for the null hypothesis that 2 independent samples have identical average values. This test assumes that samples have identical variances.

**Syntax**

``` sql
studentTTest(sample_data, sample_index)
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
│        21.1 │            0 │
│        21.9 │            1 │
│        21.7 │            0 │
│        19.9 │            1 │
│        21.8 │            1 │
└─────────────┴──────────────┘
```

Query:

``` sql
SELECT studentTTest(sample_data, sample_index) FROM student_ttest;
```

Result:

``` text
┌─studentTTest(sample_data, sample_index)───┐
│ (-0.21739130434783777,0.8385421208415731) │
└───────────────────────────────────────────┘
```

[Original article](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/studentttest/) <!--hide-->