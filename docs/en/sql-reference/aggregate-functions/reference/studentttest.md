---
toc_priority: 300
toc_title: studentTTest
---

# studentTTest {#studentttest}

Applies Student's t-test to samples from two populations. 

**Syntax**

``` sql
studentTTest(sample_data, sample_index)
```

Values of both samples are in the `sample_data` column. If `sample_index` equals to 0 then the value in that row belongs to the sample from the first population. Otherwise it belongs to the sample from the second population.
The null hypothesis is that means of populations are equal. Normal distribution with equal variances is assumed.

**Arguments**

-   `sample_data` — Sample data. [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) or [Decimal](../../../sql-reference/data-types/decimal.md).
-   `sample_index` — Sample index. [Integer](../../../sql-reference/data-types/int-uint.md).

**Returned values**

[Tuple](../../../sql-reference/data-types/tuple.md) with two elements:

-   calculated t-statistic. [Float64](../../../sql-reference/data-types/float.md).
-   calculated p-value. [Float64](../../../sql-reference/data-types/float.md).


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

**See Also**

-   [Student's t-test](https://en.wikipedia.org/wiki/Student%27s_t-test)
-   [welchTTest function](welchttest.md#welchttest)

[Original article](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/studentttest/) <!--hide-->
