---
toc_priority: 310
toc_title: mannWhitneyUTest
---

## mannWhitneyUTest {#mannwhitneyutest}

Applies the Mann-Whitney rank test to independent samples from distinct distributions.

**Syntax**

``` sql
mannWhitneyUTest(alternative)(sample_data, sample_index)
```
Values X and Y are randomly selected from two populations. The null hypothesis is that the probability of X being greater than Y is equal to the probability of Y being greater than X. There are also one-sided hypothesises which may be tested.
  
Values of both samples are in the `sample_data` column. If `sample_index` equals to 0 then the value in that row belongs to the first sample. Otherwise it belongs to the second sample. 


**Parameters**

-   `alternative` — alternative hypothesis. [String](../../../sql-reference/data-types/string.md)
    -   `"two-sided"`;
    -   `"greater"`;
    -   `"less"`;
    -   if parameter is not specified then default two sided hypothesis is tested.
-   `sample_data` — sample data. [Float64](../../../sql-reference/data-types/float.md).
-   `sample_index` — sample index. [Int8](../../../sql-reference/data-types/int-uint.md).

**Returned values**

-   statistic - calculated statistic;
-   p-value - calculated p-value.

Type: [Float64](../../../sql-reference/data-types/float.md).


**Example**

Input table:

``` text
┌─sample_data─┬─sample_index─┐
│          10 │            0 │
│          11 │            0 │
│          12 │            0 │
│           1 │            1 │
│           2 │            1 │
│           3 │            1 │
└─────────────┴──────────────┘
```

Query:

``` sql
SELECT mannWhitneyUTest('greater')(sample_data, sample_index)
FROM mww_ttest;
```

Result:

``` text
┌─mannWhitneyUTest('greater')(sample_data, sample_index)─┐
│ (9,0.04042779918503192)                                │
└────────────────────────────────────────────────────────┘
```

**See Also**

-   [Mann–Whitney U test](https://en.wikipedia.org/wiki/Mann%E2%80%93Whitney_U_test)

[Original article](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/mannwhitneyutest/) <!--hide-->