---
toc_priority: 310
toc_title: mannWhitneyUTest
---

# mannWhitneyUTest {#mannwhitneyutest}

Applies the Mann-Whitney rank test to independent samples from distinct distributions.

**Syntax**

``` sql
mannWhitneyUTest[(alternative[, continuity_correction])](sample_data, sample_index)
```

Values of both samples are in the `sample_data` column. If `sample_index` equals to 0 then the value in that row belongs to the sample from the first population. Otherwise it belongs to the sample from the second population. 
The null hypothesis is that the probability of value X (sampled from the first population) being greater than Y (sampled from the second population) is equal to the probability of Y being greater than X. Also one-sided hypothesis can be tested.
  

**Parameters**

-   `alternative` — alternative hypothesis. [String](../../../sql-reference/data-types/string.md)
    -   `"two-sided"` (by default);
    -   `"greater"`;
    -   `"less"`.
-   `continuity_correction` - whether to apply continuity correction in the normal approximation for the p-value. (Correction is applied by default.) [UInt64](../../../sql-reference/data-types/int-uint.md)
-   `sample_data` — sample data. [Float64](../../../sql-reference/data-types/float.md).
-   `sample_index` — sample index. [UInt64](../../../sql-reference/data-types/int-uint.md).

**Returned values**

-   `u_statistic` - calculated statistic;
-   `p_value` - calculated p-value.

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
SELECT mannWhitneyUTest('greater')(sample_data, sample_index) FROM mww_ttest;
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