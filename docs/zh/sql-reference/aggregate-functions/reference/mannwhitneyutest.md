---
toc_priority: 310
toc_title: mannWhitneyUTest
---

# mannWhitneyUTest {#mannwhitneyutest}

对两个总体的样本应用 Mann-Whitney 秩检验。

**语法**

``` sql
mannWhitneyUTest[(alternative[, continuity_correction])](sample_data, sample_index)
```

两个样本的值都在 `sample_data` 列中。如果 `sample_index` 等于 0，则该行的值属于第一个总体的样本。 反之属于第二个总体的样本。
零假设是两个总体随机相等。也可以检验单边假设。该检验不假设数据具有正态分布。

**参数**

-   `sample_data` — 样本数据。[Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) 或 [Decimal](../../../sql-reference/data-types/decimal.md)。
-   `sample_index` — 样本索引。[Integer](../../../sql-reference/data-types/int-uint.md).

**参数**

-   `alternative` — 供选假设。(可选，默认值是: `'two-sided'` 。) [String](../../../sql-reference/data-types/string.md)。
    -   `'two-sided'`;
    -   `'greater'`;
    -   `'less'`。
-   `continuity_correction` — 如果不为0，那么将对p值进行正态近似的连续性修正。(可选，默认：1。) [UInt64](../../../sql-reference/data-types/int-uint.md)。

**返回值**

[元组](../../../sql-reference/data-types/tuple.md)，有两个元素:

-   计算出U统计量。[Float64](../../../sql-reference/data-types/float.md)。
-   计算出的p值。[Float64](../../../sql-reference/data-types/float.md)。


**示例**

输入表:

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

查询:

``` sql
SELECT mannWhitneyUTest('greater')(sample_data, sample_index) FROM mww_ttest;
```

结果:

``` text
┌─mannWhitneyUTest('greater')(sample_data, sample_index)─┐
│ (9,0.04042779918503192)                                │
└────────────────────────────────────────────────────────┘
```

**参见**

-   [Mann–Whitney U test](https://en.wikipedia.org/wiki/Mann%E2%80%93Whitney_U_test)
-   [Stochastic ordering](https://en.wikipedia.org/wiki/Stochastic_ordering)
