---
sidebar_position: 301
sidebar_label: welchTTest
---

# welchTTest {#welchttest}

对两个总体的样本应用 Welch t检验。

**语法**

``` sql
welchTTest(sample_data, sample_index)
```
两个样本的值都在 `sample_data` 列中。如果 `sample_index` 等于 0，则该行的值属于第一个总体的样本。 反之属于第二个总体的样本。
零假设是群体的均值相等。假设为正态分布。总体可能具有不相等的方差。

**参数**

-   `sample_data` — 样本数据。[Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) 或 [Decimal](../../../sql-reference/data-types/decimal.md).
-   `sample_index` — 样本索引。[Integer](../../../sql-reference/data-types/int-uint.md).

**返回值**

[元组](../../../sql-reference/data-types/tuple.md)，有两个元素:

-   计算出的t统计量。 [Float64](../../../sql-reference/data-types/float.md)。
-   计算出的p值。[Float64](../../../sql-reference/data-types/float.md)。

**示例**

输入表:

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

查询:

``` sql
SELECT welchTTest(sample_data, sample_index) FROM welch_ttest;
```

结果:

``` text
┌─welchTTest(sample_data, sample_index)─────┐
│ (2.7988719532211235,0.051807360348581945) │
└───────────────────────────────────────────┘
```

**参见**

-   [Welch's t-test](https://en.wikipedia.org/wiki/Welch%27s_t-test)
-   [studentTTest function](../../../sql-reference/aggregate-functions/reference/studentttest.md#studentttest)
