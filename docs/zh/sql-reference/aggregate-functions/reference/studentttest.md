---
toc_priority: 300
toc_title: studentTTest
---

# studentTTest {#studentttest}

对两个总体的样本应用t检验。

**语法**

``` sql
studentTTest(sample_data, sample_index)
```

两个样本的值都在 `sample_data` 列中。如果 `sample_index` 等于 0，则该行的值属于第一个总体的样本。 反之属于第二个总体的样本。
零假设是总体的均值相等。假设为方差相等的正态分布。

**参数**

-   `sample_data` — 样本数据。[Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) 或 [Decimal](../../../sql-reference/data-types/decimal.md)。
-   `sample_index` — 样本索引。[Integer](../../../sql-reference/data-types/int-uint.md)。

**返回值**

[元组](../../../sql-reference/data-types/tuple.md)，有两个元素:

-   计算出的t统计量。 [Float64](../../../sql-reference/data-types/float.md)。
-   计算出的p值。[Float64](../../../sql-reference/data-types/float.md)。


**示例**

输入表:

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

查询:

``` sql
SELECT studentTTest(sample_data, sample_index) FROM student_ttest;
```

结果:

``` text
┌─studentTTest(sample_data, sample_index)───┐
│ (-0.21739130434783777,0.8385421208415731) │
└───────────────────────────────────────────┘
```

**参见**

-   [Student's t-test](https://en.wikipedia.org/wiki/Student%27s_t-test)
-   [welchTTest function](../../../sql-reference/aggregate-functions/reference/welchttest.md#welchttest)
