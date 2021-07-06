---
toc_priority: 153
---

# kurtPop {#kurtpop}

计算给定序列的 [峰度](https://en.wikipedia.org/wiki/Kurtosis)。

**语法**

``` sql
kurtPop(expr)
```

**参数**

`expr` —  结果为数字的 [表达式](../../../sql-reference/syntax.md#syntax-expressions)。

**返回值**

给定分布的峰度。 类型 — [Float64](../../../sql-reference/data-types/float.md)

**示例**

``` sql
SELECT kurtPop(value) FROM series_with_value_column;
