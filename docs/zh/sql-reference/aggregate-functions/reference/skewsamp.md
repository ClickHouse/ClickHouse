---
sidebar_position: 151
---

# skewSamp {#skewsamp}

计算给定序列的 [样本偏度] (https://en.wikipedia.org/wiki/Skewness)。

如果传递的值形成其样本，它代表了一个随机变量的偏度的无偏估计。

**语法**

``` sql
skewSamp(expr)
```

**参数**

`expr` — [表达式](../../../sql-reference/syntax.md#syntax-expressions) 返回一个数字。

**返回值**

给定分布的偏度。 类型 — [Float64](../../../sql-reference/data-types/float.md)。 如果 `n <= 1` (`n` 样本的大小), 函数返回 `nan`。

**示例**

``` sql
SELECT skewSamp(value) FROM series_with_value_column;
```
