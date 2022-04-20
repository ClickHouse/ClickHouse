---
sidebar_position: 154
---

# kurtSamp {#kurtsamp}

计算给定序列的 [峰度样本](https://en.wikipedia.org/wiki/Kurtosis)。
它表示随机变量峰度的无偏估计，如果传递的值形成其样本。

**语法**

``` sql
kurtSamp(expr)
```

**参数**

`expr` — 结果为数字的 [表达式](../../../sql-reference/syntax.md#syntax-expressions)。

**返回值**

给定序列的峰度。类型 — [Float64](../../../sql-reference/data-types/float.md)。 如果 `n <= 1` (`n` 是样本的大小），则该函数返回 `nan`。

**示例**

``` sql
SELECT kurtSamp(value) FROM series_with_value_column;
```
