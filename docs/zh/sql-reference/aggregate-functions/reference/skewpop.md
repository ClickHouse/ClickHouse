---
sidebar_position: 150
---

# skewPop {#skewpop}

计算给定序列的 [偏度] (https://en.wikipedia.org/wiki/Skewness)。

**语法**

``` sql
skewPop(expr)
```

**参数**

`expr` — [表达式](../../../sql-reference/syntax.md#syntax-expressions) 返回一个数字。

**返回值**

给定分布的偏度。类型 — [Float64](../../../sql-reference/data-types/float.md)

**示例**

``` sql
SELECT skewPop(value) FROM series_with_value_column;
```
