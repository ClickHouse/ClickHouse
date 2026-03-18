---
slug: /zh/sql-reference/aggregate-functions/reference/avgweighted
sidebar_position: 107
---

# avgWeighted {#avgweighted}


计算 [加权算术平均值](https://en.wikipedia.org/wiki/Weighted_arithmetic_mean)。

**语法**

``` sql
avgWeighted(x, weight)
```

**参数**

-   `x` — 值。
-   `weight` — 值的加权。

`x` 和 `weight` 的类型必须是
[整数](../../../sql-reference/data-types/int-uint.md), 或
[浮点数](../../../sql-reference/data-types/float.md),
但是可以不一样。

**返回值**

-   `NaN`。 如果所有的权重都等于0 或所提供的权重参数是空。
-   加权平均值。 其他。

类型: 总是[Float64](../../../sql-reference/data-types/float.md).

**示例**

查询:

``` sql
SELECT avgWeighted(x, w)
FROM values('x Int8, w Int8', (4, 1), (1, 0), (10, 2))
```

结果:

``` text
┌─avgWeighted(x, weight)─┐
│                      8 │
└────────────────────────┘
```


**示例**

查询:

``` sql
SELECT avgWeighted(x, w)
FROM values('x Int8, w Int8', (0, 0), (1, 0), (10, 0))
```

结果:

``` text
┌─avgWeighted(x, weight)─┐
│                    nan │
└────────────────────────┘
```

**示例**

查询:

``` sql
CREATE table test (t UInt8) ENGINE = Memory;
SELECT avgWeighted(t) FROM test
```

结果:

``` text
┌─avgWeighted(x, weight)─┐
│                    nan │
└────────────────────────┘
```
