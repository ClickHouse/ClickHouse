---
slug: /ja/sql-reference/aggregate-functions/reference/kurtpop
sidebar_position: 157
---

# kurtPop

シーケンスの[尖度](https://en.wikipedia.org/wiki/Kurtosis)を計算します。

``` sql
kurtPop(expr)
```

**引数**

`expr` — 数値を返す[式](../../../sql-reference/syntax.md#syntax-expressions)。

**戻り値**

与えられた分布の尖度。型 — [Float64](../../../sql-reference/data-types/float.md)

**例**

``` sql
SELECT kurtPop(value) FROM series_with_value_column;
```
