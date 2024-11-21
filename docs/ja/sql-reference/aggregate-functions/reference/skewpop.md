---
slug: /ja/sql-reference/aggregate-functions/reference/skewpop
sidebar_position: 185
---

# skewPop

シーケンスの[歪度](https://en.wikipedia.org/wiki/Skewness)を計算します。

``` sql
skewPop(expr)
```

**引数**

`expr` — 数値を返す[式](../../../sql-reference/syntax.md#syntax-expressions)。

**戻り値**

与えられた分布の歪度。型 — [Float64](../../../sql-reference/data-types/float.md)

**例**

``` sql
SELECT skewPop(value) FROM series_with_value_column;
```

