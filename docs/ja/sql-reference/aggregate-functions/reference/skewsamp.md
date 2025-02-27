---
slug: /ja/sql-reference/aggregate-functions/reference/skewsamp
sidebar_position: 186
---

# skewSamp

シーケンスの[標本歪度](https://en.wikipedia.org/wiki/Skewness)を計算します。

渡された値がランダム変数の標本を形成する場合、その歪度の不偏推定量を表します。

``` sql
skewSamp(expr)
```

**引数**

`expr` — 数値を返す[式](../../../sql-reference/syntax.md#syntax-expressions)。

**戻り値**

指定された分布の歪度。型 — [Float64](../../../sql-reference/data-types/float.md)。`n <= 1`（`n`は標本のサイズ）の場合、関数は`nan`を返します。

**例**

``` sql
SELECT skewSamp(value) FROM series_with_value_column;
```

