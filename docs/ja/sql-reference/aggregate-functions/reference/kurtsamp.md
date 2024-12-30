---
slug: /ja/sql-reference/aggregate-functions/reference/kurtsamp
sidebar_position: 158
---

# kurtSamp

シーケンスの[標本尖度](https://en.wikipedia.org/wiki/Kurtosis)を計算します。

これは渡された値が標本を形成する場合、確率変数の尖度の無偏推定量を表します。

``` sql
kurtSamp(expr)
```

**引数**

`expr` — 数値を返す[式](../../../sql-reference/syntax.md#syntax-expressions)。

**返される値**

与えられた分布の尖度。型 — [Float64](../../../sql-reference/data-types/float.md)。もし `n <= 1`（`n` は標本のサイズ）なら、関数は `nan` を返します。

**例**

``` sql
SELECT kurtSamp(value) FROM series_with_value_column;
```
