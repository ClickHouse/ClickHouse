---
slug: /ja/sql-reference/aggregate-functions/reference/corr
sidebar_position: 117
---

# corr

[ピアソン積率相関係数](https://en.wikipedia.org/wiki/Pearson_correlation_coefficient)を計算します:

$$
\frac{\Sigma{(x - \bar{x})(y - \bar{y})}}{\sqrt{\Sigma{(x - \bar{x})^2} * \Sigma{(y - \bar{y})^2}}}
$$

:::note
この関数は数値的に不安定なアルゴリズムを使用しています。計算において[数値的安定性](https://en.wikipedia.org/wiki/Numerical_stability)が必要な場合は、[`corrStable`](../reference/corrstable.md) 関数を使用してください。こちらは遅いですが、より正確な結果を提供します。
:::

**構文**

```sql
corr(x, y)
```

**引数**

- `x` — 最初の変数。[(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal](../../data-types/decimal.md)。
- `y` — 二番目の変数。[(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal](../../data-types/decimal.md)。

**戻り値**

- ピアソン積率相関係数。[Float64](../../data-types/float.md)。

**例**

クエリ:

```sql
DROP TABLE IF EXISTS series;
CREATE TABLE series
(
    i UInt32,
    x_value Float64,
    y_value Float64
)
ENGINE = Memory;
INSERT INTO series(i, x_value, y_value) VALUES (1, 5.6, -4.4),(2, -9.6, 3),(3, -1.3, -4),(4, 5.3, 9.7),(5, 4.4, 0.037),(6, -8.6, -7.8),(7, 5.1, 9.3),(8, 7.9, -3.6),(9, -8.2, 0.62),(10, -3, 7.3);
```

```sql
SELECT corr(x_value, y_value)
FROM series;
```

結果:

```response
┌─corr(x_value, y_value)─┐
│     0.1730265755453256 │
└────────────────────────┘
```
