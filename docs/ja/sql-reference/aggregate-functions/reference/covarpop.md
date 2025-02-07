---
slug: /ja/sql-reference/aggregate-functions/reference/covarpop
sidebar_position: 121
---

# covarPop

母集団分散共分散を計算します:

$$
\frac{\Sigma{(x - \bar{x})(y - \bar{y})}}{n}
$$

:::note
この関数は数値的に不安定なアルゴリズムを使用します。計算において[数値的安定性](https://en.wikipedia.org/wiki/Numerical_stability)が必要な場合は、[`covarPopStable`](../reference/covarpopstable.md)関数を使用してください。これは計算速度が遅くなりますが、計算誤差が少なくなります。
:::

**構文**

```sql
covarPop(x, y)
```

**引数**

- `x` — 最初の変数。[(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal](../../data-types/decimal.md)。
- `y` — 2番目の変数。[(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal](../../data-types/decimal.md)。

**返される値**

- `x` と `y` の母集団共分散。[Float64](../../data-types/float.md)。

**例**

クエリ:

```sql
DROP TABLE IF EXISTS series;
CREATE TABLE series(i UInt32, x_value Float64, y_value Float64) ENGINE = Memory;
INSERT INTO series(i, x_value, y_value) VALUES (1, 5.6, -4.4),(2, -9.6, 3),(3, -1.3, -4),(4, 5.3, 9.7),(5, 4.4, 0.037),(6, -8.6, -7.8),(7, 5.1, 9.3),(8, 7.9, -3.6),(9, -8.2, 0.62),(10, -3, 7.3);
```

```sql
SELECT covarPop(x_value, y_value)
FROM series;
```

結果:

```reference
┌─covarPop(x_value, y_value)─┐
│                   6.485648 │
└────────────────────────────┘
```
