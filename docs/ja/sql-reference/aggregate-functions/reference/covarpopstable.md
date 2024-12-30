---
slug: /ja/sql-reference/aggregate-functions/reference/covarpopstable
sidebar_position: 123
---

# covarPopStable

母集団共分散の値を計算します:

$$
\frac{\Sigma{(x - \bar{x})(y - \bar{y})}}{n}
$$

これは[covarPop](../reference/covarpop.md)関数に似ていますが、数値的に安定したアルゴリズムを使用します。その結果、`covarPopStable`は`covarPop`よりも遅いですが、より正確な結果を出します。

**構文**

```sql
covarPop(x, y)
```

**引数**

- `x` — 最初の変数。[(U)Int*](../../data-types/int-uint.md)、[Float*](../../data-types/float.md)、[Decimal](../../data-types/decimal.md)。
- `y` — 二番目の変数。[(U)Int*](../../data-types/int-uint.md)、[Float*](../../data-types/float.md)、[Decimal](../../data-types/decimal.md)。

**戻り値**

- `x`と`y`の間の母集団共分散。[Float64](../../data-types/float.md)。

**例**

クエリ:

```sql
DROP TABLE IF EXISTS series;
CREATE TABLE series(i UInt32, x_value Float64, y_value Float64) ENGINE = Memory;
INSERT INTO series(i, x_value, y_value) VALUES (1, 5.6,-4.4),(2, -9.6,3),(3, -1.3,-4),(4, 5.3,9.7),(5, 4.4,0.037),(6, -8.6,-7.8),(7, 5.1,9.3),(8, 7.9,-3.6),(9, -8.2,0.62),(10, -3,7.3);
```

```sql
SELECT covarPopStable(x_value, y_value)
FROM
(
    SELECT
        x_value,
        y_value
    FROM series
);
```

結果:

```reference
┌─covarPopStable(x_value, y_value)─┐
│                         6.485648 │
└──────────────────────────────────┘
```
