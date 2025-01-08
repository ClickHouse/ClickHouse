---
slug: /ja/sql-reference/aggregate-functions/reference/covarsamp
sidebar_position: 124
---

# covarSamp

`Σ((x - x̅)(y - y̅)) / (n - 1)` の値を計算します。

:::note
この関数は数値的に不安定なアルゴリズムを使用します。計算における[数値安定性](https://en.wikipedia.org/wiki/Numerical_stability)が必要な場合は、`covarSampStable` 関数を使用してください。この関数は動作が遅くなりますが、計算誤差を抑えます。
:::

**構文**

```sql
covarSamp(x, y)
```

**引数**

- `x` — 第一の変数。[(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal](../../data-types/decimal.md)。
- `y` — 第二の変数。[(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal](../../data-types/decimal.md)。

**戻り値**

- `x` と `y` 間のサンプル共分散。`n <= 1` の場合、`nan` が返されます。[Float64](../../data-types/float.md)。

**例**

クエリ:

```sql
DROP TABLE IF EXISTS series;
CREATE TABLE series(i UInt32, x_value Float64, y_value Float64) ENGINE = Memory;
INSERT INTO series(i, x_value, y_value) VALUES (1, 5.6,-4.4),(2, -9.6,3),(3, -1.3,-4),(4, 5.3,9.7),(5, 4.4,0.037),(6, -8.6,-7.8),(7, 5.1,9.3),(8, 7.9,-3.6),(9, -8.2,0.62),(10, -3,7.3);
```

```sql
SELECT covarSamp(x_value, y_value)
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
┌─covarSamp(x_value, y_value)─┐
│           7.206275555555556 │
└─────────────────────────────┘
```

クエリ:

```sql
SELECT covarSamp(x_value, y_value)
FROM
(
    SELECT
        x_value,
        y_value
    FROM series LIMIT 1
);

```

結果:

```reference
┌─covarSamp(x_value, y_value)─┐
│                         nan │
└─────────────────────────────┘
```
