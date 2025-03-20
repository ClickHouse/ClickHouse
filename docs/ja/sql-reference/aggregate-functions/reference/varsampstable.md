---
title: "varSampStable"
slug: /ja/sql-reference/aggregate-functions/reference/varsampstable
sidebar_position: 213
---

## varSampStable

データセットの標本分散を計算します。[`varSamp`](../reference/varsamp.md)とは異なり、この関数は数値的に安定したアルゴリズムを使用します。動作は遅くなりますが、計算誤差を低く抑えます。

**構文**

```sql
varSampStable(x)
```

別名: `VAR_SAMP_STABLE`

**パラメーター**

- `x`: 標本分散を計算したい母集団。[ (U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal*](../../data-types/decimal.md).

**返される値**

- 入力データセットの標本分散を返します。[Float64](../../data-types/float.md).

**実装の詳細**

`varSampStable` 関数は、[`varSamp`](../reference/varsamp.md)と同じ数式を使用して標本分散を計算します。

$$
\sum\frac{(x - \text{mean}(x))^2}{(n - 1)}
$$

ここで：
- `x` はデータセットの各個別データポイントです。
- `mean(x)` はデータセットの算術平均です。
- `n` はデータセットのデータポイントの数です。

**例**

クエリ：

```sql
DROP TABLE IF EXISTS test_data;
CREATE TABLE test_data
(
    x Float64
)
ENGINE = Memory;

INSERT INTO test_data VALUES (10.5), (12.3), (9.8), (11.2), (10.7);

SELECT round(varSampStable(x),3) AS var_samp_stable FROM test_data;
```

レスポンス：

```response
┌─var_samp_stable─┐
│           0.865 │
└─────────────────┘
```
