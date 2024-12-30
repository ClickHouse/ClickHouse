---
title: "varSamp"
slug: /ja/sql-reference/aggregate-functions/reference/varSamp
sidebar_position: 212
---

## varSamp

データセットの標本分散を計算します。

**構文**

```sql
varSamp(x)
```

エイリアス: `VAR_SAMP`.

**パラメータ**

- `x`: 標本分散を計算したい母集団。[(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal*](../../data-types/decimal.md)。

**返される値**

- 入力データセット `x` の標本分散を返します。[Float64](../../data-types/float.md)。

**実装の詳細**

`varSamp` 関数は次の公式を使用して標本分散を計算します：

$$
\sum\frac{(x - \text{mean}(x))^2}{(n - 1)}
$$

ここで：

- `x` はデータセット内の各データポイントです。
- `mean(x)` はデータセットの算術平均です。
- `n` はデータセット内のデータポイント数です。

この関数は入力データセットが大きな母集団からの標本を表していると仮定します。全体の母集団の分散を計算したい場合（完全なデータセットがある場合）は、代わりに [`varPop`](../reference/varpop.md) を使用してください。

**例**

クエリ:

```sql
DROP TABLE IF EXISTS test_data;
CREATE TABLE test_data
(
    x Float64
)
ENGINE = Memory;

INSERT INTO test_data VALUES (10.5), (12.3), (9.8), (11.2), (10.7);

SELECT round(varSamp(x),3) AS var_samp FROM test_data;
```

レスポンス:

```response
┌─var_samp─┐
│    0.865 │
└──────────┘
```
