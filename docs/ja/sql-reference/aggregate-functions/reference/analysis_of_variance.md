---
slug: /ja/sql-reference/aggregate-functions/reference/analysis_of_variance
sidebar_position: 101
---

# analysisOfVariance

一元配置分散分析（ANOVAテスト）を行うための統計的テストを提供します。これは、複数の正規分布に従う観測値のグループに対して、全てのグループが同じ平均を持つかどうかを調べるテストです。

**構文**

```sql
analysisOfVariance(val, group_no)
```

エイリアス: `anova`

**パラメータ**
- `val`: 値。
- `group_no`: `val`が属するグループ番号。

:::note
グループは0から列挙され、テストを実行するには少なくとも2つのグループが必要です。
観測値の数が1つ以上のグループが少なくとも1つ存在する必要があります。
:::

**返される値**

- `(f_statistic, p_value)`。[タプル](../../data-types/tuple.md)([Float64](../../data-types/float.md), [Float64](../../data-types/float.md))。

**例**

クエリ:

```sql
SELECT analysisOfVariance(number, number % 2) FROM numbers(1048575);
```

結果:

```response
┌─analysisOfVariance(number, modulo(number, 2))─┐
│ (0,1)                                         │
└───────────────────────────────────────────────┘
```
