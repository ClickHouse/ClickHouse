---
slug: /ja/sql-reference/aggregate-functions/reference/median
sidebar_position: 167
---

# median

`median*` 関数は、対応する `quantile*` 関数のエイリアスです。これらは数値データサンプルの中央値を計算します。

関数:

- `median` — [quantile](../../../sql-reference/aggregate-functions/reference/quantile.md#quantile) のエイリアス。
- `medianDeterministic` — [quantileDeterministic](../../../sql-reference/aggregate-functions/reference/quantiledeterministic.md#quantiledeterministic) のエイリアス。
- `medianExact` — [quantileExact](../../../sql-reference/aggregate-functions/reference/quantileexact.md#quantileexact) のエイリアス。
- `medianExactWeighted` — [quantileExactWeighted](../../../sql-reference/aggregate-functions/reference/quantileexactweighted.md#quantileexactweighted) のエイリアス。
- `medianTiming` — [quantileTiming](../../../sql-reference/aggregate-functions/reference/quantiletiming.md#quantiletiming) のエイリアス。
- `medianTimingWeighted` — [quantileTimingWeighted](../../../sql-reference/aggregate-functions/reference/quantiletimingweighted.md#quantiletimingweighted) のエイリアス。
- `medianTDigest` — [quantileTDigest](../../../sql-reference/aggregate-functions/reference/quantiletdigest.md#quantiletdigest) のエイリアス。
- `medianTDigestWeighted` — [quantileTDigestWeighted](../../../sql-reference/aggregate-functions/reference/quantiletdigestweighted.md#quantiletdigestweighted) のエイリアス。
- `medianBFloat16` — [quantileBFloat16](../../../sql-reference/aggregate-functions/reference/quantilebfloat16.md#quantilebfloat16) のエイリアス。
- `medianDD` — [quantileDD](../../../sql-reference/aggregate-functions/reference/quantileddsketch.md#quantileddsketch) のエイリアス。

**例**

入力テーブル:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

クエリ:

``` sql
SELECT medianDeterministic(val, 1) FROM t;
```

結果:

``` text
┌─medianDeterministic(val, 1)─┐
│                         1.5 │
└─────────────────────────────┘
```
