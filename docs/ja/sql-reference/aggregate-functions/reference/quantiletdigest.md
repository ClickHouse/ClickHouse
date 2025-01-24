---
slug: /ja/sql-reference/aggregate-functions/reference/quantiletdigest
sidebar_position: 178
---

# quantileTDigest

[t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf)アルゴリズムを使用して、数値データ列の近似的な[分位](https://en.wikipedia.org/wiki/Quantile)を計算します。

メモリ消費は`log(n)`であり、ここで`n`は値の数です。結果はクエリの実行順序に依存しており、非決定的です。

この関数のパフォーマンスは、[quantile](../../../sql-reference/aggregate-functions/reference/quantile.md#quantile)や[quantileTiming](../../../sql-reference/aggregate-functions/reference/quantiletiming.md#quantiletiming)のパフォーマンスよりも低くなっています。ステートサイズと精度の比率の観点では、この関数は`quantile`よりも優れています。

異なるレベルの`quantile*`関数をクエリで使用する場合、内部状態は結合されません（つまり、クエリは本来可能な効率よりも低効率で動作します）。この場合、[quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)関数を使用してください。

**構文**

``` sql
quantileTDigest(level)(expr)
```

エイリアス: `medianTDigest`.

**引数**

- `level` — 分位のレベル。オプションのパラメータです。0から1までの定数浮動小数点数。`level`値を`[0.01, 0.99]`の範囲で使用することをお勧めします。デフォルト値は0.5です。`level=0.5`の場合、関数は[中央値](https://en.wikipedia.org/wiki/Median)を計算します。
- `expr` — 数値[データ型](../../../sql-reference/data-types/index.md#data_types)、[Date](../../../sql-reference/data-types/date.md)または[DateTime](../../../sql-reference/data-types/datetime.md)を結果とするカラム値に対する式。

**返される値**

- 指定されたレベルの近似的な分位。

型:

- 数値データ型入力の場合: [Float64](../../../sql-reference/data-types/float.md)
- 入力値が`Date`型の場合: [Date](../../../sql-reference/data-types/date.md)
- 入力値が`DateTime`型の場合: [DateTime](../../../sql-reference/data-types/datetime.md)

**例**

クエリ:

``` sql
SELECT quantileTDigest(number) FROM numbers(10)
```

結果:

``` text
┌─quantileTDigest(number)─┐
│                     4.5 │
└─────────────────────────┘
```

**関連項目**

- [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
- [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
