---
slug: /ja/sql-reference/aggregate-functions/reference/quantiletdigestweighted
sidebar_position: 179
---

# quantileTDigestWeighted

数値データ列の近似的な[分位数](https://en.wikipedia.org/wiki/Quantile)を[t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf)アルゴリズムを使用して計算します。この関数は各列要素の重みを考慮に入れます。最大誤差は1%です。メモリ消費は`log(n)`であり、ここで`n`は値の数です。

この関数のパフォーマンスは、[quantile](../../../sql-reference/aggregate-functions/reference/quantile.md#quantile)や[quantileTiming](../../../sql-reference/aggregate-functions/reference/quantiletiming.md#quantiletiming)のパフォーマンスよりも低いです。ただし、状態サイズと精度の比率に関しては、この関数は`quantile`よりも優れています。

結果はクエリを実行する順序に依存し、非決定的です。

異なるレベルの複数の`quantile*`関数をクエリに使用する場合、内部状態は結合されません（つまり、クエリは効率的には動作しません）。この場合、[quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)関数を使用してください。

:::note    
`quantileTDigestWeighted`を小さなデータセットで使用することは[推奨されません](https://github.com/tdunning/t-digest/issues/167#issuecomment-828650275)し、重大な誤差を引き起こす可能性があります。この場合、[`quantileTDigest`](../../../sql-reference/aggregate-functions/reference/quantiletdigest.md)の使用を考慮してください。
:::

**構文**

``` sql
quantileTDigestWeighted(level)(expr, weight)
```

別名: `medianTDigestWeighted`.

**引数**

- `level` — 分位数のレベル。オプションのパラメータです。0から1までの定数の浮動小数点数。`level`の値を`[0.01, 0.99]`の範囲で使用することをお勧めします。デフォルト値: 0.5。`level=0.5`では、関数は[中央値](https://en.wikipedia.org/wiki/Median)を計算します。
- `expr` — 数値データ型、[Date](../../../sql-reference/data-types/date.md)または[DateTime](../../../sql-reference/data-types/datetime.md)のカラム値の式。
- `weight` — 列要素の重みを持つカラム。重みは値の出現回数です。

**返される値**

- 指定されたレベルの近似的な分位数。

型:

- 数値データ型入力の場合は[Float64](../../../sql-reference/data-types/float.md)。
- 入力値が`Date`型の場合は[Date](../../../sql-reference/data-types/date.md)。
- 入力値が`DateTime`型の場合は[DateTime](../../../sql-reference/data-types/datetime.md)。

**例**

クエリ:

``` sql
SELECT quantileTDigestWeighted(number, 1) FROM numbers(10)
```

結果:

``` text
┌─quantileTDigestWeighted(number, 1)─┐
│                                4.5 │
└────────────────────────────────────┘
```

**関連項目**

- [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
- [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
