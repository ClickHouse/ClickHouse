---
slug: /ja/sql-reference/aggregate-functions/reference/quantiletimingweighted
sidebar_position: 181
---

# quantileTimingWeighted

決定された精度で、それぞれのシーケンスメンバーの重みに基づいて、数値データシーケンスの[分位数](https://en.wikipedia.org/wiki/Quantile)を計算します。

この結果は決定論的（クエリ処理順序に依存しない）です。この関数は、ウェブページの読み込み時間やバックエンドの応答時間のような分布を記述するシーケンスでの作業に最適化されています。

異なるレベルの複数の`quantile*`関数をクエリで使用する場合、内部状態は結合されません（つまり、クエリは本来の効率より低下します）。この場合には、[quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)関数を使用してください。

**構文**

``` sql
quantileTimingWeighted(level)(expr, weight)
```

別名: `medianTimingWeighted`.

**引数**

- `level` — 分位数のレベル。オプションのパラメータ。0から1の範囲の定数の浮動小数点数。`level`の値は`[0.01, 0.99]`の範囲を推奨します。デフォルト値：0.5。`level=0.5`の場合、関数は[中央値](https://en.wikipedia.org/wiki/Median)を計算します。

- `expr` — [カラム](../../../sql-reference/syntax.md#syntax-expressions)値に対する式で、[Float\*](../../../sql-reference/data-types/float.md)型の番号を返します。

        - 負の値が関数に渡された場合、その動作は未定義です。
        - 値が30,000（ページ読み込み時間が30秒以上）を超える場合、30,000とみなされます。

- `weight` — シーケンス要素の重みを持つカラム。重みは値の出現回数です。

**精度**

計算は以下の場合に正確です：

- 値の総数が5670を超えない場合。
- 値の総数が5670を超えるが、ページ読み込み時間が1024ms未満の場合。

それ以外の場合、計算の結果は16msの倍数に丸められます。

:::note    
ページ読み込み時間の分位数を計算するには、この関数は[quantile](../../../sql-reference/aggregate-functions/reference/quantile.md#quantile)よりも効率的で正確です。
:::

**返される値**

- 指定されたレベルの分位数。

タイプ: `Float32`.

:::note    
関数に値が渡されない場合（`quantileTimingIf`を使用する場合）、[NaN](../../../sql-reference/data-types/float.md#data_type-float-nan-inf)が返されます。これは、結果がゼロとなる場合と区別するためです。`NaN`値のソートに関しては、[ORDER BY句](../../../sql-reference/statements/select/order-by.md#select-order-by)を参照してください。
:::

**例**

入力テーブル:

``` text
┌─response_time─┬─weight─┐
│            68 │      1 │
│           104 │      2 │
│           112 │      3 │
│           126 │      2 │
│           138 │      1 │
│           162 │      1 │
└───────────────┴────────┘
```

クエリ:

``` sql
SELECT quantileTimingWeighted(response_time, weight) FROM t
```

結果:

``` text
┌─quantileTimingWeighted(response_time, weight)─┐
│                                           112 │
└───────────────────────────────────────────────┘
```

# quantilesTimingWeighted

`quantileTimingWeighted`と同様ですが、分位数レベルの複数のパラメータを受け取り、それらの分位数の多くの値で満たされた配列を返します。

**例**

入力テーブル:

``` text
┌─response_time─┬─weight─┐
│            68 │      1 │
│           104 │      2 │
│           112 │      3 │
│           126 │      2 │
│           138 │      1 │
│           162 │      1 │
└───────────────┴────────┘
```

クエリ:

``` sql
SELECT quantilesTimingWeighted(0,5, 0.99)(response_time, weight) FROM t
```

結果:

``` text
┌─quantilesTimingWeighted(0.5, 0.99)(response_time, weight)─┐
│ [112,162]                                                 │
└───────────────────────────────────────────────────────────┘
```

**関連項目**

- [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
- [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
