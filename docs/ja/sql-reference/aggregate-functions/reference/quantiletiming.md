---  
slug: /ja/sql-reference/aggregate-functions/reference/quantiletiming  
sidebar_position: 180  
---

# quantileTiming

定められた精度で数値データシーケンスの[分位数](https://en.wikipedia.org/wiki/Quantile)を計算します。

結果は決定論的です（クエリ処理の順序には依存しません）。この関数は、ウェブページの読み込み時間やバックエンドの応答時間のような分布を説明するシーケンスでの動作に最適化されています。

異なるレベルの複数の`quantile*`関数をクエリで使用する場合、内部状態は結合されません（つまり、クエリは可能な限り効率的に動作しません）。この場合は、[quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)関数を使用してください。

**構文**

``` sql
quantileTiming(level)(expr)
```

エイリアス: `medianTiming`.

**引数**

- `level` — 分位数のレベル。オプションのパラメータです。0から1までの定数の浮動小数点数。`level`の値は `[0.01, 0.99]` の範囲を使用することをお勧めします。デフォルト値は0.5です。`level=0.5`では、関数は[中央値](https://en.wikipedia.org/wiki/Median)を計算します。

- `expr` — [カラム](../../../sql-reference/syntax.md#syntax-expressions)値に対する式で、[Float\*](../../../sql-reference/data-types/float.md)-タイプの数を返します。

    - 負の値が関数に渡された場合、動作は未定義です。
    - 値が30,000を超える場合（ページ読み込み時間が30秒を超える場合）、30,000と見なされます。

**精度**

計算は以下の場合に正確です：

- 値の総数が5670を超えない場合。
- 値の総数が5670を超えていても、ページ読み込み時間が1024ms未満の場合。

そうでない場合、計算結果は16msの倍数まで丸められます。

:::note  
ページ読み込み時間の分位数を計算するには、この関数は[quantile](../../../sql-reference/aggregate-functions/reference/quantile.md#quantile)よりも効果的かつ正確です。  
:::

**返される値**

- 指定されたレベルの分位数。

タイプ: `Float32`.

:::note    
関数に値が渡されない場合（`quantileTimingIf`を使用している場合）、[NaN](../../../sql-reference/data-types/float.md#data_type-float-nan-inf)が返されます。これは、ゼロに至る結果と区別することを目的としています。`NaN`値の並べ替えに関する注意事項については、[ORDER BY句](../../../sql-reference/statements/select/order-by.md#select-order-by)を参照してください。  
:::

**例**

入力テーブル：

``` text
┌─response_time─┐
│            72 │
│           112 │
│           126 │
│           145 │
│           104 │
│           242 │
│           313 │
│           168 │
│           108 │
└───────────────┘
```

クエリ：

``` sql
SELECT quantileTiming(response_time) FROM t
```

結果：

``` text
┌─quantileTiming(response_time)─┐
│                           126 │
└───────────────────────────────┘
```

**関連項目**

- [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
- [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
