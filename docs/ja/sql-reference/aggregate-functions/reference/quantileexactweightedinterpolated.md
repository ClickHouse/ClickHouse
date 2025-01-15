---
slug: /ja/sql-reference/aggregate-functions/reference/quantileExactWeightedInterpolated
sidebar_position: 176
---

# quantileExactWeightedInterpolated

数値データシーケンスの[quantile](https://en.wikipedia.org/wiki/Quantile)を線形補間を用いて計算し、各要素の重みを考慮します。

補間値を得るために、渡されたすべての値が配列に結合され、それらの対応する重みによってソートされます。その後、[加重パーセンタイル法](https://en.wikipedia.org/wiki/Percentile#The_weighted_percentile_method)を使用して重みに基づく累積分布を構築し、重みと値を用いた線形補間を行い、パーセンタイルを計算します。

異なる水準の複数の`quantile*`関数をクエリ内で使用する場合、内部状態は結合されません（つまり、クエリは可能な限り効率的には動作しません）。この場合、[quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)関数を使用してください。

`quantileInterpolatedWeighted`よりも`quantileExactWeightedInterpolated`の方が正確なので、`quantileExactWeightedInterpolated`を使用することを強くお勧めします。以下に例を示します：

``` sql
SELECT
    quantileExactWeightedInterpolated(0.99)(number, 1),
    quantile(0.99)(number),
    quantileInterpolatedWeighted(0.99)(number, 1)
FROM numbers(9)


┌─quantileExactWeightedInterpolated(0.99)(number, 1)─┬─quantile(0.99)(number)─┬─quantileInterpolatedWeighted(0.99)(number, 1)─┐
│                                               7.92 │                   7.92 │                                             8 │
└────────────────────────────────────────────────────┴────────────────────────┴───────────────────────────────────────────────┘
```

**構文**

``` sql
quantileExactWeightedInterpolated(level)(expr, weight)
```

エイリアス: `medianExactWeightedInterpolated`.

**引数**

- `level` — パーセンタイルのレベル。オプションのパラメータ。0から1の間の定数浮動小数点数。`level`の値には `[0.01, 0.99]` の範囲を使用することをお勧めします。デフォルト値：0.5。`level=0.5` の場合、この関数は[中央値](https://en.wikipedia.org/wiki/Median)を計算します。
- `expr` — 数値[データ型](../../../sql-reference/data-types/index.md#data_types)、[Date](../../../sql-reference/data-types/date.md)、または[DateTime](../../../sql-reference/data-types/datetime.md)に帰結するカラム値の式。
- `weight` — シーケンスメンバーの重みを持つカラム。重みは[Unsigned integer型](../../../sql-reference/data-types/int-uint.md)の値の出現回数です。

**戻り値**

- 指定されたレベルのパーセンタイル。

型:

- 数値データ型入力の場合は[Float64](../../../sql-reference/data-types/float.md)。
- 入力値が`Date`型の場合は[Date](../../../sql-reference/data-types/date.md)。
- 入力値が`DateTime`型の場合は[DateTime](../../../sql-reference/data-types/datetime.md)。

**例**

入力テーブル：

``` text
┌─n─┬─val─┐
│ 0 │   3 │
│ 1 │   2 │
│ 2 │   1 │
│ 5 │   4 │
└───┴─────┘
```

結果：

``` text
┌─quantileExactWeightedInterpolated(n, val)─┐
│                                       1.5 │
└───────────────────────────────────────────┘
```

**関連項目**

- [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
- [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
