---
slug: /ja/sql-reference/aggregate-functions/reference/quantileInterpolatedWeighted
sidebar_position: 176
---

# quantileInterpolatedWeighted

数値データシーケンスの[分位](https://en.wikipedia.org/wiki/Quantile)を、各要素の重みを考慮して線形補間を用いて計算します。

補間された値を得るためには、渡されたすべての値を配列に結合し、それらを対応する重みによってソートします。その後、重みに基づく累積分布を構築し、[重み付きパーセンタイル法](https://en.wikipedia.org/wiki/Percentile#The_weighted_percentile_method)を使用して分位の線形補間を行います。

**複数の `quantile*` 関数を異なるレベルでクエリ内で使用する場合、内部状態は結合されません（つまり、クエリは効率的に動作しません）。この場合、[quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles) 関数を使用してください。**

**構文**

``` sql
quantileInterpolatedWeighted(level)(expr, weight)
```

エイリアス: `medianInterpolatedWeighted`.

**引数**

- `level` — 分位のレベル。オプションのパラメータ。0から1の範囲での定数浮動小数点数。`level` の値は `[0.01, 0.99]` の範囲で使用することをお勧めします。デフォルト値: 0.5。`level=0.5` の場合、関数は[中央値](https://en.wikipedia.org/wiki/Median)を計算します。
- `expr` — カラム値に対する式で、数値[データ型](../../../sql-reference/data-types/index.md#data_types)、[Date](../../../sql-reference/data-types/date.md) または [DateTime](../../../sql-reference/data-types/datetime.md)を結果とします。
- `weight` — シーケンスメンバーの重みを示すカラム。重みは値の出現回数を表します。

**返される値**

- 指定されたレベルの分位。

型:

- 数値データ型入力に対しては [Float64](../../../sql-reference/data-types/float.md)。
- 入力値が `Date` 型の場合は [Date](../../../sql-reference/data-types/date.md)。
- 入力値が `DateTime` 型の場合は [DateTime](../../../sql-reference/data-types/datetime.md)。

**例**

入力テーブル:

``` text
┌─n─┬─val─┐
│ 0 │   3 │
│ 1 │   2 │
│ 2 │   1 │
│ 5 │   4 │
└───┴─────┘
```

クエリ:

``` sql
SELECT quantileInterpolatedWeighted(n, val) FROM t
```

結果:

``` text
┌─quantileInterpolatedWeighted(n, val)─┐
│                                    1 │
└──────────────────────────────────────┘
```

**関連項目**

- [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
- [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
