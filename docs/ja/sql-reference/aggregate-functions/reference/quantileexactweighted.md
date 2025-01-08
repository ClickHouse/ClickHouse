---
slug: /ja/sql-reference/aggregate-functions/reference/quantileexactweighted
sidebar_position: 174
---

# quantileExactWeighted

数値データシーケンスの[分位数](https://en.wikipedia.org/wiki/Quantile)を各要素の重みを考慮して正確に計算します。

正確な値を取得するために、渡されたすべての値を配列に結合し、部分的にソートされます。各値はその重みとともにカウントされ、まるで `weight` 回出現するかのように扱われます。アルゴリズムではハッシュテーブルが使用されます。このため、渡された値が頻繁に繰り返される場合、関数は [quantileExact](../../../sql-reference/aggregate-functions/reference/quantileexact.md#quantileexact) よりもRAMを少なく消費します。この関数を `quantileExact` の代わりに使用し、重みを1に指定することができます。

クエリ内で異なるレベルの複数の `quantile*` 関数を使用する場合、内部状態は結合されません（つまり、クエリは可能な限り効率的には動作しません）。その場合、[quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles) 関数を使用してください。

**構文**

``` sql
quantileExactWeighted(level)(expr, weight)
```

別名: `medianExactWeighted`.

**引数**

- `level` — 分位数のレベル。オプションのパラメータ。0から1までの定数浮動小数点数。`level` 値は `[0.01, 0.99]` の範囲で使用することを推奨します。デフォルト値: 0.5。`level=0.5` の場合、関数は[中央値](https://en.wikipedia.org/wiki/Median)を計算します。
- `expr` — 数値[データ型](../../../sql-reference/data-types/index.md#data_types)、[日付](../../../sql-reference/data-types/date.md)または[日時](../../../sql-reference/data-types/datetime.md)に結果を持つカラム値に対する式。
- `weight` — シーケンスメンバーの重みを持つカラム。重みは [非負整数型](../../../sql-reference/data-types/int-uint.md)として値の出現回数です。

**返される値**

- 指定されたレベルの分位数。

型:

- 数値データ型入力の場合は [Float64](../../../sql-reference/data-types/float.md)。
- 入力が `Date` 型の場合は [日付](../../../sql-reference/data-types/date.md)。
- 入力が `DateTime` 型の場合は [日時](../../../sql-reference/data-types/datetime.md)。

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
SELECT quantileExactWeighted(n, val) FROM t
```

結果:

``` text
┌─quantileExactWeighted(n, val)─┐
│                             1 │
└───────────────────────────────┘
```

**関連項目**

- [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
- [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
