---
slug: /ja/sql-reference/aggregate-functions/reference/quantile
sidebar_position: 170
---

# quantile

数値データ列の近似[分位数](https://en.wikipedia.org/wiki/Quantile)を計算します。

この関数は、最大8192のリザーバーサイズを持つ[リザーバーサンプリング](https://en.wikipedia.org/wiki/Reservoir_sampling)とサンプリング用の乱数生成器を適用します。結果は非決定的です。正確な分位数を取得するには、[quantileExact](../../../sql-reference/aggregate-functions/reference/quantileexact.md#quantileexact)関数を使用してください。

`クエリ`で異なるレベルの複数の`quantile*`関数を使用する場合、内部状態は結合されません（つまり、`クエリ`は本来可能な効率で動作しません）。この場合は、[quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)関数を使用してください。

空の数値列の場合、`quantile`はNaNを返しますが、`quantile*`のバリエーションはNaNまたは列型のデフォルト値を返します（バリエーションによります）。

**構文**

``` sql
quantile(level)(expr)
```

別名: `median`.

**引数**

- `level` — 分位数のレベル。オプションのパラメータ。0から1までの定数の浮動小数点数。`level`値として`[0.01, 0.99]`の範囲を使用することをお勧めします。デフォルト値: 0.5。`level=0.5`の場合、関数は[中央値](https://en.wikipedia.org/wiki/Median)を計算します。
- `expr` — 数値型、[Date](../../../sql-reference/data-types/date.md)または[DateTime](../../../sql-reference/data-types/datetime.md)の結果となるカラム値への式。

**返される値**

- 指定されたレベルの近似分位数。

型:

- 数値データ型入力の場合は[Float64](../../../sql-reference/data-types/float.md)。
- 入力値が`Date`型の場合は[Date](../../../sql-reference/data-types/date.md)。
- 入力値が`DateTime`型の場合は[DateTime](../../../sql-reference/data-types/datetime.md)。

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
SELECT quantile(val) FROM t
```

結果:

``` text
┌─quantile(val)─┐
│           1.5 │
└───────────────┘
```

**参照**

- [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
- [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
