---
slug: /ja/sql-reference/aggregate-functions/reference/quantiledeterministic
sidebar_position: 172
---

# quantileDeterministic

数値データシーケンスの近似的な[分位数](https://en.wikipedia.org/wiki/Quantile)を計算します。

この関数は、最大8192のリザーバサイズを持つ[リザーバサンプリング](https://en.wikipedia.org/wiki/Reservoir_sampling)とサンプリングの決定的なアルゴリズムを適用します。結果は決定論的です。正確な分位数を得るには、[quantileExact](../../../sql-reference/aggregate-functions/reference/quantileexact.md#quantileexact)関数を使用してください。

クエリで異なるレベルの複数の`quantile*`関数を使用する場合、内部状態は結合されません（つまり、クエリは効率的に動作しません）。この場合、[quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)関数を使用してください。

**構文**

``` sql
quantileDeterministic(level)(expr, determinator)
```

エイリアス: `medianDeterministic`.

**引数**

- `level` — 分位数のレベル。オプションのパラメータ。0から1までの定数浮動小数点数。`level`値は `[0.01, 0.99]` の範囲で使用することをお勧めします。デフォルト値: 0.5。`level=0.5`では、関数は[中央値](https://en.wikipedia.org/wiki/Median)を計算します。
- `expr` — 数値[データ型](../../../sql-reference/data-types/index.md#data_types)、[Date](../../../sql-reference/data-types/date.md)または[DateTime](../../../sql-reference/data-types/datetime.md)のカラム値に基づく式。
- `determinator` — リザーバサンプリングアルゴリズムでランダム数生成器の代わりに用いるハッシュが使用される数値。サンプリング結果を決定論的にするために利用されます。任意の決定論的な正の数値、例えばユーザーIDやイベントIDを使用できます。同じ`determinator`値が頻繁に出現する場合、関数は正しく動作しません。

**戻り値**

- 指定されたレベルの近似的な分位数。

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
SELECT quantileDeterministic(val, 1) FROM t
```

結果:

``` text
┌─quantileDeterministic(val, 1)─┐
│                           1.5 │
└───────────────────────────────┘
```

**関連項目**

- [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
- [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
