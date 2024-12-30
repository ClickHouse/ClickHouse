---
slug: /ja/sql-reference/aggregate-functions/reference/quantilebfloat16
sidebar_position: 171
title: quantileBFloat16
---

[bfloat16](https://en.wikipedia.org/wiki/Bfloat16_floating-point_format) 数値のサンプルから近似的な[分位数](https://en.wikipedia.org/wiki/Quantile)を計算します。`bfloat16` は、1ビットの符号ビット、8ビットの指数ビット、および7ビットの仮数ビットを持つ浮動小数点データ型です。関数は入力値を32ビット浮動小数点に変換し、最上位の16ビットを取り出します。次に `bfloat16` の分位数を計算し、結果を64ビットの浮動小数点にゼロビットを追加して変換します。関数は相対誤差が0.390625%以下の高速な分位数推定器です。

**構文**

``` sql
quantileBFloat16[(level)](expr)
```

エイリアス: `medianBFloat16`

**引数**

- `expr` — 数値データを持つカラム。[整数](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md)。

**パラメータ**

- `level` — 分位数のレベル。任意項目。可能な値は0から1までの範囲です。デフォルト値: 0.5。 [Float](../../../sql-reference/data-types/float.md)。

**返される値**

- 指定されたレベルの近似的な分位数。

タイプ: [Float64](../../../sql-reference/data-types/float.md#float32-float64)。

**例**

入力テーブルには整数カラムと浮動小数点カラムがあります:

``` text
┌─a─┬─────b─┐
│ 1 │ 1.001 │
│ 2 │ 1.002 │
│ 3 │ 1.003 │
│ 4 │ 1.004 │
└───┴───────┘
```

0.75分位数（第3四分位）を計算するクエリ:

``` sql
SELECT quantileBFloat16(0.75)(a), quantileBFloat16(0.75)(b) FROM example_table;
```

結果:

``` text
┌─quantileBFloat16(0.75)(a)─┬─quantileBFloat16(0.75)(b)─┐
│                         3 │                         1 │
└───────────────────────────┴───────────────────────────┘
```
例では、浮動小数点値がすべて `bfloat16` に変換されるときに1.0に切り捨てられていることに注意してください。

# quantileBFloat16Weighted

`quantileBFloat16` に似ていますが、シーケンスメンバーの重みを考慮します。

**関連項目**

- [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
- [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
