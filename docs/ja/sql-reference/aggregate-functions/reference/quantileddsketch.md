---
slug: /ja/sql-reference/aggregate-functions/reference/quantileddsketch
sidebar_position: 171
title: quantileDD
---

サンプルの近似[分位数](https://en.wikipedia.org/wiki/Quantile)を相対誤差保証付きで計算します。これは[DD](https://www.vldb.org/pvldb/vol12/p2195-masson.pdf)を構築することで動作します。

**構文**

``` sql
quantileDD(relative_accuracy, [level])(expr)
```

**引数**

- `expr` — 数値データを含むカラム。 [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md).

**パラメータ**

- `relative_accuracy` — 分位数の相対精度。可能な値は0から1の範囲です。[Float](../../../sql-reference/data-types/float.md)。スケッチのサイズはデータの範囲と相対精度に依存します。範囲が大きく相対精度が小さいほど、スケッチは大きくなります。スケッチの大まかなメモリサイズは `log(max_value/min_value)/relative_accuracy` です。推奨値は0.001以上です。

- `level` — 分位数のレベル。省略可能。可能な値は0から1の範囲です。デフォルト値：0.5。[Float](../../../sql-reference/data-types/float.md).

**返される値**

- 指定されたレベルの近似分位数。

型: [Float64](../../../sql-reference/data-types/float.md#float32-float64).

**例**

入力テーブルは整数カラムと浮動小数点カラムを持ちます:

``` text
┌─a─┬─────b─┐
│ 1 │ 1.001 │
│ 2 │ 1.002 │
│ 3 │ 1.003 │
│ 4 │ 1.004 │
└───┴───────┘
```

0.75-分位数（第3四分位）を計算するクエリ:

``` sql
SELECT quantileDD(0.01, 0.75)(a), quantileDD(0.01, 0.75)(b) FROM example_table;
```

結果:

``` text
┌─quantileDD(0.01, 0.75)(a)─┬─quantileDD(0.01, 0.75)(b)─┐
│               2.974233423476717 │                            1.01 │
└─────────────────────────────────┴─────────────────────────────────┘
```

**関連項目**

- [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
- [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
