---
slug: /ja/sql-reference/aggregate-functions/reference/grouparraymovingavg
sidebar_position: 143
---

# groupArrayMovingAvg

入力値の移動平均を計算します。

``` sql
groupArrayMovingAvg(numbers_for_summing)
groupArrayMovingAvg(window_size)(numbers_for_summing)
```

この関数はウィンドウサイズをパラメーターとして取ることができます。指定しない場合、関数はカラム内の行数と同じサイズのウィンドウを取ります。

**引数**

- `numbers_for_summing` — 数値データ型の値を結果とする[式](../../../sql-reference/syntax.md#syntax-expressions)。
- `window_size` — 計算ウィンドウのサイズ。

**返される値**

- 入力データと同じサイズと型の配列。

この関数は[ゼロに向かう丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用します。結果のデータ型にとって重要でない小数点以下の桁数を切り捨てます。

**例**

サンプルテーブル `b`:

``` sql
CREATE TABLE t
(
    `int` UInt8,
    `float` Float32,
    `dec` Decimal32(2)
)
ENGINE = TinyLog
```

``` text
┌─int─┬─float─┬──dec─┐
│   1 │   1.1 │ 1.10 │
│   2 │   2.2 │ 2.20 │
│   4 │   4.4 │ 4.40 │
│   7 │  7.77 │ 7.77 │
└─────┴───────┴──────┘
```

クエリ:

``` sql
SELECT
    groupArrayMovingAvg(int) AS I,
    groupArrayMovingAvg(float) AS F,
    groupArrayMovingAvg(dec) AS D
FROM t
```

``` text
┌─I─────────┬─F───────────────────────────────────┬─D─────────────────────┐
│ [0,0,1,3] │ [0.275,0.82500005,1.9250001,3.8675] │ [0.27,0.82,1.92,3.86] │
└───────────┴─────────────────────────────────────┴───────────────────────┘
```

``` sql
SELECT
    groupArrayMovingAvg(2)(int) AS I,
    groupArrayMovingAvg(2)(float) AS F,
    groupArrayMovingAvg(2)(dec) AS D
FROM t
```

``` text
┌─I─────────┬─F────────────────────────────────┬─D─────────────────────┐
│ [0,1,3,5] │ [0.55,1.6500001,3.3000002,6.085] │ [0.55,1.65,3.30,6.08] │
└───────────┴──────────────────────────────────┴───────────────────────┘
```
