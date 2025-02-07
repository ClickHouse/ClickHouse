---
slug: /ja/sql-reference/aggregate-functions/reference/grouparraymovingsum
sidebar_position: 144
---

# groupArrayMovingSum

入力値の移動合計を計算します。

``` sql
groupArrayMovingSum(numbers_for_summing)
groupArrayMovingSum(window_size)(numbers_for_summing)
```

この関数はウィンドウサイズをパラメータとして取ることができます。指定しない場合、関数はカラムの行数と同じウィンドウサイズを取ります。

**引数**

- `numbers_for_summing` — 数値データ型の値を生成する[式](../../../sql-reference/syntax.md#syntax-expressions)。
- `window_size` — 計算ウィンドウのサイズ。

**返される値**

- 入力データと同じサイズと型の配列。

**例**

サンプルテーブル:

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
    groupArrayMovingSum(int) AS I,
    groupArrayMovingSum(float) AS F,
    groupArrayMovingSum(dec) AS D
FROM t
```

``` text
┌─I──────────┬─F───────────────────────────────┬─D──────────────────────┐
│ [1,3,7,14] │ [1.1,3.3000002,7.7000003,15.47] │ [1.10,3.30,7.70,15.47] │
└────────────┴─────────────────────────────────┴────────────────────────┘
```

``` sql
SELECT
    groupArrayMovingSum(2)(int) AS I,
    groupArrayMovingSum(2)(float) AS F,
    groupArrayMovingSum(2)(dec) AS D
FROM t
```

``` text
┌─I──────────┬─F───────────────────────────────┬─D──────────────────────┐
│ [1,3,6,11] │ [1.1,3.3000002,6.6000004,12.17] │ [1.10,3.30,6.60,12.17] │
└────────────┴─────────────────────────────────┴────────────────────────┘
```
