---
slug: /ja/sql-reference/aggregate-functions/reference/cramersvbiascorrected
sidebar_position: 128
---

# cramersVBiasCorrected

Cramer's Vは、テーブル内の二つのカラム間の関連性を測定する指標です。[`cramersV`関数](./cramersv.md)の結果は0（変数間に関連性がないことを示す）から1（各値が他方の値によって完全に決定される場合）までの範囲を持ちます。この関数は大きなバイアスを持つ可能性があるため、このバージョンのCramer's Vでは[バイアス補正](https://en.wikipedia.org/wiki/Cram%C3%A9r%27s_V#Bias_correction)を使用します。

**構文**

``` sql
cramersVBiasCorrected(column1, column2)
```

**パラメータ**

- `column1`: 比較する最初のカラム。
- `column2`: 比較する2番目のカラム。

**返される値**

- 0（カラムの値間に関連性がないことを示す）から1（完全な関連性）までの値。

型:  常に[Float64](../../../sql-reference/data-types/float.md)。

**例**

以下の二つのカラムは互いに小さな関連性を持っています。`cramersVBiasCorrected`の結果は`cramersV`の結果よりも小さいことに注目してください:

クエリ:

``` sql
SELECT
    cramersV(a, b),
    cramersVBiasCorrected(a ,b)
FROM
    (
        SELECT
            number % 10 AS a,
            number % 4 AS b
        FROM
            numbers(150)
    );
```

結果:

```response
┌──────cramersV(a, b)─┬─cramersVBiasCorrected(a, b)─┐
│ 0.41171788506213564 │         0.33369281784141364 │
└─────────────────────┴─────────────────────────────┘
```
