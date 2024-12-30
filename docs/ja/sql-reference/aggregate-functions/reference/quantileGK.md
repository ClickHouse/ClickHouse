---
slug: /ja/sql-reference/aggregate-functions/reference/quantileGK
sidebar_position: 175
---

# quantileGK

数値データシーケンスの[分位数](https://en.wikipedia.org/wiki/Quantile)を[Greenwald-Khanna](http://infolab.stanford.edu/~datar/courses/cs361a/papers/quantiles.pdf)アルゴリズムを使用して計算します。Greenwald-Khannaアルゴリズムはデータストリーム上で分位数を非常に効率的に計算するためのアルゴリズムです。2001年にMichael GreenwaldとSanjeev Khannaによって発表されました。このアルゴリズムは、リアルタイムで大規模なデータストリーム上で正確な分位数を計算する必要があるデータベースやビッグデータシステムで広く使用されています。アルゴリズムは非常に効率的で、O(log n)の空間とO(log log n)の時間で項目を処理します（ここでnは入力のサイズです）。また非常に正確で、高い確率で近似分位数の値を提供します。

`quantileGK`は、ClickHouseの他の分位数関数とは異なり、ユーザーが近似分位数結果の精度を制御することができます。

**構文**

``` sql
quantileGK(accuracy, level)(expr)
```

エイリアス: `medianGK`.

**引数**

- `accuracy` — 分位数の精度。定数の正の整数。大きな精度値はエラーが少ないことを意味します。例えば、精度引数を100に設定すると、高い確率で計算された分位数のエラーは1%を超えません。計算された分位数の精度とアルゴリズムの計算複雑性との間にはトレードオフがあります。高い精度は、分位数を正確に計算するためにより多くのメモリと計算リソースを必要としますが、低い精度引数は、わずかに精度が低いものの、より高速でメモリエフィシェントな計算を可能にします。

- `level` — 分位数のレベル。オプションのパラメータ。0から1の範囲の定数浮動小数点数。デフォルト値: 0.5。`level=0.5`のとき、関数は[中央値](https://en.wikipedia.org/wiki/Median)を計算します。

- `expr` — 数値[データ型](../../../sql-reference/data-types/index.md#data_types)、[Date](../../../sql-reference/data-types/date.md)または[DateTime](../../../sql-reference/data-types/datetime.md)になるカラム値に対する式です。

**返される値**

- 指定したレベルと精度の分位数。

タイプ:

- 数値データ型入力の場合は[Float64](../../../sql-reference/data-types/float.md)。
- 入力値が`Date`型の場合は[Date](../../../sql-reference/data-types/date.md)。
- 入力値が`DateTime`型の場合は[DateTime](../../../sql-reference/data-types/datetime.md)。

**例**

``` sql
SELECT quantileGK(1, 0.25)(number + 1)
FROM numbers(1000)

┌─quantileGK(1, 0.25)(plus(number, 1))─┐
│                                    1 │
└──────────────────────────────────────┘

SELECT quantileGK(10, 0.25)(number + 1)
FROM numbers(1000)

┌─quantileGK(10, 0.25)(plus(number, 1))─┐
│                                   156 │
└───────────────────────────────────────┘

SELECT quantileGK(100, 0.25)(number + 1)
FROM numbers(1000)

┌─quantileGK(100, 0.25)(plus(number, 1))─┐
│                                    251 │
└────────────────────────────────────────┘

SELECT quantileGK(1000, 0.25)(number + 1)
FROM numbers(1000)

┌─quantileGK(1000, 0.25)(plus(number, 1))─┐
│                                     249 │
└─────────────────────────────────────────┘
```

**参照**

- [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
- [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
