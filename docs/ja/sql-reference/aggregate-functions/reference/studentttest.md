---
slug: /ja/sql-reference/aggregate-functions/reference/studentttest
sidebar_position: 194
sidebar_label: studentTTest
---

# studentTTest

2つの母集団のサンプルに対してスチューデントのt検定を適用します。

**構文**

``` sql
studentTTest([confidence_level])(sample_data, sample_index)
```

両サンプルの値は`sample_data`カラムにあります。`sample_index`が0の場合、その行の値は最初の母集団からのサンプルに属します。それ以外の場合は2番目の母集団からのサンプルに属します。帰無仮説は母集団の平均が等しいとしています。正規分布で等しい分散を仮定します。

**引数**

- `sample_data` — サンプルデータ。[Integer](../../../sql-reference/data-types/int-uint.md)、[Float](../../../sql-reference/data-types/float.md)または[Decimal](../../../sql-reference/data-types/decimal.md)。
- `sample_index` — サンプルインデックス。[Integer](../../../sql-reference/data-types/int-uint.md)。

**パラメータ**

- `confidence_level` — 信頼区間を計算するための信頼水準。[Float](../../../sql-reference/data-types/float.md)。


**返される値**

2つまたは4つの要素を持つ[Tuple](../../../sql-reference/data-types/tuple.md)（オプションの`confidence_level`が指定されている場合）:

- 計算されたt統計量。[Float64](../../../sql-reference/data-types/float.md)。
- 計算されたp値。[Float64](../../../sql-reference/data-types/float.md)。
- [計算された信頼区間の下側。[Float64](../../../sql-reference/data-types/float.md)。]
- [計算された信頼区間の上側。[Float64](../../../sql-reference/data-types/float.md)。]


**例**

入力テーブル:

``` text
┌─sample_data─┬─sample_index─┐
│        20.3 │            0 │
│        21.1 │            0 │
│        21.9 │            1 │
│        21.7 │            0 │
│        19.9 │            1 │
│        21.8 │            1 │
└─────────────┴──────────────┘
```

クエリ:

``` sql
SELECT studentTTest(sample_data, sample_index) FROM student_ttest;
```

結果:

``` text
┌─studentTTest(sample_data, sample_index)───┐
│ (-0.21739130434783777,0.8385421208415731) │
└───────────────────────────────────────────┘
```

**関連項目**

- [スチューデントのt検定](https://en.wikipedia.org/wiki/Student%27s_t-test)
- [welchTTest関数](welchttest.md#welchttest)
