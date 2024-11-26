---
slug: /ja/sql-reference/aggregate-functions/reference/kolmogorovsmirnovtest
sidebar_position: 156
sidebar_label: kolmogorovSmirnovTest
---

# kolmogorovSmirnovTest

2つの母集団からのサンプルにコルモゴロフ-スミルノフ検定を適用します。

**構文**

``` sql
kolmogorovSmirnovTest([alternative, computation_method])(sample_data, sample_index)
```

両方のサンプルの値は `sample_data` カラムにあります。`sample_index` が0の場合、その行の値は最初の母集団からのサンプルに属します。それ以外の場合は、2番目の母集団からのサンプルに属します。
サンプルは連続した一次元の確率分布に属している必要があります。

**引数**

- `sample_data` — サンプルデータ。 [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) または [Decimal](../../../sql-reference/data-types/decimal.md)。
- `sample_index` — サンプルインデックス。 [Integer](../../../sql-reference/data-types/int-uint.md)。

**パラメータ**

- `alternative` — 代替仮説。（オプション、デフォルト: `'two-sided'`。）[String](../../../sql-reference/data-types/string.md)。
    F(x) と G(x) をそれぞれ最初と2番目の分布のCDFとします。
    - `'two-sided'`
        帰無仮説はサンプルが同じ分布から来ていることで、つまり F(x) = G(x) がすべての x において成り立つことです。
        代替仮説は分布が同一でないということです。
    - `'greater'`
        帰無仮説は、最初のサンプル内の値が2番目のサンプルのものより*確率的に小さい*ということで、
        つまり最初の分布のCDFが2番目のものの上側（よって左側）にあるということです。
        これにより、すべての x において F(x) >= G(x) となります。この場合の代替は F(x) < G(x) の x が少なくとも1つ存在することです。
    - `'less'`。
        帰無仮説は、最初のサンプル内の値が2番目のサンプルのものより*確率的に大きい*ということで、
        つまり最初の分布のCDFが2番目のものの下側（よって右側）にあるということです。
        これにより、すべての x において F(x) <= G(x) となります。この場合の代替は F(x) > G(x) の x が少なくとも1つ存在することです。
- `computation_method` — p値を計算するために使用される方法。（オプション、デフォルト: `'auto'`。）[String](../../../sql-reference/data-types/string.md)。
    - `'exact'` - 検定統計量の正確な確率分布を使用して計算されます。計算量が多く、少量のサンプル以外では非効率です。
    - `'asymp'`（`'asymptotic'`） - 近似によって計算されます。大サンプルサイズの場合、正確p値と漸近p値は非常に似ています。
    - `'auto'` - サンプルの最大数が10,000未満の場合に `'exact'` メソッドを使用します。

**返される値**

2つの要素を持つ[Tuple](../../../sql-reference/data-types/tuple.md):

- 計算された統計量。 [Float64](../../../sql-reference/data-types/float.md)。
- 計算されたp値。 [Float64](../../../sql-reference/data-types/float.md)。

**例**

クエリ:

``` sql
SELECT kolmogorovSmirnovTest('less', 'exact')(value, num)
FROM
(
    SELECT
        randNormal(0, 10) AS value,
        0 AS num
    FROM numbers(10000)
    UNION ALL
    SELECT
        randNormal(0, 10) AS value,
        1 AS num
    FROM numbers(10000)
)
```

結果:

``` text
┌─kolmogorovSmirnovTest('less', 'exact')(value, num)─┐
│ (0.009899999999999996,0.37528595205132287)         │
└────────────────────────────────────────────────────┘
```

注意:
p値が0.05より大きい（信頼レベル95%の場合）ため、帰無仮説は棄却されません。

クエリ:

``` sql
SELECT kolmogorovSmirnovTest('two-sided', 'exact')(value, num)
FROM
(
    SELECT
        randStudentT(10) AS value,
        0 AS num
    FROM numbers(100)
    UNION ALL
    SELECT
        randNormal(0, 10) AS value,
        1 AS num
    FROM numbers(100)
)
```

結果:

``` text
┌─kolmogorovSmirnovTest('two-sided', 'exact')(value, num)─┐
│ (0.4100000000000002,6.61735760482795e-8)                │
└─────────────────────────────────────────────────────────┘
```

注意:
p値が0.05より小さい（信頼レベル95%の場合）ため、帰無仮説は棄却されます。

**参照**

- [Kolmogorov-Smirnov'test](https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test)
