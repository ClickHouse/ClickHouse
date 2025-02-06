---
slug: /ja/sql-reference/functions/random-functions
sidebar_position: 145
sidebar_label: ランダム数
---

# ランダム数生成関数

このセクションのすべての関数は、0または1つの引数を受け付けます。引数が提供された場合、その唯一の使用目的は、同じランダム関数の行内での異なる実行に異なるランダム値を返すように、[共通部分式の除去](../../sql-reference/functions/index.md#common-subexpression-elimination)を防ぐことです。

関連コンテンツ

- ブログ: [ClickHouseでのランダムデータ生成](https://clickhouse.com/blog/generating-random-test-distribution-data-for-clickhouse)

:::note
ランダム数は暗号化を目的としないアルゴリズムによって生成されます。
:::

## rand

均等な分布でランダムなUInt32の数値を返します。

線形合同法生成器を使用し、システムから取得した初期状態を用います。これは、ランダムに見えますが、実際には初期状態が知られている場合には予測可能であることを意味します。真にランダムであることが重要なシナリオでは、システムレベルの呼び出しや外部ライブラリとの統合などの代替手法を検討してください。

**構文**

```sql
rand()
```

別名: `rand32`

**引数**

なし。

**戻り値**

UInt32型の数値を返します。

**例**

```sql
SELECT rand();
```

```response
1569354847 -- 注意: 実際の出力はこの例に示された特定の数値ではなく、ランダムな数値になります
```

## rand64

ランダムなUInt64の整数（UInt64）を返します。

**構文**

```sql
rand64()
```

**引数**

なし。

**戻り値**

均等な分布でのランダムなUInt64の数値。

線形合同法生成器を使用し、システムから取得した初期状態を用います。これは、ランダムに見えますが、実際には初期状態が知られている場合には予測可能であることを意味します。真にランダムであることが重要なシナリオでは、システムレベルの呼び出しや外部ライブラリとの統合などの代替手法を検討してください。

**例**

```sql
SELECT rand64();
```

```response
15030268859237645412 -- 注意: 実際の出力はこの例に示された特定の数値ではなく、ランダムな数値になります。
```

## randCanonical

ランダムなFloat64の数値を返します。

**構文**

```sql
randCanonical()
```

**引数**

なし。

**戻り値**

0（含む）と1（含まない）との間のFloat64の値を返します。

**例**

```sql
SELECT randCanonical();
```

```response
0.3452178901234567 - 注意: 実際の出力はこの例に示された特定の数値ではなく、0と1の間のランダムなFloat64の数値になります。
```

## randConstant

単一のランダムな値で満たされた一定のカラムを生成します。`rand`と異なり、この関数は生成されたカラムの各行に同じランダム値を表示します。単一のクエリ内の行に対して一貫したランダムなシードが必要なシナリオで有用です。

**構文**

```sql
randConstant([x]);
```

**引数**

- **[x] (オプション):** 生成されたランダム値に影響を与えるオプションの式です。与えられても、結果の値は同じクエリ実行内で一定のままです。同じ式を使用する異なるクエリでは、異なる一定値が生成される可能性があります。

**戻り値**

各行に同じランダム値を含むUInt32型のカラムを返します。

**実装の詳細**

オプションの式が同じ場合でも、実際の出力はクエリの実行ごとに異なります。このオプションのパラメーターは、`randConstant`を単独で使用するのと比較して生成された値に大きな変更を加えるわけではありません。

**例**

```sql
SELECT randConstant() AS random_value;
```

```response
| random_value |
|--------------|
| 1234567890   |
```

```sql
SELECT randConstant(10) AS random_value;
```

```response
| random_value |
|--------------|
| 9876543210   |
```

## randUniform

[`min`, `max`]の範囲から均等に引かれたランダムなFloat64を返します。

**構文**

```sql
randUniform(min, max)
```

**引数**

- `min` - `Float64` - 範囲の左境界,
- `max` - `Float64` - 範囲の右境界.

**戻り値**

[Float64](../data-types/float.md)型のランダムな数値。

**例**

```sql
SELECT randUniform(5.5, 10) FROM numbers(5)
```

```response
┌─randUniform(5.5, 10)─┐
│    8.094978491443102 │
│   7.3181248914450885 │
│    7.177741903868262 │
│    6.483347380953762 │
│    6.122286382885112 │
└──────────────────────┘
```

## randNormal

[正規分布](https://en.wikipedia.org/wiki/Normal_distribution)から引かれたランダムなFloat64を返します。

**構文**

```sql
randNormal(mean, variance)
```

**引数**

- `mean` - `Float64` - 分布の平均,
- `variance` - `Float64` - 分布の[分散](https://en.wikipedia.org/wiki/Variance)。

**戻り値**

- ランダム数。[Float64](../data-types/float.md)。

**例**

```sql
SELECT randNormal(10, 2) FROM numbers(5)
```

結果:

```result
┌──randNormal(10, 2)─┐
│ 13.389228911709653 │
│  8.622949707401295 │
│ 10.801887062682981 │
│ 4.5220192605895315 │
│ 10.901239123982567 │
└────────────────────┘
```

## randLogNormal

[対数正規分布](https://en.wikipedia.org/wiki/Log-normal_distribution)から引かれたランダムなFloat64を返します。

**構文**

```sql
randLogNormal(mean, variance)
```

**引数**

- `mean` - `Float64` - 分布の平均,
- `variance` - `Float64` - 分布の[分散](https://en.wikipedia.org/wiki/Variance)。

**戻り値**

- ランダム数。[Float64](../data-types/float.md)。

**例**

```sql
SELECT randLogNormal(100, 5) FROM numbers(5)
```

結果:

```result
┌─randLogNormal(100, 5)─┐
│  1.295699673937363e48 │
│  9.719869109186684e39 │
│  6.110868203189557e42 │
│  9.912675872925529e39 │
│ 2.3564708490552458e42 │
└───────────────────────┘
```

## randBinomial

[二項分布](https://en.wikipedia.org/wiki/Binomial_distribution)から引かれたランダムなUInt64を返します。

**構文**

```sql
randBinomial(experiments, probability)
```

**引数**

- `experiments` - `UInt64` - 実験の回数,
- `probability` - `Float64` - 各実験における成功の確率, 0から1の間の値。

**戻り値**

- ランダム数。[UInt64](../data-types/int-uint.md)。

**例**

```sql
SELECT randBinomial(100, .75) FROM numbers(5)
```

結果:

```result
┌─randBinomial(100, 0.75)─┐
│                      74 │
│                      78 │
│                      76 │
│                      77 │
│                      80 │
└─────────────────────────┘
```

## randNegativeBinomial

[負の二項分布](https://en.wikipedia.org/wiki/Negative_binomial_distribution)から引かれたランダムなUInt64を返します。

**構文**

```sql
randNegativeBinomial(experiments, probability)
```

**引数**

- `experiments` - `UInt64` - 実験の回数,
- `probability` - `Float64` - 各実験における失敗の確率, 0から1の間の値。

**戻り値**

- ランダム数。[UInt64](../data-types/int-uint.md)。

**例**

```sql
SELECT randNegativeBinomial(100, .75) FROM numbers(5)
```

結果:

```result
┌─randNegativeBinomial(100, 0.75)─┐
│                              33 │
│                              32 │
│                              39 │
│                              40 │
│                              50 │
└─────────────────────────────────┘
```

## randPoisson

[ポアソン分布](https://en.wikipedia.org/wiki/Poisson_distribution)から引かれたランダムなUInt64を返します。

**構文**

```sql
randPoisson(n)
```

**引数**

- `n` - `UInt64` - 発生の平均数。

**戻り値**

- ランダム数。[UInt64](../data-types/int-uint.md)。

**例**

```sql
SELECT randPoisson(10) FROM numbers(5)
```

結果:

```result
┌─randPoisson(10)─┐
│               8 │
│               8 │
│               7 │
│              10 │
│               6 │
└─────────────────┘
```

## randBernoulli

[ベルヌーイ分布](https://en.wikipedia.org/wiki/Bernoulli_distribution)から引かれたランダムなUInt64を返します。

**構文**

```sql
randBernoulli(probability)
```

**引数**

- `probability` - `Float64` - 成功の確率、0から1の間の値。

**戻り値**

- ランダム数。[UInt64](../data-types/int-uint.md)。

**例**

```sql
SELECT randBernoulli(.75) FROM numbers(5)
```

結果:

```result
┌─randBernoulli(0.75)─┐
│                   1 │
│                   1 │
│                   0 │
│                   1 │
│                   1 │
└─────────────────────┘
```

## randExponential

[指数分布](https://en.wikipedia.org/wiki/Exponential_distribution)から引かれたランダムなFloat64を返します。

**構文**

```sql
randExponential(lambda)
```

**引数**

- `lambda` - `Float64` - ラムダ値。

**戻り値**

- ランダム数。[Float64](../data-types/float.md)。

**例**

```sql
SELECT randExponential(1/10) FROM numbers(5)
```

結果:

```result
┌─randExponential(divide(1, 10))─┐
│              44.71628934340778 │
│              4.211013337903262 │
│             10.809402553207766 │
│              15.63959406553284 │
│             1.8148392319860158 │
└────────────────────────────────┘
```

## randChiSquared

[カイ二乗分布](https://en.wikipedia.org/wiki/Chi-squared_distribution)から引かれたランダムなFloat64を返します。この分布は、`k`個の独立した標準正規乱数変数の平方和の分布です。

**構文**

```sql
randChiSquared(degree_of_freedom)
```

**引数**

- `degree_of_freedom` - `Float64` - 自由度。

**戻り値**

- ランダム数。[Float64](../data-types/float.md)。

**例**

```sql
SELECT randChiSquared(10) FROM numbers(5)
```

結果:

```result
┌─randChiSquared(10)─┐
│ 10.015463656521543 │
│  9.621799919882768 │
│   2.71785015634699 │
│ 11.128188665931908 │
│  4.902063104425469 │
└────────────────────┘
```

## randStudentT

[スチューデントのt分布](https://en.wikipedia.org/wiki/Student%27s_t-distribution)から引かれたランダムなFloat64を返します。

**構文**

```sql
randStudentT(degree_of_freedom)
```

**引数**

- `degree_of_freedom` - `Float64` - 自由度。

**戻り値**

- ランダム数。[Float64](../data-types/float.md)。

**例**

```sql
SELECT randStudentT(10) FROM numbers(5)
```

結果:

```result
┌─────randStudentT(10)─┐
│   1.2217309938538725 │
│   1.7941971681200541 │
│ -0.28192176076784664 │
│   0.2508897721303792 │
│  -2.7858432909761186 │
└──────────────────────┘
```

## randFisherF

[F分布](https://en.wikipedia.org/wiki/F-distribution)から引かれたランダムなFloat64を返します。

**構文**

```sql
randFisherF(d1, d2)
```

**引数**

- `d1` - `Float64` - `X = (S1 / d1) / (S2 / d2)`におけるd1の自由度,
- `d2` - `Float64` - `X = (S1 / d1) / (S2 / d2)`におけるd2の自由度,

**戻り値**

- ランダム数。[Float64](../data-types/float.md)。

**例**

```sql
SELECT randFisherF(10, 3) FROM numbers(5)
```

結果:

```result
┌──randFisherF(10, 3)─┐
│   7.286287504216609 │
│ 0.26590779413050386 │
│ 0.22207610901168987 │
│  0.7953362728449572 │
│ 0.19278885985221572 │
└─────────────────────┘
```

## randomString {#randomString}

指定された長さのランダムなバイト（ゼロバイトを含む）で満たされた文字列を生成します。すべての文字が印刷可能であるとは限りません。

**構文**

```sql
randomString(length)
```

**引数**

- `length` — 文字列のバイト長。正の整数。

**戻り値**

- ランダムなバイトで満たされた文字列。[String](../data-types/string.md)。

**例**

クエリ:

```sql
SELECT randomString(30) AS str, length(str) AS len FROM numbers(2) FORMAT Vertical;
```

結果:

```text
Row 1:
──────
str: 3 G  :   pT ?w тi  k aV f6
len: 30

Row 2:
──────
str: 9 ,]    ^   )  ]??  8
len: 30
```

## randomFixedString

指定された長さのランダムなバイト（ゼロバイトを含む）で満たされたバイナリ文字列を生成します。すべての文字が印刷可能であるとは限りません。

**構文**

```sql
randomFixedString(length);
```

**引数**

- `length` — 文字列のバイト長。[UInt64](../data-types/int-uint.md)。

**戻り値**

- ランダムなバイトで満たされた文字列。[FixedString](../data-types/fixedstring.md)。

**例**

クエリ:

```sql
SELECT randomFixedString(13) as rnd, toTypeName(rnd)
```

結果:

```text
┌─rnd──────┬─toTypeName(randomFixedString(13))─┐
│ j▒h㋖HɨZ'▒ │ FixedString(13)                 │
└──────────┴───────────────────────────────────┘
```

## randomPrintableASCII

ランダムな[ASCII](https://en.wikipedia.org/wiki/ASCII#Printable_characters)文字の集合を持つ文字列を生成します。すべての文字が印刷可能です。
`length < 0`を渡すと、この関数の動作は未定義です。

**構文**

```sql
randomPrintableASCII(length)
```

**引数**

- `length` — 文字列のバイト長。正の整数。

**戻り値**

- ランダムな[ASCII](https://en.wikipedia.org/wiki/ASCII#Printable_characters)印刷可能文字の集合を持つ文字列。[String](../data-types/string.md)

**例**

```sql
SELECT number, randomPrintableASCII(30) as str, length(str) FROM system.numbers LIMIT 3
```

```text
┌─number─┬─str────────────────────────────┬─length(randomPrintableASCII(30))─┐
│      0 │ SuiCOSTvC0csfABSw=UcSzp2.`rv8x │                               30 │
│      1 │ 1Ag NlJ &RCN:*>HVPG;PE-nO"SUFD │                               30 │
│      2 │ /"+<"wUTh:=LjJ Vm!c&hI*m#XTfzz │                               30 │
└────────┴────────────────────────────────┴──────────────────────────────────┘
```

## randomStringUTF8

指定された長さのランダムな文字列を生成します。結果の文字列は有効なUTF-8コードポイントを含みます。コードポイントの値は、Unicodeに割り当てられている範囲外である可能性があります。

**構文**

```sql
randomStringUTF8(length);
```

**引数**

- `length` — コードポイントでの文字列の長さ。[UInt64](../data-types/int-uint.md)。

**戻り値**

- UTF-8のランダム文字列。[String](../data-types/string.md)。

**例**

クエリ:

```sql
SELECT randomStringUTF8(13)
```

結果:

```text
┌─randomStringUTF8(13)─┐
│ 𘤗𙉝д兠庇󡅴󱱎󦐪􂕌𔊹𓰛   │
└──────────────────────┘
```

## fuzzBits {#fuzzBits}

**構文**

StringまたはFixedString `s`のビットをひっくり返します。それぞれのビットは確率`prob`で変更されます。

**構文**

```sql
fuzzBits(s, prob)
```

**引数**

- `s` - `String`または`FixedString`,
- `prob` - 0.0から1.0の間の定数`Float32/64`。

**戻り値**

`同タイプの` s`と同じタイプのファズされた文字列。

**例**

```sql
SELECT fuzzBits(materialize('abacaba'), 0.1)
FROM numbers(3)
```

結果:

```result
┌─fuzzBits(materialize('abacaba'), 0.1)─┐
│ abaaaja                               │
│ a*cjab+                               │
│ aeca2A                                │
└───────────────────────────────────────┘
```
