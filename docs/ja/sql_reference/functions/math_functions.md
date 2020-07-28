---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 44
toc_title: "\u6570\u5B66"
---

# 数学関数 {#mathematical-functions}

すべての関数はfloat64番号を返します。 結果の精度は可能な最大精度に近いですが、結果は対応する実数に最も近いマシン表現可能な数値と一致しない場合があります。

## e() {#e}

数値eに近いfloat64数値を返します。

## pi() {#pi}

Returns a Float64 number that is close to the number π.

## exp(x) {#expx}

数値引数を受け取り、引数の指数に近いfloat64数値を返します。

## ログ(x),ln(x) {#logx-lnx}

数値引数を受け取り、引数の自然対数に近いfloat64数値を返します。

## exp2(x) {#exp2x}

数値引数を受け取り、float64の数値を2に近づけてxの累乗を返します。

## log2(x) {#log2x}

数値引数を受け取り、引数のバイナリ対数に近いfloat64数値を返します。

## exp10(x) {#exp10x}

数値引数を受け取り、float64の数値をxの累乗に近い10に返します。

## log10(x) {#log10x}

数値引数を受け取り、引数の小数点以下の対数に近いfloat64数値を返します。

## sqrt(x) {#sqrtx}

数値引数を受け取り、引数の平方根に近いfloat64数値を返します。

## cbrt(x) {#cbrtx}

数値引数を受け取り、引数の三次根に近いfloat64数値を返します。

## erf(x) {#erfx}

もし ‘x’ is non-negative, then erf(x / σ√2)<g> 標準偏差を持つ正規分布を持つ確率変数です ‘σ’ 期待値から分離されている値を次の値よりも大きくします ‘x’.

例(三つのシグマルール):

``` sql
SELECT erf(3 / sqrt(2))
```

``` text
┌─erf(divide(3, sqrt(2)))─┐
│      0.9973002039367398 │
└─────────────────────────┘
```

## erfc(x) {#erfcx}

数値引数を受け取り、float64数値を1-erf(x)に近い値に返しますが、大きな値の精度を失うことはありません ‘x’ 値。

## lgamma(x) {#lgammax}

ガンマ関数の対数。

## tgamma(x) {#tgammax}

ガンマ関数。

## sin(x) {#sinx}

サイン。

## cos(x) {#cosx}

コサイン

## tan(x) {#tanx}

タンジェント。

## asin(x) {#asinx}

アークの正弦。

## acos(x) {#acosx}

アークコサイン。

## atan(x) {#atanx}

アークタンジェント。

## pow(x,y),パワー(x,y) {#powx-y-powerx-y}

Xに近いFloat64数をyの累乗に返します。

## intExp2 {#intexp2}

数値引数を受け取り、xの累乗に対してuint64の2に近い数値を返します。

## intExp10 {#intexp10}

数値引数を受け取り、xの累乗に近いuint64の数値を10に返します。

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/math_functions/) <!--hide-->
