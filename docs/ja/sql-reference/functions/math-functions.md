---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: "\u6570\u5B66"
---

# 数学関数 {#mathematical-functions}

すべての関数はFloat64番号を返します。 結果の精度は可能な最大精度に近いですが、結果は対応する実数に最も近い機械表現可能な数と一致しない可能性があります。

## e() {#e}

数値eに近いFloat64番号を返します。

## pi() {#pi}

Returns a Float64 number that is close to the number π.

## exp(x) {#expx}

数値引数を受け取り、引数の指数に近いFloat64番号を返します。

## ログ(x),ln(x) {#logx-lnx}

数値引数を受け取り、引数の自然対数に近いFloat64数を返します。

## exp2(x) {#exp2x}

数値引数を受け取り、Float64をxの累乗に近い2に近い数値を返します。

## log2(x) {#log2x}

数値引数を受け取り、引数のバイナリ対数に近いFloat64数値を返します。

## exp10(x) {#exp10x}

数値引数を受け取り、Float64のxの累乗に近い10の数値を返します。

## log10(x) {#log10x}

数値引数を受け取り、引数の小数対数に近いFloat64数値を返します。

## sqrt(x) {#sqrtx}

数値引数を受け取り、引数の平方根に近いFloat64番号を返します。

## cbrt(x) {#cbrtx}

数値引数を受け取り、引数の三次根に近いFloat64番号を返します。

## erf(x) {#erfx}

もし ‘x’ 負でない場合、 `erf(x / σ√2)` 確率変数が標準偏差を持つ正規分布を持つ確率です ‘σ’ より多くの期待値から分離された値を取ります ‘x’.

例（三シグマ則):

``` sql
SELECT erf(3 / sqrt(2))
```

``` text
┌─erf(divide(3, sqrt(2)))─┐
│      0.9973002039367398 │
└─────────────────────────┘
```

## erfc(x) {#erfcx}

数値引数を受け取り、Float64の1-erf(x)に近い数値を返します。 ‘x’ 値。

## lgamma(x) {#lgammax}

ガンマ関数の対数。

## tgamma(x) {#tgammax}

ガンマ関数

## sin(x) {#sinx}

正弦。

## cos(x) {#cosx}

コサイン

## タン(x) {#tanx}

接線。

## asin(x) {#asinx}

アークサイン。

## acos(x) {#acosx}

アークコサイン。

## アタン(x) {#atanx}

円弧正接。

## pow(x,y),power(x,y) {#powx-y-powerx-y}

Yの累乗に近いFloat64の数値を返します。

## intExp2 {#intexp2}

数値引数を受け取り、xの累乗に近いUInt64数値を返します。

## intExp10 {#intexp10}

数値引数を受け取り、xの累乗に10に近いUInt64の数値を返します。

[元の記事](https://clickhouse.com/docs/en/query_language/functions/math_functions/) <!--hide-->
