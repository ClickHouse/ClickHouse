---
slug: /ja/sql-reference/functions/math-functions
sidebar_position: 125
sidebar_label: 数学関数
---

# 数学関数

## e

$e$（[オイラー数](https://en.wikipedia.org/wiki/Euler%27s_constant)）を返します。

**構文**

```sql
e()
```

**返される値**

型: [Float64](../data-types/float.md)。

## pi

$\pi$（[パイ](https://en.wikipedia.org/wiki/Pi)）を返します。

**構文**

```sql
pi()
```
**返される値**

型: [Float64](../data-types/float.md)。

## exp

$x$を指定した引数として、$e^{x}$を返します。

**構文**

```sql
exp(x)
```

**引数**

- `x` - [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**例**

クエリ:

```sql
SELECT round(exp(-1), 4);
```

結果:

```response
┌─round(exp(-1), 4)─┐
│            0.3679 │
└───────────────────┘
```

**返される値**

型: [Float*](../data-types/float.md)。

## log

引数の自然対数を返します。

**構文**

```sql
log(x)
```

別名: `ln(x)`

**引数**

- `x` - [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

型: [Float*](../data-types/float.md)。

## exp2

指定した引数の2のべき乗を返します。

**構文**

```sql
exp2(x)
```

**引数**

- `x` - [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

型: [Float*](../data-types/float.md)。

## intExp2

[`exp`](#exp) のように挙動しますが、UInt64を返します。

**構文**

```sql
intExp2(x)
```

## log2

引数の二進対数を返します。

**構文**

```sql
log2(x)
```

**引数**

- `x` - [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

型: [Float*](../data-types/float.md)。

## exp10

指定した引数の10のべき乗を返します。

**構文**

```sql
exp10(x)
```

**引数**

- `x` - [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

型: [Float*](../data-types/float.md)。

## intExp10

[`exp10`](#exp10) のように挙動しますが、UInt64を返します。

**構文**

```sql
intExp10(x)
```

## log10

引数の十進対数を返します。

**構文**

```sql
log10(x)
```

**引数**

- `x` - [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

型: [Float*](../data-types/float.md)。

## sqrt

引数の平方根を返します。

```sql
sqrt(x)
```

**引数**

- `x` - [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

型: [Float*](../data-types/float.md)。

## cbrt

引数の立方根を返します。

```sql
cbrt(x)
```

**引数**

- `x` - [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

型: [Float*](../data-types/float.md)。

## erf

`x` が非負である場合、$erf(\frac{x}{\sigma\sqrt{2}})$ は、標準偏差 $\sigma$ を持つ正規分布において、期待値から `x` を超える値を取る確率です。

**構文**

```sql
erf(x)
```

**引数**

- `x` - [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

型: [Float*](../data-types/float.md)。

**例**

（3 sigma ルール）

``` sql
SELECT erf(3 / sqrt(2));
```

```result
┌─erf(divide(3, sqrt(2)))─┐
│      0.9973002039367398 │
└─────────────────────────┘
```

## erfc

大きな `x` 値に対して精度を失うことなく $1-erf(x)$ に近い数値を返します。

**構文**

```sql
erfc(x)
```

**引数**

- `x` - [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

型: [Float*](../data-types/float.md)。

## lgamma

ガンマ関数の対数を返します。

**構文**

```sql
lgamma(x)
```

**引数**

- `x` - [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

型: [Float*](../data-types/float.md)。

## tgamma

ガンマ関数を返します。

**構文**

```sql
gamma(x)
```

**引数**

- `x` - [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

型: [Float*](../data-types/float.md)。

## sin

引数の正弦を返します。

**構文**

```sql
sin(x)
```

**引数**

- `x` - [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

型: [Float*](../data-types/float.md)。

**例**

クエリ:

```sql
SELECT sin(1.23);
```

```response
0.9424888019316975
```

## cos

引数の余弦を返します。

**構文**

```sql
cos(x)
```

**引数**

- `x` - [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

型: [Float*](../data-types/float.md)。

## tan

引数の正接を返します。

**構文**

```sql
tan(x)
```

**引数**

- `x` - [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

型: [Float*](../data-types/float.md)。

## asin

引数の逆正弦を返します。

**構文**

```sql
asin(x)
```

**引数**

- `x` - [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

型: [Float*](../data-types/float.md)。

## acos

引数の逆余弦を返します。

**構文**

```sql
acos(x)
```

**引数**

- `x` - [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

型: [Float*](../data-types/float.md)。

## atan

引数の逆正接を返します。

**構文**

```sql
atan(x)
```

**引数**

- `x` - [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

型: [Float*](../data-types/float.md)。

## pow

$x^y$ を返します。

**構文**

```sql
pow(x, y)
```

別名: `power(x, y)`

**引数**

- `x` - [(U)Int8/16/32/64](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)
- `y` - [(U)Int8/16/32/64](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)

**返される値**

型: [Float64](../data-types/float.md)。

## cosh

引数の[双曲線余弦](https://in.mathworks.com/help/matlab/ref/cosh.html)を返します。

**構文**

``` sql
cosh(x)
```

**引数**

- `x` — 角度（ラジアン）。区間の値: $-\infty \lt x \lt +\infty$。[ (U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

- 区間: $1 \le cosh(x) \lt +\infty$。

型: [Float64](../data-types/float.md#float32-float64)。

**例**

``` sql
SELECT cosh(0);
```

結果:

```result
┌─cosh(0)──┐
│        1 │
└──────────┘
```

## acosh

[inverse hyperbolic 余弦](https://www.mathworks.com/help/matlab/ref/acosh.html)を返します。

**構文**

``` sql
acosh(x)
```

**引数**

- `x` — 角度の双曲線余弦。区間の値: $1 \le x \lt +\infty$。[ (U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

- 角度（ラジアン）。区間の値: $0 \le acosh(x) \lt +\infty$。

型: [Float64](../data-types/float.md#float32-float64)。

**例**

``` sql
SELECT acosh(1);
```

結果:

```result
┌─acosh(1)─┐
│        0 │
└──────────┘
```

## sinh

[双曲線正弦](https://www.mathworks.com/help/matlab/ref/sinh.html)を返します。

**構文**

``` sql
sinh(x)
```

**引数**

- `x` — 角度（ラジアン）。区間の値: $-\infty \lt x \lt +\infty$。[ (U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

- 区間の値: $-\infty \lt sinh(x) \lt +\infty$。

型: [Float64](../data-types/float.md#float32-float64)。

**例**

``` sql
SELECT sinh(0);
```

結果:

```result
┌─sinh(0)──┐
│        0 │
└──────────┘
```

## asinh

[逆双曲線正弦](https://www.mathworks.com/help/matlab/ref/asinh.html)を返します。

**構文**

``` sql
asinh(x)
```

**引数**

- `x` — 角度の双曲線正弦。区間の値: $-\infty \lt x \lt +\infty$。[ (U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

- 角度（ラジアン）。区間の値: $-\infty \lt asinh(x) \lt +\infty$。

型: [Float64](../data-types/float.md#float32-float64)。

**例**

``` sql
SELECT asinh(0);
```

結果:

```result
┌─asinh(0)─┐
│        0 │
└──────────┘
```

## tanh

[双曲線正接](https://www.mathworks.com/help/matlab/ref/tanh.html)を返します。

**構文**

``` sql
tanh(x)
```

**引数**

- `x` — 角度（ラジアン）。区間の値: $-\infty \lt x \lt +\infty$。[ (U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

- 区間の値: $-1 \lt tanh(x) \lt 1$。

型: [Float*](../data-types/float.md#float32-float64)。

**例**

``` sql
SELECT tanh(0);
```

結果:

```result
0
```

## atanh

[逆双曲線正接](https://www.mathworks.com/help/matlab/ref/atanh.html)を返します。

**構文**

``` sql
atanh(x)
```

**引数**

- `x` — 角度の双曲線正接。区間の値: $-1 \lt x \lt 1$。[ (U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

- 角度（ラジアン）。区間の値: $-\infty \lt atanh(x) \lt +\infty$。

型: [Float64](../data-types/float.md#float32-float64)。

**例**

``` sql
SELECT atanh(0);
```

結果:

```result
┌─atanh(0)─┐
│        0 │
└──────────┘
```

## atan2

[atan2](https://en.wikipedia.org/wiki/Atan2) を、ユークリッド平面における角度として返し、ラジアンで表されますが、正の x 軸と点 `(x, y) ≠ (0, 0)` への光線との間のものです。

**構文**

``` sql
atan2(y, x)
```

**引数**

- `y` — 光線が通過する点の y 座標。 [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。
- `x` — 光線が通過する点の x 座標。 [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

- 角度 `θ` の値として、$-\pi \lt 0 \le \pi$ の区間で、ラジアンで表されます。

型: [Float64](../data-types/float.md#float32-float64)。

**例**

``` sql
SELECT atan2(1, 1);
```

結果:

```result
┌────────atan2(1, 1)─┐
│ 0.7853981633974483 │
└────────────────────┘
```

## hypot

直角三角形の斜辺の長さを返します。[Hypot](https://en.wikipedia.org/wiki/Hypot) は非常に大きな数や非常に小さな数を平方する際の問題を回避します。

**構文**

``` sql
hypot(x, y)
```

**引数**

- `x` — 直角三角形の一つのカテート。[(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。
- `y` — 直角三角形のもう一つのカテート。[(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

- 直角三角形の斜辺の長さ。

型: [Float64](../data-types/float.md#float32-float64)。

**例**

``` sql
SELECT hypot(1, 1);
```

結果:

```result
┌────────hypot(1, 1)─┐
│ 1.4142135623730951 │
└────────────────────┘
```

## log1p

`log(1+x)` を計算します。 [計算](https://en.wikipedia.org/wiki/Natural_logarithm#lnp1) `log1p(x)` は、小さな値の x に対して `log(1+x)` よりも精度が高いです。

**構文**

``` sql
log1p(x)
```

**引数**

- `x` — 値の範囲: $-1 \lt x \lt +\infty$。[(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md) または [Decimal*](../data-types/decimal.md)。

**返される値**

- 値の範囲: $-\infty < log1p(x) \lt +\infty$。

型: [Float64](../data-types/float.md#float32-float64)。

**例**

``` sql
SELECT log1p(0);
```

結果:

```result
┌─log1p(0)─┐
│        0 │
└──────────┘
```

## sign

実数の符号を返します。

**構文**

``` sql
sign(x)
```

**引数**

- `x` — 値は $-\infty$ から $+\infty$ まで。ClickHouse のすべての数値型をサポートしています。

**返される値**

- `x < 0` の場合は -1
- `x = 0` の場合は 0
- `x > 0` の場合は 1

型: [Int8](../data-types/int-uint.md)。

**例**

ゼロ値の符号:

``` sql
SELECT sign(0);
```

結果:

```result
┌─sign(0)─┐
│       0 │
└─────────┘
```

正の値の符号:

``` sql
SELECT sign(1);
```

結果:

```result
┌─sign(1)─┐
│       1 │
└─────────┘
```

負の値の符号:

``` sql
SELECT sign(-1);
```

結果:

```result
┌─sign(-1)─┐
│       -1 │
└──────────┘
```

## sigmoid

[sigmoid 関数](https://en.wikipedia.org/wiki/Sigmoid_function)を返します。

**構文**

```sql
sigmoid(x)
```

**パラメータ**

- `x` — 入力値。範囲は $-\infty \lt x \lt +\infty$。[(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md), または [Decimal*](../data-types/decimal.md)。

**返される値**

- 0から1の間の sigmoid 曲線に沿った対応する値。[Float64](../data-types/float.md)。

**例**

クエリ:

``` sql
SELECT round(sigmoid(x), 5) FROM (SELECT arrayJoin([-1, 0, 1]) AS x);
```

結果:

```result
0.26894
0.5
0.73106
```

## degrees

ラジアンを度に変換します。

**構文**

``` sql
degrees(x)
```

**引数**

- `x` — 入力値（ラジアン）。[(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md), または [Decimal*](../data-types/decimal.md)。
- `x` — 入力値（ラジアン）。[(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md), または [Decimal*](../data-types/decimal.md)。

**返される値**

- 度で表された値。[Float64](../data-types/float.md#float32-float64)。

**例**

``` sql
SELECT degrees(3.141592653589793);
```

結果:

```result
┌─degrees(3.141592653589793)─┐
│                        180 │
└────────────────────────────┘
```

## radians

度をラジアンに変換します。

**構文**

``` sql
radians(x)
```

**引数**

- `x` — 入力値（度）。[(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md), または [Decimal*](../data-types/decimal.md)。

**返される値**

- ラジアンで表された値。

型: [Float64](../data-types/float.md#float32-float64)。

**例**

``` sql
SELECT radians(180);
```

結果:

```result
┌──────radians(180)─┐
│ 3.141592653589793 │
└───────────────────┘
```

## factorial

整数値の階乗を計算します。UInt(8|16|32|64) および Int(8|16|32|64) を含む任意のネイティブ整数型で動作します。戻りの型は UInt64 です。

0 の階乗は 1 です。同様に、`factorial()` 関数は任意の負の値に対して 1 を返します。入力引数の最大正の値は 20 で、21 以上の値は例外をスローします。

**構文**

``` sql
factorial(n)
```

**例**

``` sql
SELECT factorial(10);
```

結果:

```result
┌─factorial(10)─┐
│       3628800 │
└───────────────┘
```

## width_bucket

ヒストグラムにおいて、`operand` が `low` から `high` までの範囲に分割された `count` 等幅のバケットのうち、どのバケットに属するかを返します。`operand < low` の場合は `0` を返し、`operand >= high` の場合は `count+1` を返します。

`operand`、`low`、`high` は任意のネイティブ数値型にすることができます。 `count` は符号なしのネイティブ整数型にすることができ、その値はゼロであってはなりません。

**構文**

```sql
widthBucket(operand, low, high, count)
```
別名: `WIDTH_BUCKET`

**例**

``` sql
SELECT widthBucket(10.15, -8.6, 23, 18);
```

結果:

```result
┌─widthBucket(10.15, -8.6, 23, 18)─┐
│                               11 │
└──────────────────────────────────┘
```

## proportionsZTest

二つの母集団 `x` と `y` の比率を比較するための統計的検定である2比率Z検定の統計量を返します。

**構文**

```sql
proportionsZTest(successes_x, successes_y, trials_x, trials_y, conf_level, pool_type)
```

**引数**

- `successes_x`: 母集団 `x` における成功の数。 [UInt64](../data-types/int-uint.md)。
- `successes_y`: 母集団 `y` における成功の数。 [UInt64](../data-types/int-uint.md)。
- `trials_x`: 母集団 `x` における試行の数。 [UInt64](../data-types/int-uint.md)。
- `trials_y`: 母集団 `y` における試行の数。 [UInt64](../data-types/int-uint.md)。
- `conf_level`: 検定における信頼水準。 [Float64](../data-types/float.md)。
- `pool_type`: プール方式（標準誤差の推定方式）。`unpooled` または `pooled` のいずれか。 [String](../data-types/string.md)。 

:::note
引数 `pool_type` に関して: プールされたバージョンでは、2つの比率が平均され、1つの比率だけが標準誤差を推定するために使用されます。プールされていないバージョンでは、2つの比率が別々に使用されます。
:::

**返される値**

- `z_stat`: Z 統計量。 [Float64](../data-types/float.md)。
- `p_val`: P 値。 [Float64](../data-types/float.md)。
- `ci_low`: 下側の信頼区間。 [Float64](../data-types/float.md)。
- `ci_high`: 上側の信頼区間。 [Float64](../data-types/float.md)。

**例**

クエリ:

```sql
SELECT proportionsZTest(10, 11, 100, 101, 0.95, 'unpooled');
```

結果:

```response
┌─proportionsZTest(10, 11, 100, 101, 0.95, 'unpooled')───────────────────────────────┐
│ (-0.20656724435948853,0.8363478437079654,-0.09345975390115283,0.07563797172293502) │
└────────────────────────────────────────────────────────────────────────────────────┘
```
