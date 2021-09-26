# 数学函数 {#shu-xue-han-shu}

以下所有的函数都返回一个Float64类型的数值。返回结果总是以尽可能最大精度返回，但还是可能与机器中可表示最接近该值的数字不同。

## e() {#e}

返回一个接近数学常量e的Float64数字。

## pi() {#pi}

返回一个接近数学常量π的Float64数字。

## exp(x) {#expx}

接受一个数值类型的参数并返回它的指数。

## log(x),ln(x) {#logx-lnx}

接受一个数值类型的参数并返回它的自然对数。

## exp2(x) {#exp2x}

接受一个数值类型的参数并返回它的2的x次幂。

## log2(x) {#log2x}

接受一个数值类型的参数并返回它的底2对数。

## exp10(x) {#exp10x}

接受一个数值类型的参数并返回它的10的x次幂。

## log10(x) {#log10x}

接受一个数值类型的参数并返回它的底10对数。

## sqrt(x) {#sqrtx}

接受一个数值类型的参数并返回它的平方根。

## cbrt(x) {#cbrtx}

接受一个数值类型的参数并返回它的立方根。

## erf(x) {#erfx}

如果’x’是非负数，那么`erf(x / σ√2)`是具有正态分布且标准偏差为«σ»的随机变量的值与预期值之间的距离大于«x»。

示例 （三西格玛准则）:

``` sql
SELECT erf(3 / sqrt(2))
```

    ┌─erf(divide(3, sqrt(2)))─┐
    │      0.9973002039367398 │
    └─────────────────────────┘

## erfc(x) {#erfcx}

接受一个数值参数并返回一个接近1 - erf(x)的Float64数字，但不会丢失大«x»值的精度。

## lgamma(x) {#lgammax}

返回x的绝对值的自然对数的伽玛函数。

## tgamma(x) {#tgammax}

返回x的伽玛函数。

## sin(x) {#sinx}

返回x的三角正弦值。

## cos(x) {#cosx}

返回x的三角余弦值。

## tan(x) {#tanx}

返回x的三角正切值。

## asin(x) {#asinx}

返回x的反三角正弦值。

## acos(x) {#acosx}

返回x的反三角余弦值。

## atan(x) {#atanx}

返回x的反三角正切值。

## pow(x,y),power(x,y) {#powx-y-powerx-y}

接受x和y两个参数。返回x的y次方。

## intExp2 {#intexp2}

接受一个数值类型的参数并返回它的2的x次幂（UInt64）。

## intExp10 {#intexp10}

接受一个数值类型的参数并返回它的10的x次幂（UInt64）。

[来源文章](https://clickhouse.com/docs/en/query_language/functions/math_functions/) <!--hide-->
