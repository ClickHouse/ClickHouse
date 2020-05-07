---
toc_priority: 44
toc_title: Mathematical
---

# Mathematical Functions {#mathematical-functions}

All the functions return a Float64 number. The accuracy of the result is close to the maximum precision possible, but the result might not coincide with the machine representable number nearest to the corresponding real number.

## e() {#e}

Returns a Float64 number that is close to the number e.

## pi() {#pi}

Returns a Float64 number that is close to the number π.

## exp(x) {#expx}

Accepts a numeric argument and returns a Float64 number close to the exponent of the argument.

## log(x), ln(x) {#logx-lnx}

Accepts a numeric argument and returns a Float64 number close to the natural logarithm of the argument.

## exp2(x) {#exp2x}

Accepts a numeric argument and returns a Float64 number close to 2 to the power of x.

## log2(x) {#log2x}

Accepts a numeric argument and returns a Float64 number close to the binary logarithm of the argument.

## exp10(x) {#exp10x}

Accepts a numeric argument and returns a Float64 number close to 10 to the power of x.

## log10(x) {#log10x}

Accepts a numeric argument and returns a Float64 number close to the decimal logarithm of the argument.

## sqrt(x) {#sqrtx}

Accepts a numeric argument and returns a Float64 number close to the square root of the argument.

## cbrt(x) {#cbrtx}

Accepts a numeric argument and returns a Float64 number close to the cubic root of the argument.

## erf(x) {#erfx}

If ‘x’ is non-negative, then `erf(x / σ√2)` is the probability that a random variable having a normal distribution with standard deviation ‘σ’ takes the value that is separated from the expected value by more than ‘x’.

Example (three sigma rule):

``` sql
SELECT erf(3 / sqrt(2))
```

``` text
┌─erf(divide(3, sqrt(2)))─┐
│      0.9973002039367398 │
└─────────────────────────┘
```

## erfc(x) {#erfcx}

Accepts a numeric argument and returns a Float64 number close to 1 - erf(x), but without loss of precision for large ‘x’ values.

## lgamma(x) {#lgammax}

The logarithm of the gamma function.

## tgamma(x) {#tgammax}

Gamma function.

## sin(x) {#sinx}

The sine.

## cos(x) {#cosx}

The cosine.

## tan(x) {#tanx}

The tangent.

## asin(x) {#asinx}

The arc sine.

## acos(x) {#acosx}

The arc cosine.

## atan(x) {#atanx}

The arc tangent.

## pow(x, y), power(x, y) {#powx-y-powerx-y}

Takes two numeric arguments x and y. Returns a Float64 number close to x to the power of y.

## intExp2 {#intexp2}

Accepts a numeric argument and returns a UInt64 number close to 2 to the power of x.

## intExp10 {#intexp10}

Accepts a numeric argument and returns a UInt64 number close to 10 to the power of x.

[Original article](https://clickhouse.tech/docs/en/query_language/functions/math_functions/) <!--hide-->
