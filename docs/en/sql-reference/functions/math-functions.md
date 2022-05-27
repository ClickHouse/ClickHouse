---
sidebar_position: 44
sidebar_label: Mathematical
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
SELECT erf(3 / sqrt(2));
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

## cosh(x) {#coshx}

[Hyperbolic cosine](https://in.mathworks.com/help/matlab/ref/cosh.html).

**Syntax**

``` sql
cosh(x)
```

**Arguments**

-   `x` — The angle, in radians. Values from the interval: `-∞ < x < +∞`. [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Returned value**

-   Values from the interval: `1 <= cosh(x) < +∞`.

Type: [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Example**

Query:

``` sql
SELECT cosh(0);
```

Result:

``` text
┌─cosh(0)──┐
│        1 │
└──────────┘
```

## acosh(x) {#acoshx}

[Inverse hyperbolic cosine](https://www.mathworks.com/help/matlab/ref/acosh.html).

**Syntax**

``` sql
acosh(x)
```

**Arguments**

-   `x` — Hyperbolic cosine of angle. Values from the interval: `1 <= x < +∞`. [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Returned value**

-   The angle, in radians. Values from the interval: `0 <= acosh(x) < +∞`.

Type: [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Example**

Query:

``` sql
SELECT acosh(1);
```

Result:

``` text
┌─acosh(1)─┐
│        0 │
└──────────┘
```

**See Also**

-   [cosh(x)](../../sql-reference/functions/math-functions.md#coshx)

## sinh(x) {#sinhx}

[Hyperbolic sine](https://www.mathworks.com/help/matlab/ref/sinh.html).

**Syntax**

``` sql
sinh(x)
```

**Arguments**

-   `x` — The angle, in radians. Values from the interval: `-∞ < x < +∞`. [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Returned value**

-   Values from the interval: `-∞ < sinh(x) < +∞`.

Type: [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Example**

Query:

``` sql
SELECT sinh(0);
```

Result:

``` text
┌─sinh(0)──┐
│        0 │
└──────────┘
```

## asinh(x) {#asinhx}

[Inverse hyperbolic sine](https://www.mathworks.com/help/matlab/ref/asinh.html).

**Syntax**

``` sql
asinh(x)
```

**Arguments**

-   `x` — Hyperbolic sine of angle. Values from the interval: `-∞ < x < +∞`. [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Returned value**

-   The angle, in radians. Values from the interval: `-∞ < asinh(x) < +∞`.

Type: [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Example**

Query:

``` sql
SELECT asinh(0);
```

Result:

``` text
┌─asinh(0)─┐
│        0 │
└──────────┘
```

**See Also**

-   [sinh(x)](../../sql-reference/functions/math-functions.md#sinhx)

## atanh(x) {#atanhx}

[Inverse hyperbolic tangent](https://www.mathworks.com/help/matlab/ref/atanh.html).

**Syntax**

``` sql
atanh(x)
```

**Arguments**

-   `x` — Hyperbolic tangent of angle. Values from the interval: `–1 < x < 1`. [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Returned value**

-   The angle, in radians. Values from the interval: `-∞ < atanh(x) < +∞`.

Type: [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Example**

Query:

``` sql
SELECT atanh(0);
```

Result:

``` text
┌─atanh(0)─┐
│        0 │
└──────────┘
```

## atan2(y, x) {#atan2yx}

The [function](https://en.wikipedia.org/wiki/Atan2) calculates the angle in the Euclidean plane, given in radians, between the positive x axis and the ray to the point `(x, y) ≠ (0, 0)`.

**Syntax**

``` sql
atan2(y, x)
```

**Arguments**

-   `y` — y-coordinate of the point through which the ray passes. [Float64](../../sql-reference/data-types/float.md#float32-float64).
-   `x` — x-coordinate of the point through which the ray passes. [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Returned value**

-   The angle `θ` such that `−π < θ ≤ π`, in radians.

Type: [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Example**

Query:

``` sql
SELECT atan2(1, 1);
```

Result:

``` text
┌────────atan2(1, 1)─┐
│ 0.7853981633974483 │
└────────────────────┘
```

## hypot(x, y) {#hypotxy}

Calculates the length of the hypotenuse of a right-angle triangle. The [function](https://en.wikipedia.org/wiki/Hypot) avoids problems that occur when squaring very large or very small numbers.

**Syntax**

``` sql
hypot(x, y)
```

**Arguments**

-   `x` — The first cathetus of a right-angle triangle. [Float64](../../sql-reference/data-types/float.md#float32-float64).
-   `y` — The second cathetus of a right-angle triangle. [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Returned value**

-   The length of the hypotenuse of a right-angle triangle.

Type: [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Example**

Query:

``` sql
SELECT hypot(1, 1);
```

Result:

``` text
┌────────hypot(1, 1)─┐
│ 1.4142135623730951 │
└────────────────────┘
```

## log1p(x) {#log1px}

Calculates `log(1+x)`. The [function](https://en.wikipedia.org/wiki/Natural_logarithm#lnp1) `log1p(x)` is more accurate than `log(1+x)` for small values of x.

**Syntax**

``` sql
log1p(x)
```

**Arguments**

-   `x` — Values from the interval: `-1 < x < +∞`. [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Returned value**

-   Values from the interval: `-∞ < log1p(x) < +∞`.

Type: [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Example**

Query:

``` sql
SELECT log1p(0);
```

Result:

``` text
┌─log1p(0)─┐
│        0 │
└──────────┘
```

**See Also**

-   [log(x)](../../sql-reference/functions/math-functions.md#logx-lnx)

## sign(x) {#signx}

Returns the sign of a real number.

**Syntax**

``` sql
sign(x)
```

**Arguments**

-   `x` — Values from  `-∞` to `+∞`. Support all numeric types in ClickHouse.

**Returned value**

- -1 for `x < 0`
-  0 for `x = 0`
-  1 for `x > 0`

**Examples**

Sign for the zero value:

``` sql
SELECT sign(0);
```

Result:

``` text
┌─sign(0)─┐
│       0 │
└─────────┘
```

Sign for the positive value:

``` sql
SELECT sign(1);
```

Result:

``` text
┌─sign(1)─┐
│       1 │
└─────────┘
```

Sign for the negative value:

``` sql
SELECT sign(-1);
```

Result:

``` text
┌─sign(-1)─┐
│       -1 │
└──────────┘
```

## degrees(x) {#degreesx}

Converts the input value in radians to degrees.

**Syntax**

``` sql
degrees(x)
```

**Arguments**

-   `x` — Input in radians. [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Returned value**

-   Value in degrees.

Type: [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Example**

Query:

``` sql
SELECT degrees(3.141592653589793);
```

Result:

``` text
┌─degrees(3.141592653589793)─┐
│                        180 │
└────────────────────────────┘
```

## radians(x) {#radiansx}

Converts the input value in degrees to radians.

**Syntax**

``` sql
radians(x)
```

**Arguments**

-   `x` — Input in degrees. [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Returned value**

-   Value in radians.

Type: [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Example**

Query:

``` sql
SELECT radians(180);
```

Result:

``` text
┌──────radians(180)─┐
│ 3.141592653589793 │
└───────────────────┘
```
