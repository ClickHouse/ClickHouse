---
slug: /en/sql-reference/functions/math-functions
sidebar_position: 125
sidebar_label: Mathematical
---

# Mathematical Functions

## e

Returns e ([Euler's constant](https://en.wikipedia.org/wiki/Euler%27s_constant))

**Syntax**

```sql
e()
```

**Returned value**

Type: [Float64](../../sql-reference/data-types/float.md).

## pi

Returns π ([Pi](https://en.wikipedia.org/wiki/Pi)).

**Syntax**

```sql
pi()
```
**Returned value**

Type: [Float64](../../sql-reference/data-types/float.md).

## exp

Returns e to the power of the given argument.

**Syntax**

```sql
exp(x)
```

**Arguments**

- `x` - [(U)Int*](../../sql-reference/data-types/int-uint.md), [Float*](../../sql-reference/data-types/float.md) or [Decimal*](../../sql-reference/data-types/decimal.md)

**Returned value**

Type: [Float*](../../sql-reference/data-types/float.md).

## log

Returns the natural logarithm of the argument.

**Syntax**

```sql
log(x)
```

Alias: `ln(x)`

**Arguments**

- `x` - [(U)Int*](../../sql-reference/data-types/int-uint.md), [Float*](../../sql-reference/data-types/float.md) or [Decimal*](../../sql-reference/data-types/decimal.md)

**Returned value**

Type: [Float*](../../sql-reference/data-types/float.md).

## exp2

Returns 2 to the power of the given argument

**Syntax**

```sql
exp2(x)
```

**Arguments**

- `x` - [(U)Int*](../../sql-reference/data-types/int-uint.md), [Float*](../../sql-reference/data-types/float.md) or [Decimal*](../../sql-reference/data-types/decimal.md)

**Returned value**

Type: [Float*](../../sql-reference/data-types/float.md).

## intExp2

Like `exp` but returns a UInt64.

**Syntax**

```sql
intExp2(x)
```

## log2

Returns the binary logarithm of the argument.

**Syntax**

```sql
log2(x)
```

**Arguments**

- `x` - [(U)Int*](../../sql-reference/data-types/int-uint.md), [Float*](../../sql-reference/data-types/float.md) or [Decimal*](../../sql-reference/data-types/decimal.md)

**Returned value**

Type: [Float*](../../sql-reference/data-types/float.md).

## exp10

Returns 10 to the power of the given argument.

**Syntax**

```sql
exp10(x)
```

**Arguments**

- `x` - [(U)Int*](../../sql-reference/data-types/int-uint.md), [Float*](../../sql-reference/data-types/float.md) or [Decimal*](../../sql-reference/data-types/decimal.md)

**Returned value**

Type: [Float*](../../sql-reference/data-types/float.md).

## intExp10

Like `exp10` but returns a UInt64.

**Syntax**

```sql
intExp10(x)
```

## log10

Returns the decimal logarithm of the argument.

**Syntax**

```sql
log10(x)
```

**Arguments**

- `x` - [(U)Int*](../../sql-reference/data-types/int-uint.md), [Float*](../../sql-reference/data-types/float.md) or [Decimal*](../../sql-reference/data-types/decimal.md)

**Returned value**

Type: [Float*](../../sql-reference/data-types/float.md).

## sqrt

Returns the square root of the argument.

```sql
sqrt(x)
```

**Arguments**

- `x` - [(U)Int*](../../sql-reference/data-types/int-uint.md), [Float*](../../sql-reference/data-types/float.md) or [Decimal*](../../sql-reference/data-types/decimal.md)

**Returned value**

Type: [Float*](../../sql-reference/data-types/float.md).

## cbrt

Returns the cubic root of the argument.

```sql
cbrt(x)
```

**Arguments**

- `x` - [(U)Int*](../../sql-reference/data-types/int-uint.md), [Float*](../../sql-reference/data-types/float.md) or [Decimal*](../../sql-reference/data-types/decimal.md)

**Returned value**

Type: [Float*](../../sql-reference/data-types/float.md).

## erf

If `x` is non-negative, then `erf(x / σ√2)` is the probability that a random variable having a normal distribution with standard deviation `σ` takes the value that is separated from the expected value by more than `x`.

**Syntax**

```sql
erf(x)
```

**Arguments**

- `x` - [(U)Int*](../../sql-reference/data-types/int-uint.md), [Float*](../../sql-reference/data-types/float.md) or [Decimal*](../../sql-reference/data-types/decimal.md)

**Returned value**

Type: [Float*](../../sql-reference/data-types/float.md).

**Example**

(three sigma rule)

``` sql
SELECT erf(3 / sqrt(2));
```

```result
┌─erf(divide(3, sqrt(2)))─┐
│      0.9973002039367398 │
└─────────────────────────┘
```

## erfc

Returns a number close to `1 - erf(x)` without loss of precision for large ‘x’ values.

**Syntax**

```sql
erfc(x)
```

**Arguments**

- `x` - [(U)Int*](../../sql-reference/data-types/int-uint.md), [Float*](../../sql-reference/data-types/float.md) or [Decimal*](../../sql-reference/data-types/decimal.md)

**Returned value**

Type: [Float*](../../sql-reference/data-types/float.md).

## lgamma

Returns the logarithm of the gamma function.

**Syntax**

```sql
lgamma(x)
```

**Arguments**

- `x` - [(U)Int*](../../sql-reference/data-types/int-uint.md), [Float*](../../sql-reference/data-types/float.md) or [Decimal*](../../sql-reference/data-types/decimal.md)

**Returned value**

Type: [Float*](../../sql-reference/data-types/float.md).

## tgamma

Returns the gamma function.

**Syntax**

```sql
gamma(x)
```

**Arguments**

- `x` - [(U)Int*](../../sql-reference/data-types/int-uint.md), [Float*](../../sql-reference/data-types/float.md) or [Decimal*](../../sql-reference/data-types/decimal.md)

**Returned value**

Type: [Float*](../../sql-reference/data-types/float.md).

## sin

Returns the sine of the argument

**Syntax**

```sql
sin(x)
```

**Arguments**

- `x` - [(U)Int*](../../sql-reference/data-types/int-uint.md), [Float*](../../sql-reference/data-types/float.md) or [Decimal*](../../sql-reference/data-types/decimal.md)

**Returned value**

Type: [Float*](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT sin(1.23);
```

```response
0.9424888019316975
```

## cos

Returns the cosine of the argument.

**Syntax**

```sql
cos(x)
```

**Arguments**

- `x` - [(U)Int*](../../sql-reference/data-types/int-uint.md), [Float*](../../sql-reference/data-types/float.md) or [Decimal*](../../sql-reference/data-types/decimal.md)

**Returned value**

Type: [Float*](../../sql-reference/data-types/float.md).

## tan

Returns the tangent of the argument.

**Syntax**

```sql
tan(x)
```

**Arguments**

- `x` - [(U)Int*](../../sql-reference/data-types/int-uint.md), [Float*](../../sql-reference/data-types/float.md) or [Decimal*](../../sql-reference/data-types/decimal.md)

**Returned value**

Type: [Float*](../../sql-reference/data-types/float.md).

## asin

Returns the arc sine of the argument.

**Syntax**

```sql
asin(x)
```

**Arguments**

- `x` - [(U)Int*](../../sql-reference/data-types/int-uint.md), [Float*](../../sql-reference/data-types/float.md) or [Decimal*](../../sql-reference/data-types/decimal.md)

**Returned value**

Type: [Float*](../../sql-reference/data-types/float.md).

## acos

Returns the arc cosine of the argument.

**Syntax**

```sql
acos(x)
```

**Arguments**

- `x` - [(U)Int*](../../sql-reference/data-types/int-uint.md), [Float*](../../sql-reference/data-types/float.md) or [Decimal*](../../sql-reference/data-types/decimal.md)

**Returned value**

Type: [Float*](../../sql-reference/data-types/float.md).

## atan

Returns the arc tangent of the argument.

**Syntax**

```sql
atan(x)
```

**Arguments**

- `x` - [(U)Int*](../../sql-reference/data-types/int-uint.md), [Float*](../../sql-reference/data-types/float.md) or [Decimal*](../../sql-reference/data-types/decimal.md)

**Returned value**

Type: [Float*](../../sql-reference/data-types/float.md).

## pow

Returns `x` to the power of `y`.

**Syntax**

```sql
pow(x, y)
```

Alias: `power(x, y)`

**Arguments**

- `x` - [(U)Int8/16/32/64](../../sql-reference/data-types/int-uint.md) or [Float*](../../sql-reference/data-types/float.md)
- `y` - [(U)Int8/16/32/64](../../sql-reference/data-types/int-uint.md) or [Float*](../../sql-reference/data-types/float.md)

**Returned value**

Type: [Float64](../../sql-reference/data-types/float.md).

## cosh

Returns the [hyperbolic cosine](https://in.mathworks.com/help/matlab/ref/cosh.html) of the argument.

**Syntax**

``` sql
cosh(x)
```

**Arguments**

- `x` — The angle, in radians. Values from the interval: `-∞ < x < +∞`. [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Returned value**

- Values from the interval: `1 <= cosh(x) < +∞`.

Type: [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Example**

``` sql
SELECT cosh(0);
```

Result:

```result
┌─cosh(0)──┐
│        1 │
└──────────┘
```

## acosh

Returns the [inverse hyperbolic cosine](https://www.mathworks.com/help/matlab/ref/acosh.html).

**Syntax**

``` sql
acosh(x)
```

**Arguments**

- `x` — Hyperbolic cosine of angle. Values from the interval: `1 <= x < +∞`. [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Returned value**

- The angle, in radians. Values from the interval: `0 <= acosh(x) < +∞`.

Type: [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Example**

``` sql
SELECT acosh(1);
```

Result:

```result
┌─acosh(1)─┐
│        0 │
└──────────┘
```

## sinh

Returns the [hyperbolic sine](https://www.mathworks.com/help/matlab/ref/sinh.html).

**Syntax**

``` sql
sinh(x)
```

**Arguments**

- `x` — The angle, in radians. Values from the interval: `-∞ < x < +∞`. [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Returned value**

- Values from the interval: `-∞ < sinh(x) < +∞`.

Type: [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Example**

``` sql
SELECT sinh(0);
```

Result:

```result
┌─sinh(0)──┐
│        0 │
└──────────┘
```

## asinh

Returns the [inverse hyperbolic sine](https://www.mathworks.com/help/matlab/ref/asinh.html).

**Syntax**

``` sql
asinh(x)
```

**Arguments**

- `x` — Hyperbolic sine of angle. Values from the interval: `-∞ < x < +∞`. [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Returned value**

- The angle, in radians. Values from the interval: `-∞ < asinh(x) < +∞`.

Type: [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Example**

``` sql
SELECT asinh(0);
```

Result:

```result
┌─asinh(0)─┐
│        0 │
└──────────┘
```
## tanh

Returns the [hyperbolic tangent](https://www.mathworks.com/help/matlab/ref/tanh.html).

**Syntax**

``` sql
tanh(x)
```

**Arguments**

- `x` — The angle, in radians. Values from the interval: `-∞ < x < +∞`. [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Returned value**

- Values from the interval: `-1 < tanh(x) < 1`.

Type: [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Example**

``` sql
SELECT tanh(0);
```

Result:

```result
0
```

## atanh

Returns the [inverse hyperbolic tangent](https://www.mathworks.com/help/matlab/ref/atanh.html).

**Syntax**

``` sql
atanh(x)
```

**Arguments**

- `x` — Hyperbolic tangent of angle. Values from the interval: `–1 < x < 1`. [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Returned value**

- The angle, in radians. Values from the interval: `-∞ < atanh(x) < +∞`.

Type: [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Example**

``` sql
SELECT atanh(0);
```

Result:

```result
┌─atanh(0)─┐
│        0 │
└──────────┘
```

## atan2

Returns the [atan2](https://en.wikipedia.org/wiki/Atan2) as the angle in the Euclidean plane, given in radians, between the positive x axis and the ray to the point `(x, y) ≠ (0, 0)`.

**Syntax**

``` sql
atan2(y, x)
```

**Arguments**

- `y` — y-coordinate of the point through which the ray passes. [Float64](../../sql-reference/data-types/float.md#float32-float64).
- `x` — x-coordinate of the point through which the ray passes. [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Returned value**

- The angle `θ` such that `−π < θ ≤ π`, in radians.

Type: [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Example**

``` sql
SELECT atan2(1, 1);
```

Result:

```result
┌────────atan2(1, 1)─┐
│ 0.7853981633974483 │
└────────────────────┘
```

## hypot

Returns the length of the hypotenuse of a right-angle triangle. [Hypot](https://en.wikipedia.org/wiki/Hypot) avoids problems that occur when squaring very large or very small numbers.

**Syntax**

``` sql
hypot(x, y)
```

**Arguments**

- `x` — The first cathetus of a right-angle triangle. [Float64](../../sql-reference/data-types/float.md#float32-float64).
- `y` — The second cathetus of a right-angle triangle. [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Returned value**

- The length of the hypotenuse of a right-angle triangle.

Type: [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Example**

``` sql
SELECT hypot(1, 1);
```

Result:

```result
┌────────hypot(1, 1)─┐
│ 1.4142135623730951 │
└────────────────────┘
```

## log1p

Calculates `log(1+x)`. The [calculation](https://en.wikipedia.org/wiki/Natural_logarithm#lnp1) `log1p(x)` is more accurate than `log(1+x)` for small values of x.

**Syntax**

``` sql
log1p(x)
```

**Arguments**

- `x` — Values from the interval: `-1 < x < +∞`. [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Returned value**

- Values from the interval: `-∞ < log1p(x) < +∞`.

Type: [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Example**

``` sql
SELECT log1p(0);
```

Result:

```result
┌─log1p(0)─┐
│        0 │
└──────────┘
```

## sign

Returns the sign of a real number.

**Syntax**

``` sql
sign(x)
```

**Arguments**

- `x` — Values from  `-∞` to `+∞`. Support all numeric types in ClickHouse.

**Returned value**

- -1 for `x < 0`
- 0 for `x = 0`
- 1 for `x > 0`

**Examples**

Sign for the zero value:

``` sql
SELECT sign(0);
```

Result:

```result
┌─sign(0)─┐
│       0 │
└─────────┘
```

Sign for the positive value:

``` sql
SELECT sign(1);
```

Result:

```result
┌─sign(1)─┐
│       1 │
└─────────┘
```

Sign for the negative value:

``` sql
SELECT sign(-1);
```

Result:

```result
┌─sign(-1)─┐
│       -1 │
└──────────┘
```

## degrees

Converts radians to degrees.

**Syntax**

``` sql
degrees(x)
```

**Arguments**

- `x` — Input in radians. [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Returned value**

- Value in degrees.

Type: [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Example**

``` sql
SELECT degrees(3.141592653589793);
```

Result:

```result
┌─degrees(3.141592653589793)─┐
│                        180 │
└────────────────────────────┘
```

## radians

Converts degrees to radians.

**Syntax**

``` sql
radians(x)
```

**Arguments**

- `x` — Input in degrees. [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Returned value**

- Value in radians.

Type: [Float64](../../sql-reference/data-types/float.md#float32-float64).

**Example**

``` sql
SELECT radians(180);
```

Result:

```result
┌──────radians(180)─┐
│ 3.141592653589793 │
└───────────────────┘
```

## factorial

Computes the factorial of an integer value. Works with any native integer type including UInt(8|16|32|64) and Int(8|16|32|64). The return type is UInt64.

The factorial of 0 is 1. Likewise, the factorial() function returns 1 for any negative value. The maximum positive value for the input argument is 20, a value of 21 or greater will cause exception throw.


**Syntax**

``` sql
factorial(n)
```

**Example**

``` sql
SELECT factorial(10);
```

Result:

```result
┌─factorial(10)─┐
│       3628800 │
└───────────────┘
```

## width_bucket

Returns the number of the bucket in which `operand` falls in a histogram having `count` equal-width buckets spanning the range `low` to `high`. Returns `0` if `operand < low`, and returns `count+1` if `operand >= high`.

`operand`, `low`, `high` can be any native number type. `count` can only be unsigned native integer and its value cannot be zero.

**Syntax**

```sql
widthBucket(operand, low, high, count)
```
Alias: `WIDTH_BUCKET`

**Example**

``` sql
SELECT widthBucket(10.15, -8.6, 23, 18);
```

Result:

```result
┌─widthBucket(10.15, -8.6, 23, 18)─┐
│                               11 │
└──────────────────────────────────┘
```
