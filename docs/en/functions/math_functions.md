# Mathematical functions

All the functions return a Float64 number. The accuracy of the result is close to the maximum precision possible, but the result might not coincide with the machine representable number nearest to the corresponding real number.

## e()

Returns a Float64 number close to the e number.

## pi()

Returns a Float64 number close to π.

## exp(x)

Accepts a numeric argument and returns a Float64 number close to the exponent of the argument.

## log(x)

Accepts a numeric argument and returns a Float64 number close to the natural logarithm of the argument.

## exp2(x)

Accepts a numeric argument and returns a Float64 number close to 2^x.

## log2(x)

Accepts a numeric argument and returns a Float64 number close to the binary logarithm of the argument.

## exp10(x)

Accepts a numeric argument and returns a Float64 number close to 10^x.

## log10(x)

Accepts a numeric argument and returns a Float64 number close to the decimal logarithm of the argument.

## sqrt(x)

Accepts a numeric argument and returns a Float64 number close to the square root of the argument.

## cbrt(x)

Accepts a numeric argument and returns a Float64 number close to the cubic root of the argument.

## erf(x)

If 'x' is non-negative, then erf(x / σ√2)<g> is the probability that a random variable having a normal distribution with standard deviation 'σ' takes the value that is separated from the expected value by more than 'x'.

Example (three sigma rule):

```sql
SELECT erf(3 / sqrt(2))
```

```text
┌─erf(divide(3, sqrt(2)))─┐
│      0.9973002039367398 │
└─────────────────────────┘
```

## erfc(x)

Accepts a numeric argument and returns a Float64 number close to 1 - erf(x), but without loss of precision for large 'x' values.

## lgamma(x)

The logarithm of the gamma function.

## tgamma(x)

Gamma function.

## sin(x)

The sine.

## cos(x)

The cosine.

## tan(x)

The tangent.

## asin(x)

The arc sine.

## acos(x)

The arc cosine.

## atan(x)

The arc tangent.

## pow(x, y)

Accepts two numeric arguments and returns a Float64 number close to x^y.
