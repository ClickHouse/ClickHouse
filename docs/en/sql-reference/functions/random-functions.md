---
slug: /en/sql-reference/functions/random-functions
sidebar_position: 51
sidebar_label: Pseudo-Random Numbers
---

# Functions for Generating Pseudo-Random Numbers

All the functions accept zero arguments or one argument. If an argument is passed, it can be any type, and its value is not used for anything. The only purpose of this argument is to prevent common subexpression elimination, so that two different instances of the same function return different columns with different random numbers.

:::note    
Non-cryptographic generators of pseudo-random numbers are used.
:::

## rand, rand32

Returns a pseudo-random UInt32 number, evenly distributed among all UInt32-type numbers.

Uses a linear congruential generator.

## rand64

Returns a pseudo-random UInt64 number, evenly distributed among all UInt64-type numbers.

Uses a linear congruential generator.

## randCanonical
The function generates pseudo random results with independent and identically distributed uniformly distributed values in [0, 1).

Non-deterministic. Return type is Float64.

## randConstant

Produces a constant column with a random value.

**Syntax**

``` sql
randConstant([x])
```

**Arguments**

-   `x` — [Expression](../../sql-reference/syntax.md#syntax-expressions) resulting in any of the [supported data types](../../sql-reference/data-types/index.md#data_types). The resulting value is discarded, but the expression itself if used for bypassing [common subexpression elimination](../../sql-reference/functions/index.md#common-subexpression-elimination) if the function is called multiple times in one query. Optional parameter.

**Returned value**

-   Pseudo-random number.

Type: [UInt32](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT rand(), rand(1), rand(number), randConstant(), randConstant(1), randConstant(number)
FROM numbers(3)
```

Result:

``` text
┌─────rand()─┬────rand(1)─┬─rand(number)─┬─randConstant()─┬─randConstant(1)─┬─randConstant(number)─┐
│ 3047369878 │ 4132449925 │   4044508545 │     2740811946 │      4229401477 │           1924032898 │
│ 2938880146 │ 1267722397 │   4154983056 │     2740811946 │      4229401477 │           1924032898 │
│  956619638 │ 4238287282 │   1104342490 │     2740811946 │      4229401477 │           1924032898 │
└────────────┴────────────┴──────────────┴────────────────┴─────────────────┴──────────────────────┘
```

# Functions for Generating Random Numbers based on Distributions

:::note    
These functions are available starting from 22.10.
:::



## randUniform

Return random number based on [continuous uniform distribution](https://en.wikipedia.org/wiki/Continuous_uniform_distribution) in a specified range from `min` to `max`.

**Syntax**

``` sql
randUniform(min, max)
```

**Arguments**

- `min` - `Float64` - min value of the range,
- `max` - `Float64` - max value of the range.

**Returned value**

- Pseudo-random number.

Type: [Float64](/docs/en/sql-reference/data-types/float.md).

**Example**

Query:

``` sql
SELECT randUniform(5.5, 10) FROM numbers(5)
```

Result:

``` text
┌─randUniform(5.5, 10)─┐
│    8.094978491443102 │
│   7.3181248914450885 │
│    7.177741903868262 │
│    6.483347380953762 │
│    6.122286382885112 │
└──────────────────────┘
```



## randNormal

Return random number based on [normal distribution](https://en.wikipedia.org/wiki/Normal_distribution).

**Syntax**

``` sql
randNormal(meam, variance)
```

**Arguments**

- `meam` - `Float64` mean value of distribution,
- `variance` - `Float64` - [variance](https://en.wikipedia.org/wiki/Variance).

**Returned value**

- Pseudo-random number.

Type: [Float64](/docs/en/sql-reference/data-types/float.md).

**Example**

Query:

``` sql
SELECT randNormal(10, 2) FROM numbers(5)
```

Result:

``` text
┌──randNormal(10, 2)─┐
│ 13.389228911709653 │
│  8.622949707401295 │
│ 10.801887062682981 │
│ 4.5220192605895315 │
│ 10.901239123982567 │
└────────────────────┘
```



## randLogNormal

Return random number based on [log-normal distribution](https://en.wikipedia.org/wiki/Log-normal_distribution).

**Syntax**

``` sql
randLogNormal(meam, variance)
```

**Arguments**

- `meam` - `Float64` mean value of distribution,
- `variance` - `Float64` - [variance](https://en.wikipedia.org/wiki/Variance).

**Returned value**

- Pseudo-random number.

Type: [Float64](/docs/en/sql-reference/data-types/float.md).

**Example**

Query:

``` sql
SELECT randLogNormal(100, 5) FROM numbers(5)
```

Result:

``` text
┌─randLogNormal(100, 5)─┐
│  1.295699673937363e48 │
│  9.719869109186684e39 │
│  6.110868203189557e42 │
│  9.912675872925529e39 │
│ 2.3564708490552458e42 │
└───────────────────────┘
```



## randBinomial

Return random number based on [binomial distribution](https://en.wikipedia.org/wiki/Binomial_distribution).

**Syntax**

``` sql
randBinomial(experiments, probability)
```

**Arguments**

- `experiments` - `UInt64` number of experiments,
- `probability` - `Float64` - probability of success in each experiment (values in `0...1` range only).

**Returned value**

- Pseudo-random number.

Type: [UInt64](/docs/en/sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT randBinomial(100, .75) FROM numbers(5)
```

Result:

``` text
┌─randBinomial(100, 0.75)─┐
│                      74 │
│                      78 │
│                      76 │
│                      77 │
│                      80 │
└─────────────────────────┘
```



## randNegativeBinomial

Return random number based on [negative binomial distribution](https://en.wikipedia.org/wiki/Negative_binomial_distribution).

**Syntax**

``` sql
randNegativeBinomial(experiments, probability)
```

**Arguments**

- `experiments` - `UInt64` number of experiments,
- `probability` - `Float64` - probability of failure in each experiment (values in `0...1` range only).

**Returned value**

- Pseudo-random number.

Type: [UInt64](/docs/en/sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT randNegativeBinomial(100, .75) FROM numbers(5)
```

Result:

``` text
┌─randNegativeBinomial(100, 0.75)─┐
│                              33 │
│                              32 │
│                              39 │
│                              40 │
│                              50 │
└─────────────────────────────────┘
```



## randPoisson

Return random number based on [Poisson distribution](https://en.wikipedia.org/wiki/Poisson_distribution).

**Syntax**

``` sql
randPoisson(n)
```

**Arguments**

- `n` - `UInt64` mean number of occurrences.

**Returned value**

- Pseudo-random number.

Type: [UInt64](/docs/en/sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT randPoisson(10) FROM numbers(5)
```

Result:

``` text
┌─randPoisson(10)─┐
│               8 │
│               8 │
│               7 │
│              10 │
│               6 │
└─────────────────┘
```



## randBernoulli

Return random number based on [Bernoulli distribution](https://en.wikipedia.org/wiki/Bernoulli_distribution).

**Syntax**

``` sql
randBernoulli(probability)
```

**Arguments**

- `probability` - `Float64` - probability of success (values in `0...1` range only).

**Returned value**

- Pseudo-random number.

Type: [UInt64](/docs/en/sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT randBernoulli(.75) FROM numbers(5)
```

Result:

``` text
┌─randBernoulli(0.75)─┐
│                   1 │
│                   1 │
│                   0 │
│                   1 │
│                   1 │
└─────────────────────┘
```



## randExponential

Return random number based on [exponential distribution](https://en.wikipedia.org/wiki/Exponential_distribution).

**Syntax**

``` sql
randExponential(lambda)
```

**Arguments**

- `lambda` - `Float64` lambda value.

**Returned value**

- Pseudo-random number.

Type: [Float64](/docs/en/sql-reference/data-types/float.md).

**Example**

Query:

``` sql
SELECT randExponential(1/10) FROM numbers(5)
```

Result:

``` text
┌─randExponential(divide(1, 10))─┐
│              44.71628934340778 │
│              4.211013337903262 │
│             10.809402553207766 │
│              15.63959406553284 │
│             1.8148392319860158 │
└────────────────────────────────┘
```



## randChiSquared

Return random number based on [Chi-square distribution](https://en.wikipedia.org/wiki/Chi-squared_distribution) - a distribution of a sum of the squares of k independent standard normal random variables.

**Syntax**

``` sql
randChiSquared(degree_of_freedom)
```

**Arguments**

- `degree_of_freedom` - `Float64` degree of freedom.

**Returned value**

- Pseudo-random number.

Type: [Float64](/docs/en/sql-reference/data-types/float.md).

**Example**

Query:

``` sql
SELECT randChiSquared(10) FROM numbers(5)
```

Result:

``` text
┌─randChiSquared(10)─┐
│ 10.015463656521543 │
│  9.621799919882768 │
│   2.71785015634699 │
│ 11.128188665931908 │
│  4.902063104425469 │
└────────────────────┘
```



## randStudentT

Return random number based on [Student's t-distribution](https://en.wikipedia.org/wiki/Student%27s_t-distribution).

**Syntax**

``` sql
randStudentT(degree_of_freedom)
```

**Arguments**

- `degree_of_freedom` - `Float64` degree of freedom.

**Returned value**

- Pseudo-random number.

Type: [Float64](/docs/en/sql-reference/data-types/float.md).

**Example**

Query:

``` sql
SELECT randStudentT(10) FROM numbers(5)
```

Result:

``` text
┌─────randStudentT(10)─┐
│   1.2217309938538725 │
│   1.7941971681200541 │
│ -0.28192176076784664 │
│   0.2508897721303792 │
│  -2.7858432909761186 │
└──────────────────────┘
```



## randFisherF

Return random number based on [F-distribution](https://en.wikipedia.org/wiki/F-distribution).

**Syntax**

``` sql
randFisherF(d1, d2)
```

**Arguments**

- `d1` - `Float64` d1 degree of freedom in `X = (S1 / d1) / (S2 / d2)`,
- `d2` - `Float64` d2 degree of freedom in `X = (S1 / d1) / (S2 / d2)`,

**Returned value**

- Pseudo-random number.

Type: [Float64](/docs/en/sql-reference/data-types/float.md).

**Example**

Query:

``` sql
SELECT randFisherF(10, 3) FROM numbers(5)
```

Result:

``` text
┌──randFisherF(10, 3)─┐
│   7.286287504216609 │
│ 0.26590779413050386 │
│ 0.22207610901168987 │
│  0.7953362728449572 │
│ 0.19278885985221572 │
└─────────────────────┘
```




# Random Functions for Working with Strings

## randomString

## randomFixedString

## randomPrintableASCII

## randomStringUTF8

## fuzzBits

**Syntax**

``` sql
fuzzBits([s], [prob])
```

Inverts bits of `s`, each with probability `prob`.

**Arguments**
- `s` - `String` or `FixedString`
- `prob` - constant `Float32/64`

**Returned value**
Fuzzed string with same as s type.

**Example**

``` sql
SELECT fuzzBits(materialize('abacaba'), 0.1)
FROM numbers(3)
```

Result:

``` text
┌─fuzzBits(materialize('abacaba'), 0.1)─┐
│ abaaaja                               │
│ a*cjab+                               │
│ aeca2A                                │
└───────────────────────────────────────┘
```

## Related content
- Blog: [Generating random data in ClickHouse](https://clickhouse.com/blog/generating-random-test-distribution-data-for-clickhouse)
