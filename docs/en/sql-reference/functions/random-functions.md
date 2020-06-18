---
toc_priority: 51
toc_title: Generating Pseudo-Random Numbers
---

# Functions for Generating Pseudo-random Numbers {#functions-for-generating-pseudo-random-numbers}

Non-cryptographic generators of pseudo-random numbers are used.

All the functions accept zero arguments or one argument.
If an argument is passed, it can be any type, and its value is not used for anything.
The only purpose of this argument is to prevent common subexpression elimination, so that two different instances of the same function return different columns with different random numbers.

## rand, rand32 {#rand}

Returns a pseudo-random UInt32 number, evenly distributed among all UInt32-type numbers.
Uses a linear congruential generator.

## rand64 {#rand64}

Returns a pseudo-random UInt64 number, evenly distributed among all UInt64-type numbers.
Uses a linear congruential generator.

## randConstant {#randconstant}

Produces a constant column with a random value.

**Syntax**

``` sql
randConstant([x])
```

**Parameters**

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

# Random Functions for Working with Strings {#random-functions-for-working-with-strings}

## randomString {#random-string}

## randomFixedString {#random-fixed-string}

## randomPrintableASCII {#random-printable-ascii}

## randomStringUTF8 {#random-string-utf8}

## fuzzBits {#fuzzbits}

**Syntax**

``` sql
fuzzBits([s], [prob])
```

Inverts bits of `s`, each with probability `prob`.

**Parameters**
- `s` - `String` or `FixedString`
- `prob` - constant `Float32/64`

**Returned value**
Fuzzed string with same as s type.

**Example**

``` sql
SELECT fuzzBits(materialize('abacaba'), 0.1)
FROM numbers(3)
```

\`\`\` text
┌─fuzzBits(materialize(‘abacaba’), 0.1)─┐
│ abaaaja │
│ a\*cjab+ │
│ aeca2A │
└───────────────────────────────────────┘

[Original article](https://clickhouse.tech/docs/en/query_language/functions/random_functions/) <!--hide-->
