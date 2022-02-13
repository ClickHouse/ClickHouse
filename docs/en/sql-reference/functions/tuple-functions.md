---
toc_priority: 66
toc_title: Tuples
---

# Functions for Working with Tuples {#tuple-functions}

## tuple {#tuple}

A function that allows grouping multiple columns.
For columns with the types T1, T2, …, it returns a Tuple(T1, T2, …) type tuple containing these columns. There is no cost to execute the function.
Tuples are normally used as intermediate values for an argument of IN operators, or for creating a list of formal parameters of lambda functions. Tuples can’t be written to a table.

The function implements the operator `(x, y, …)`.

**Syntax**

``` sql
tuple(x, y, …)
```

## tupleElement {#tupleelement}

A function that allows getting a column from a tuple.
‘N’ is the column index, starting from 1. ‘N’ must be a constant. ‘N’ must be a strict postive integer no greater than the size of the tuple.
There is no cost to execute the function.

The function implements the operator `x.N`.

**Syntax**

``` sql
tupleElement(tuple, n)
```

## untuple {#untuple}

Performs syntactic substitution of [tuple](../../sql-reference/data-types/tuple.md#tuplet1-t2) elements in the call location.

**Syntax**

``` sql
untuple(x)
```

You can use the `EXCEPT` expression to skip columns as a result of the query.

**Arguments**

-   `x` — A `tuple` function, column, or tuple of elements. [Tuple](../../sql-reference/data-types/tuple.md).

**Returned value**

-   None.

**Examples**

Input table:

``` text
┌─key─┬─v1─┬─v2─┬─v3─┬─v4─┬─v5─┬─v6────────┐
│   1 │ 10 │ 20 │ 40 │ 30 │ 15 │ (33,'ab') │
│   2 │ 25 │ 65 │ 70 │ 40 │  6 │ (44,'cd') │
│   3 │ 57 │ 30 │ 20 │ 10 │  5 │ (55,'ef') │
│   4 │ 55 │ 12 │  7 │ 80 │ 90 │ (66,'gh') │
│   5 │ 30 │ 50 │ 70 │ 25 │ 55 │ (77,'kl') │
└─────┴────┴────┴────┴────┴────┴───────────┘
```

Example of using a `Tuple`-type column as the `untuple` function parameter:

Query:

``` sql
SELECT untuple(v6) FROM kv;
```

Result:

``` text
┌─_ut_1─┬─_ut_2─┐
│    33 │ ab    │
│    44 │ cd    │
│    55 │ ef    │
│    66 │ gh    │
│    77 │ kl    │
└───────┴───────┘
```

Note: the names are implementation specific and are subject to change. You should not assume specific names of the columns after application of the `untuple`.

Example of using an `EXCEPT` expression:

Query:

``` sql
SELECT untuple((* EXCEPT (v2, v3),)) FROM kv;
```

Result:

``` text
┌─key─┬─v1─┬─v4─┬─v5─┬─v6────────┐
│   1 │ 10 │ 30 │ 15 │ (33,'ab') │
│   2 │ 25 │ 40 │  6 │ (44,'cd') │
│   3 │ 57 │ 10 │  5 │ (55,'ef') │
│   4 │ 55 │ 80 │ 90 │ (66,'gh') │
│   5 │ 30 │ 25 │ 55 │ (77,'kl') │
└─────┴────┴────┴────┴───────────┘
```

**See Also**

-   [Tuple](../../sql-reference/data-types/tuple.md)

## tupleHammingDistance {#tuplehammingdistance}

Returns the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) between two tuples of the same size.

**Syntax**

``` sql
tupleHammingDistance(tuple1, tuple2)
```

**Arguments**

-   `tuple1` — First tuple. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple2` — Second tuple. [Tuple](../../sql-reference/data-types/tuple.md).

Tuples should have the same type of the elements.

**Returned value**

-   The Hamming distance.

Type: The result type is calculed the same way it is for [Arithmetic functions](../../sql-reference/functions/arithmetic-functions.md), based on the number of elements in the input tuples.

``` sql
SELECT
    toTypeName(tupleHammingDistance(tuple(0), tuple(0))) AS t1,
    toTypeName(tupleHammingDistance((0, 0), (0, 0))) AS t2,
    toTypeName(tupleHammingDistance((0, 0, 0), (0, 0, 0))) AS t3,
    toTypeName(tupleHammingDistance((0, 0, 0, 0), (0, 0, 0, 0))) AS t4,
    toTypeName(tupleHammingDistance((0, 0, 0, 0, 0), (0, 0, 0, 0, 0))) AS t5
```

``` text
┌─t1────┬─t2─────┬─t3─────┬─t4─────┬─t5─────┐
│ UInt8 │ UInt16 │ UInt32 │ UInt64 │ UInt64 │
└───────┴────────┴────────┴────────┴────────┘
```


**Examples**

Query:

``` sql
SELECT tupleHammingDistance((1, 2, 3), (3, 2, 1)) AS HammingDistance;
```

Result:

``` text
┌─HammingDistance─┐
│               2 │
└─────────────────┘
```

Can be used with [MinHash](../../sql-reference/functions/hash-functions.md#ngramminhash) functions for detection of semi-duplicate strings:

``` sql
SELECT tupleHammingDistance(wordShingleMinHash(string), wordShingleMinHashCaseInsensitive(string)) as HammingDistance FROM (SELECT 'ClickHouse is a column-oriented database management system for online analytical processing of queries.' AS string);
```

Result:

``` text
┌─HammingDistance─┐
│               2 │
└─────────────────┘
```

## tupleToNameValuePairs {#tupletonamevaluepairs}

Turns a named tuple into an array of (name, value) pairs. For a `Tuple(a T, b T, ..., c T)` returns `Array(Tuple(String, T), ...)`
in which the `Strings` represents the named fields of the tuple and `T` are the values associated with those names. All values in the tuple should be of the same type.

**Syntax**

``` sql
tupleToNameValuePairs(tuple)
```

**Arguments**

-   `tuple` — Named tuple. [Tuple](../../sql-reference/data-types/tuple.md) with any types of values.

**Returned value**

-   An array with (name, value) pairs.

Type: [Array](../../sql-reference/data-types/array.md)([Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md), ...)).

**Example**

Query:

``` sql
CREATE TABLE tupletest (`col` Tuple(user_ID UInt64, session_ID UInt64) ENGINE = Memory;

INSERT INTO tupletest VALUES (tuple( 100, 2502)), (tuple(1,100));

SELECT tupleToNameValuePairs(col) FROM tupletest;
```

Result:

``` text
┌─tupleToNameValuePairs(col)────────────┐
│ [('user_ID',100),('session_ID',2502)] │
│ [('user_ID',1),('session_ID',100)]    │
└───────────────────────────────────────┘
```

It is possible to transform colums to rows using this function:

``` sql
CREATE TABLE tupletest (`col` Tuple(CPU Float64, Memory Float64, Disk Float64)) ENGINE = Memory;

INSERT INTO tupletest VALUES(tuple(3.3, 5.5, 6.6));

SELECT arrayJoin(tupleToNameValuePairs(col))FROM tupletest;
```

Result:

``` text
┌─arrayJoin(tupleToNameValuePairs(col))─┐
│ ('CPU',3.3)                           │
│ ('Memory',5.5)                        │
│ ('Disk',6.6)                          │
└───────────────────────────────────────┘
```

If you pass a simple tuple to the function, ClickHouse uses the indexes of the values as their names:

``` sql
SELECT tupleToNameValuePairs(tuple(3, 2, 1));
```

Result:

``` text
┌─tupleToNameValuePairs(tuple(3, 2, 1))─┐
│ [('1',3),('2',2),('3',1)]             │
└───────────────────────────────────────┘
```

## tuplePlus {#tupleplus}

Calculates the sum of corresponding values of two tuples of the same size.

**Syntax**

```sql
tuplePlus(tuple1, tuple2)
```

Alias: `vectorSum`.

**Arguments**

-   `tuple1` — First tuple. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple2` — Second tuple. [Tuple](../../sql-reference/data-types/tuple.md).

**Returned value**

-   Tuple with the sum.

Type: [Tuple](../../sql-reference/data-types/tuple.md).

**Example**

Query:

```sql
SELECT tuplePlus((1, 2), (2, 3));
```

Result:

```text
┌─tuplePlus((1, 2), (2, 3))─┐
│ (3,5)                     │
└───────────────────────────┘
```

## tupleMinus {#tupleminus}

Calculates the subtraction of corresponding values of two tuples of the same size.

**Syntax**

```sql
tupleMinus(tuple1, tuple2)
```

Alias: `vectorDifference`.

**Arguments**

-   `tuple1` — First tuple. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple2` — Second tuple. [Tuple](../../sql-reference/data-types/tuple.md).

**Returned value**

-   Tuple with the result of subtraction.

Type: [Tuple](../../sql-reference/data-types/tuple.md).

**Example**

Query:

```sql
SELECT tupleMinus((1, 2), (2, 3));
```

Result:

```text
┌─tupleMinus((1, 2), (2, 3))─┐
│ (-1,-1)                    │
└────────────────────────────┘
```

## tupleMultiply {#tuplemultiply}

Calculates the multiplication of corresponding values of two tuples of the same size.

**Syntax**

```sql
tupleMultiply(tuple1, tuple2)
```

**Arguments**

-   `tuple1` — First tuple. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple2` — Second tuple. [Tuple](../../sql-reference/data-types/tuple.md).

**Returned value**

-   Tuple with the multiplication.

Type: [Tuple](../../sql-reference/data-types/tuple.md).

**Example**

Query:

```sql
SELECT tupleMultiply((1, 2), (2, 3));
```

Result:

```text
┌─tupleMultiply((1, 2), (2, 3))─┐
│ (2,6)                         │
└───────────────────────────────┘
```

## tupleDivide {#tupledivide}

Calculates the division of corresponding values of two tuples of the same size. Note that division by zero will return `inf`.

**Syntax**

```sql
tupleDivide(tuple1, tuple2)
```

**Arguments**

-   `tuple1` — First tuple. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple2` — Second tuple. [Tuple](../../sql-reference/data-types/tuple.md).

**Returned value**

-   Tuple with the result of division.

Type: [Tuple](../../sql-reference/data-types/tuple.md).

**Example**

Query:

```sql
SELECT tupleDivide((1, 2), (2, 3));
```

Result:

```text
┌─tupleDivide((1, 2), (2, 3))─┐
│ (0.5,0.6666666666666666)    │
└─────────────────────────────┘
```

## tupleNegate {#tuplenegate}

Calculates the negation of the tuple values.

**Syntax**

```sql
tupleNegate(tuple)
```

**Arguments**

-   `tuple` — [Tuple](../../sql-reference/data-types/tuple.md).

**Returned value**

-   Tuple with the result of negation.

Type: [Tuple](../../sql-reference/data-types/tuple.md).

**Example**

Query:

```sql
SELECT tupleNegate((1,  2));
```

Result:

```text
┌─tupleNegate((1, 2))─┐
│ (-1,-2)             │
└─────────────────────┘
```

## tupleMultiplyByNumber {#tuplemultiplybynumber}

Returns a tuple with all values multiplied by a number.

**Syntax**

```sql
tupleMultiplyByNumber(tuple, number)
```

**Arguments**

-   `tuple` — [Tuple](../../sql-reference/data-types/tuple.md).
-   `number` — Multiplier. [Int/UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).

**Returned value**

-   Tuple with multiplied values.

Type: [Tuple](../../sql-reference/data-types/tuple.md).

**Example**

Query:

```sql
SELECT tupleMultiplyByNumber((1, 2), -2.1);
```

Result:

```text
┌─tupleMultiplyByNumber((1, 2), -2.1)─┐
│ (-2.1,-4.2)                         │
└─────────────────────────────────────┘
```

## tupleDivideByNumber {#tupledividebynumber}

Returns a tuple with all values divided by a number. Note that division by zero will return `inf`.

**Syntax**

```sql
tupleDivideByNumber(tuple, number)
```

**Arguments**

-   `tuple` — [Tuple](../../sql-reference/data-types/tuple.md).
-   `number` — Divider. [Int/UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).

**Returned value**

-   Tuple with divided values.

Type: [Tuple](../../sql-reference/data-types/tuple.md).

**Example**

Query:

```sql
SELECT tupleDivideByNumber((1, 2), 0.5);
```

Result:

```text
┌─tupleDivideByNumber((1, 2), 0.5)─┐
│ (2,4)                            │
└──────────────────────────────────┘
```

## dotProduct {#dotproduct}

Calculates the scalar product of two tuples of the same size.

**Syntax**

```sql
dotProduct(tuple1, tuple2)
```

Alias: `scalarProduct`.

**Arguments**

-   `tuple1` — First tuple. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple2` — Second tuple. [Tuple](../../sql-reference/data-types/tuple.md).

**Returned value**

-   Scalar product.

Type: [Int/UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).

**Example**

Query:

```sql
SELECT dotProduct((1, 2), (2, 3));
```

Result:

```text
┌─dotProduct((1, 2), (2, 3))─┐
│                          8 │
└────────────────────────────┘
```

## L1Norm {#l1norm}

Calculates the sum of absolute values of a tuple.

**Syntax**

```sql
L1Norm(tuple)
```

Alias: `normL1`.

**Arguments**

-   `tuple` — [Tuple](../../sql-reference/data-types/tuple.md).

**Returned value**

-   L1-norm or [taxicab geometry](https://en.wikipedia.org/wiki/Taxicab_geometry) distance.

Type: [UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).

**Example**

Query:

```sql
SELECT L1Norm((1, 2));
```

Result:

```text
┌─L1Norm((1, 2))─┐
│              3 │
└────────────────┘
```

## L2Norm {#l2norm}

Calculates the square root of the sum of the squares of the tuple values.

**Syntax**

```sql
L2Norm(tuple)
```

Alias: `normL2`.

**Arguments**

-   `tuple` — [Tuple](../../sql-reference/data-types/tuple.md).

**Returned value**

-   L2-norm or [Euclidean distance](https://en.wikipedia.org/wiki/Euclidean_distance).

Type: [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT L2Norm((1, 2));
```

Result:

```text
┌───L2Norm((1, 2))─┐
│ 2.23606797749979 │
└──────────────────┘
```

## LinfNorm {#linfnorm}

Calculates the maximum of absolute values of a tuple.

**Syntax**

```sql
LinfNorm(tuple)
```

Alias: `normLinf`.

**Arguments**

-   `tuple` — [Tuple](../../sql-reference/data-types/tuple.md).

**Returned value**

-   Linf-norm or the maximum absolute value.

Type: [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT LinfNorm((1, -2));
```

Result:

```text
┌─LinfNorm((1, -2))─┐
│                 2 │
└───────────────────┘
```

## LpNorm {#lpnorm}

Calculates the root of `p`-th power of the sum of the absolute values of a tuple in the power of `p`.

**Syntax**

```sql
LpNorm(tuple, p)
```

Alias: `normLp`.

**Arguments**

-   `tuple` — [Tuple](../../sql-reference/data-types/tuple.md).
-   `p` — The power. Possible values: real number in `[1; inf)`. [UInt](../../sql-reference/data-types/int-uint.md) or [Float](../../sql-reference/data-types/float.md).

**Returned value**

-   [Lp-norm](https://en.wikipedia.org/wiki/Norm_(mathematics)#p-norm)

Type: [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT LpNorm((1, -2), 2);
```

Result:

```text
┌─LpNorm((1, -2), 2)─┐
│   2.23606797749979 │
└────────────────────┘
```

## L1Distance {#l1distance}

Calculates the distance between two points (the values of the tuples are the coordinates) in `L1` space (1-norm ([taxicab geometry](https://en.wikipedia.org/wiki/Taxicab_geometry) distance)).

**Syntax**

```sql
L1Distance(tuple1, tuple2)
```

Alias: `distanceL1`.

**Arguments**

-   `tuple1` — First tuple. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple1` — Second tuple. [Tuple](../../sql-reference/data-types/tuple.md).

**Returned value**

-   1-norm distance.

Type: [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT L1Distance((1, 2), (2, 3));
```

Result:

```text
┌─L1Distance((1, 2), (2, 3))─┐
│                          2 │
└────────────────────────────┘
```

## L2Distance {#l2distance}

Calculates the distance between two points (the values of the tuples are the coordinates) in Euclidean space ([Euclidean distance](https://en.wikipedia.org/wiki/Euclidean_distance)).

**Syntax**

```sql
L2Distance(tuple1, tuple2)
```

Alias: `distanceL2`.

**Arguments**

-   `tuple1` — First tuple. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple1` — Second tuple. [Tuple](../../sql-reference/data-types/tuple.md).

**Returned value**

-   2-norm distance.

Type: [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT L2Distance((1, 2), (2, 3));
```

Result:

```text
┌─L2Distance((1, 2), (2, 3))─┐
│         1.4142135623730951 │
└────────────────────────────┘
```

## LinfDistance {#linfdistance}

Calculates the distance between two points (the values of the tuples are the coordinates) in `L_{inf}` space ([maximum norm](https://en.wikipedia.org/wiki/Norm_(mathematics)#Maximum_norm_(special_case_of:_infinity_norm,_uniform_norm,_or_supremum_norm))).

**Syntax**

```sql
LinfDistance(tuple1, tuple2)
```

Alias: `distanceLinf`.

**Arguments**

-   `tuple1` — First tuple. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple1` — Second tuple. [Tuple](../../sql-reference/data-types/tuple.md).

**Returned value**

-   Infinity-norm distance.

Type: [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT LinfDistance((1, 2), (2, 3));
```

Result:

```text
┌─LinfDistance((1, 2), (2, 3))─┐
│                            1 │
└──────────────────────────────┘
```

## LpDistance {#lpdistance}

Calculates the distance between two points (the values of the tuples are the coordinates) in `Lp` space ([p-norm distance](https://en.wikipedia.org/wiki/Norm_(mathematics)#p-norm)).

**Syntax**

```sql
LpDistance(tuple1, tuple2, p)
```

Alias: `distanceLp`.

**Arguments**

-   `tuple1` — First tuple. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple1` — Second tuple. [Tuple](../../sql-reference/data-types/tuple.md).
-   `p` — The power. Possible values: real number from `[1; inf)`. [UInt](../../sql-reference/data-types/int-uint.md) or [Float](../../sql-reference/data-types/float.md).

**Returned value**

-   p-norm distance.

Type: [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT LpDistance((1, 2), (2, 3), 3);
```

Result:

```text
┌─LpDistance((1, 2), (2, 3), 3)─┐
│            1.2599210498948732 │
└───────────────────────────────┘
```

## L1Normalize {#l1normalize}

Calculates the unit vector of a given vector (the values of the tuple are the coordinates) in `L1` space ([taxicab geometry](https://en.wikipedia.org/wiki/Taxicab_geometry)).

**Syntax**

```sql
L1Normalize(tuple)
```

Alias: `normalizeL1`.

**Arguments**

-   `tuple` — [Tuple](../../sql-reference/data-types/tuple.md).

**Returned value**

-   Unit vector.

Type: [Tuple](../../sql-reference/data-types/tuple.md) of [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT L1Normalize((1, 2));
```

Result:

```text
┌─L1Normalize((1, 2))─────────────────────┐
│ (0.3333333333333333,0.6666666666666666) │
└─────────────────────────────────────────┘
```

## L2Normalize {#l2normalize}

Calculates the unit vector of a given vector (the values of the tuple are the coordinates) in Euclidean space (using [Euclidean distance](https://en.wikipedia.org/wiki/Euclidean_distance)).

**Syntax**

```sql
L2Normalize(tuple)
```

Alias: `normalizeL1`.

**Arguments**

-   `tuple` — [Tuple](../../sql-reference/data-types/tuple.md).

**Returned value**

-   Unit vector.

Type: [Tuple](../../sql-reference/data-types/tuple.md) of [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT L2Normalize((3, 4));
```

Result:

```text
┌─L2Normalize((3, 4))─┐
│ (0.6,0.8)           │
└─────────────────────┘
```

## LinfNormalize {#linfnormalize}

Calculates the unit vector of a given vector (the values of the tuple are the coordinates) in `L_{inf}` space (using [maximum norm](https://en.wikipedia.org/wiki/Norm_(mathematics)#Maximum_norm_(special_case_of:_infinity_norm,_uniform_norm,_or_supremum_norm))).

**Syntax**

```sql
LinfNormalize(tuple)
```

Alias: `normalizeLinf `.

**Arguments**

-   `tuple` — [Tuple](../../sql-reference/data-types/tuple.md).

**Returned value**

-   Unit vector.

Type: [Tuple](../../sql-reference/data-types/tuple.md) of [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT LinfNormalize((3, 4));
```

Result:

```text
┌─LinfNormalize((3, 4))─┐
│ (0.75,1)              │
└───────────────────────┘
```

## LpNormalize {#lpnormalize}

Calculates the unit vector of a given vector (the values of the tuple are the coordinates) in `Lp` space (using [p-norm](https://en.wikipedia.org/wiki/Norm_(mathematics)#p-norm)).

**Syntax**

```sql
LpNormalize(tuple, p)
```

Alias: `normalizeLp `.

**Arguments**

-   `tuple` — [Tuple](../../sql-reference/data-types/tuple.md).
-   `p` — The power. Possible values: any number from [1;inf). [UInt](../../sql-reference/data-types/int-uint.md) or [Float](../../sql-reference/data-types/float.md).

**Returned value**

-   Unit vector.

Type: [Tuple](../../sql-reference/data-types/tuple.md) of [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT LpNormalize((3, 4),5);
```

Result:

```text
┌─LpNormalize((3, 4), 5)──────────────────┐
│ (0.7187302630182624,0.9583070173576831) │
└─────────────────────────────────────────┘
```

## cosineDistance {#cosinedistance}

Calculates the cosine distance between two vectors (the values of the tuples are the coordinates). The less the returned value is, the more similar are the vectors.

**Syntax**

```sql
cosineDistance(tuple1, tuple2)
```

**Arguments**

-   `tuple1` — First tuple. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple2` — Second tuple. [Tuple](../../sql-reference/data-types/tuple.md).

**Returned value**

-   Cosine of the angle between two vectors substracted from one.

Type: [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT cosineDistance((1, 2), (2, 3));
```

Result:

```text
┌─cosineDistance((1, 2), (2, 3))─┐
│           0.007722123286332261 │
└────────────────────────────────┘
```
