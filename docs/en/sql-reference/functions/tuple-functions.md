---
sidebar_position: 66
sidebar_label: Tuples
---

# Functions for Working with Tuples

## tuple

A function that allows grouping multiple columns.
For columns with the types T1, T2, …, it returns a Tuple(T1, T2, …) type tuple containing these columns. There is no cost to execute the function.
Tuples are normally used as intermediate values for an argument of IN operators, or for creating a list of formal parameters of lambda functions. Tuples can’t be written to a table.

The function implements the operator `(x, y, …)`.

**Syntax**

``` sql
tuple(x, y, …)
```

## tupleElement

A function that allows getting a column from a tuple.
‘N’ is the column index, starting from 1. ‘N’ must be a constant. ‘N’ must be a strict postive integer no greater than the size of the tuple.
There is no cost to execute the function.

The function implements the operator `x.N`.

**Syntax**

``` sql
tupleElement(tuple, n)
```

## untuple

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

## tupleHammingDistance

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

## tupleToNameValuePairs

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

## tuplePlus

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

## tupleMinus

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

## tupleMultiply

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

## tupleDivide

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

## tupleNegate

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

## tupleMultiplyByNumber

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

## tupleDivideByNumber

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

## dotProduct

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


## Distance functions

All supported functions are described in [distance functions documentation](../../sql-reference/functions/distance-functions.md).
