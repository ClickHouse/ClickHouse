---
slug: /en/sql-reference/functions/tuple-functions
sidebar_position: 180
sidebar_label: Tuples
---

## tuple

A function that allows grouping multiple columns.
For columns C1, C2, ... with the types T1, T2, ..., it returns a named Tuple(C1 T1, C2 T2, ...) type tuple containing these columns if their names are unique and can be treated as unquoted identifiers, otherwise a Tuple(T1, T2, ...) is returned. There is no cost to execute the function.
Tuples are normally used as intermediate values for an argument of IN operators, or for creating a list of formal parameters of lambda functions. Tuples can’t be written to a table.

The function implements the operator `(x, y, ...)`.

**Syntax**

``` sql
tuple(x, y, ...)
```

## tupleElement

A function that allows getting a column from a tuple.

If the second argument is a number `index`, it is the column index, starting from 1. If the second argument is a string `name`, it represents the name of the element. Besides, we can provide the third optional argument, such that when index out of bounds or no element exist for the name, the default value returned instead of throwing an exception. The second and third arguments, if provided, must be constants. There is no cost to execute the function.

The function implements operators `x.index` and `x.name`.

**Syntax**

``` sql
tupleElement(tuple, index, [, default_value])
tupleElement(tuple, name, [, default_value])
```

## untuple

Performs syntactic substitution of [tuple](../data-types/tuple.md#tuplet1-t2) elements in the call location.

The names of the result columns are implementation-specific and subject to change. Do not assume specific column names after `untuple`.

**Syntax**

``` sql
untuple(x)
```

You can use the `EXCEPT` expression to skip columns as a result of the query.

**Arguments**

- `x` — A `tuple` function, column, or tuple of elements. [Tuple](../data-types/tuple.md).

**Returned value**

- None.

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

- [Tuple](../data-types/tuple.md)

## tupleHammingDistance

Returns the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) between two tuples of the same size.

**Syntax**

``` sql
tupleHammingDistance(tuple1, tuple2)
```

**Arguments**

- `tuple1` — First tuple. [Tuple](../data-types/tuple.md).
- `tuple2` — Second tuple. [Tuple](../data-types/tuple.md).

Tuples should have the same type of the elements.

**Returned value**

- The Hamming distance.

:::note
The result type is calculated the same way it is for [Arithmetic functions](../../sql-reference/functions/arithmetic-functions.md), based on the number of elements in the input tuples.
:::

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
SELECT tupleHammingDistance(wordShingleMinHash(string), wordShingleMinHashCaseInsensitive(string)) AS HammingDistance
FROM (SELECT 'ClickHouse is a column-oriented database management system for online analytical processing of queries.' AS string);
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

- `tuple` — Named tuple. [Tuple](../data-types/tuple.md) with any types of values.

**Returned value**

- An array with (name, value) pairs. [Array](../data-types/array.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md), ...)).

**Example**

Query:

``` sql
CREATE TABLE tupletest (col Tuple(user_ID UInt64, session_ID UInt64)) ENGINE = Memory;

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

It is possible to transform columns to rows using this function:

``` sql
CREATE TABLE tupletest (col Tuple(CPU Float64, Memory Float64, Disk Float64)) ENGINE = Memory;

INSERT INTO tupletest VALUES(tuple(3.3, 5.5, 6.6));

SELECT arrayJoin(tupleToNameValuePairs(col)) FROM tupletest;
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

## tupleNames

Converts a tuple into an array of column names. For a tuple in the form `Tuple(a T, b T, ...)`, it returns an array of strings representing the named columns of the tuple. If the tuple elements do not have explicit names, their indices will be used as the column names instead.

**Syntax**

``` sql
tupleNames(tuple)
```

**Arguments**

- `tuple` — Named tuple. [Tuple](../../sql-reference/data-types/tuple.md) with any types of values.

**Returned value**

- An array with strings.

Type: [Array](../../sql-reference/data-types/array.md)([Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md), ...)).

**Example**

Query:

``` sql
CREATE TABLE tupletest (col Tuple(user_ID UInt64, session_ID UInt64)) ENGINE = Memory;

INSERT INTO tupletest VALUES (tuple(1, 2));

SELECT tupleNames(col) FROM tupletest;
```

Result:

``` text
┌─tupleNames(col)──────────┐
│ ['user_ID','session_ID'] │
└──────────────────────────┘
```

If you pass a simple tuple to the function, ClickHouse uses the indexes of the columns as their names:

``` sql
SELECT tupleNames(tuple(3, 2, 1));
```

Result:

``` text
┌─tupleNames((3, 2, 1))─┐
│ ['1','2','3']         │
└───────────────────────┘
```

## tuplePlus

Calculates the sum of corresponding values of two tuples of the same size.

**Syntax**

```sql
tuplePlus(tuple1, tuple2)
```

Alias: `vectorSum`.

**Arguments**

- `tuple1` — First tuple. [Tuple](../data-types/tuple.md).
- `tuple2` — Second tuple. [Tuple](../data-types/tuple.md).

**Returned value**

- Tuple with the sum. [Tuple](../data-types/tuple.md).

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

- `tuple1` — First tuple. [Tuple](../data-types/tuple.md).
- `tuple2` — Second tuple. [Tuple](../data-types/tuple.md).

**Returned value**

- Tuple with the result of subtraction. [Tuple](../data-types/tuple.md).

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

- `tuple1` — First tuple. [Tuple](../data-types/tuple.md).
- `tuple2` — Second tuple. [Tuple](../data-types/tuple.md).

**Returned value**

- Tuple with the multiplication. [Tuple](../data-types/tuple.md).

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

- `tuple1` — First tuple. [Tuple](../data-types/tuple.md).
- `tuple2` — Second tuple. [Tuple](../data-types/tuple.md).

**Returned value**

- Tuple with the result of division. [Tuple](../data-types/tuple.md).

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

- `tuple` — [Tuple](../data-types/tuple.md).

**Returned value**

- Tuple with the result of negation. [Tuple](../data-types/tuple.md).

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

- `tuple` — [Tuple](../data-types/tuple.md).
- `number` — Multiplier. [Int/UInt](../data-types/int-uint.md), [Float](../data-types/float.md) or [Decimal](../data-types/decimal.md).

**Returned value**

- Tuple with multiplied values. [Tuple](../data-types/tuple.md).

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

- `tuple` — [Tuple](../data-types/tuple.md).
- `number` — Divider. [Int/UInt](../data-types/int-uint.md), [Float](../data-types/float.md) or [Decimal](../data-types/decimal.md).

**Returned value**

- Tuple with divided values. [Tuple](../data-types/tuple.md).

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

## tupleConcat

Combines tuples passed as arguments.

``` sql
tupleConcat(tuples)
```

**Arguments**

- `tuples` – Arbitrary number of arguments of [Tuple](../data-types/tuple.md) type.

**Example**

``` sql
SELECT tupleConcat((1, 2), (3, 4), (true, false)) AS res
```

``` text
┌─res──────────────────┐
│ (1,2,3,4,true,false) │
└──────────────────────┘
```

## tupleIntDiv

Does integer division of a tuple of numerators and a tuple of denominators, and returns a tuple of the quotients.

**Syntax**

```sql
tupleIntDiv(tuple_num, tuple_div)
```

**Parameters**

- `tuple_num`: Tuple of numerator values. [Tuple](../data-types/tuple) of numeric type.
- `tuple_div`: Tuple of divisor values. [Tuple](../data-types/tuple) of numeric type.

**Returned value**

- Tuple of the quotients of `tuple_num` and `tuple_div`. [Tuple](../data-types/tuple) of integer values.

**Implementation details**

- If either `tuple_num` or `tuple_div` contain non-integer values then the result is calculated by rounding to the nearest integer for each non-integer numerator or divisor.
- An error will be thrown for division by 0. 

**Examples**

Query:

``` sql
SELECT tupleIntDiv((15, 10, 5), (5, 5, 5));
```

Result:

``` text
┌─tupleIntDiv((15, 10, 5), (5, 5, 5))─┐
│ (3,2,1)                             │
└─────────────────────────────────────┘
```

Query:

``` sql
SELECT tupleIntDiv((15, 10, 5), (5.5, 5.5, 5.5));
```

Result:

``` text
┌─tupleIntDiv((15, 10, 5), (5.5, 5.5, 5.5))─┐
│ (2,1,0)                                   │
└───────────────────────────────────────────┘
```

## tupleIntDivOrZero

Like [tupleIntDiv](#tupleintdiv) it does integer division of a tuple of numerators and a tuple of denominators, and returns a tuple of the quotients. It does not throw an error for 0 divisors, but rather returns the quotient as 0. 

**Syntax**

```sql
tupleIntDivOrZero(tuple_num, tuple_div)
```

- `tuple_num`: Tuple of numerator values. [Tuple](../data-types/tuple) of numeric type.
- `tuple_div`: Tuple of divisor values. [Tuple](../data-types/tuple) of numeric type.

**Returned value**

- Tuple of the quotients of `tuple_num` and `tuple_div`. [Tuple](../data-types/tuple) of integer values.
- Returns 0 for quotients where the divisor is 0.

**Implementation details**

- If either `tuple_num` or `tuple_div` contain non-integer values then the result is calculated by rounding to the nearest integer for each non-integer numerator or divisor as in [tupleIntDiv](#tupleintdiv).

**Examples**

Query:

``` sql
SELECT tupleIntDivOrZero((5, 10, 15), (0, 0, 0));
```

Result:

``` text
┌─tupleIntDivOrZero((5, 10, 15), (0, 0, 0))─┐
│ (0,0,0)                                   │
└───────────────────────────────────────────┘
```

## tupleIntDivByNumber

Does integer division of a tuple of numerators by a given denominator, and returns a tuple of the quotients.

**Syntax**

```sql
tupleIntDivByNumber(tuple_num, div)
```

**Parameters**

- `tuple_num`: Tuple of numerator values. [Tuple](../data-types/tuple) of numeric type.
- `div`: The divisor value. [Numeric](../data-types/int-uint.md) type.

**Returned value**

- Tuple of the quotients of `tuple_num` and `div`. [Tuple](../data-types/tuple) of integer values.

**Implementation details**

- If either `tuple_num` or `div` contain non-integer values then the result is calculated by rounding to the nearest integer for each non-integer numerator or divisor.
- An error will be thrown for division by 0. 

**Examples**

Query:

``` sql
SELECT tupleIntDivByNumber((15, 10, 5), 5);
```

Result:

``` text
┌─tupleIntDivByNumber((15, 10, 5), 5)─┐
│ (3,2,1)                             │
└─────────────────────────────────────┘
```

Query:

``` sql
SELECT tupleIntDivByNumber((15.2, 10.7, 5.5), 5.8);
```

Result:

``` text
┌─tupleIntDivByNumber((15.2, 10.7, 5.5), 5.8)─┐
│ (2,1,0)                                     │
└─────────────────────────────────────────────┘
```

## tupleIntDivOrZeroByNumber

Like [tupleIntDivByNumber](#tupleintdivbynumber) it does integer division of a tuple of numerators by a given denominator, and returns a tuple of the quotients. It does not throw an error for 0 divisors, but rather returns the quotient as 0.

**Syntax**

```sql
tupleIntDivOrZeroByNumber(tuple_num, div)
```

**Parameters**

- `tuple_num`: Tuple of numerator values. [Tuple](../data-types/tuple) of numeric type.
- `div`: The divisor value. [Numeric](../data-types/int-uint.md) type.

**Returned value**

- Tuple of the quotients of `tuple_num` and `div`. [Tuple](../data-types/tuple) of integer values.
- Returns 0 for quotients where the divisor is 0.

**Implementation details**

- If either `tuple_num` or `div` contain non-integer values then the result is calculated by rounding to the nearest integer for each non-integer numerator or divisor as in [tupleIntDivByNumber](#tupleintdivbynumber).

**Examples**

Query:

``` sql
SELECT tupleIntDivOrZeroByNumber((15, 10, 5), 5);
```

Result:

``` text
┌─tupleIntDivOrZeroByNumber((15, 10, 5), 5)─┐
│ (3,2,1)                                   │
└───────────────────────────────────────────┘
```

Query:

``` sql
SELECT tupleIntDivOrZeroByNumber((15, 10, 5), 0)
```

Result:

``` text
┌─tupleIntDivOrZeroByNumber((15, 10, 5), 0)─┐
│ (0,0,0)                                   │
└───────────────────────────────────────────┘
```

## tupleModulo

Returns a tuple of the moduli (remainders) of division operations of two tuples.

**Syntax**

```sql
tupleModulo(tuple_num, tuple_mod)
```

**Parameters**

- `tuple_num`: Tuple of numerator values. [Tuple](../data-types/tuple) of numeric type.
- `tuple_div`: Tuple of modulus values. [Tuple](../data-types/tuple) of numeric type.

**Returned value**

- Tuple of the remainders of division of `tuple_num` and `tuple_div`. [Tuple](../data-types/tuple) of non-zero integer values.
- An error is thrown for division by zero.

**Examples**

Query:

``` sql
SELECT tupleModulo((15, 10, 5), (5, 3, 2));
```

Result:

``` text
┌─tupleModulo((15, 10, 5), (5, 3, 2))─┐
│ (0,1,1)                             │
└─────────────────────────────────────┘
```

## tupleModuloByNumber

Returns a tuple of the moduli (remainders) of division operations of a tuple and a given divisor.

**Syntax**

```sql
tupleModuloByNumber(tuple_num, div)
```

**Parameters**

- `tuple_num`: Tuple of numerator values. [Tuple](../data-types/tuple) of numeric type.
- `div`: The divisor value. [Numeric](../data-types/int-uint.md) type.

**Returned value**

- Tuple of the remainders of division of `tuple_num` and `div`. [Tuple](../data-types/tuple) of non-zero integer values.
- An error is thrown for division by zero.

**Examples**

Query:

``` sql
SELECT tupleModuloByNumber((15, 10, 5), 2);
```

Result:

``` text
┌─tupleModuloByNumber((15, 10, 5), 2)─┐
│ (1,0,1)                             │
└─────────────────────────────────────┘
```

## flattenTuple

Returns a flattened `output` tuple from a nested named `input` tuple. Elements of the `output` tuple are the paths from the original `input` tuple. For instance: `Tuple(a Int, Tuple(b Int, c Int)) -> Tuple(a Int, b Int, c Int)`. `flattenTuple` can be used to select all paths from type `Object` as separate columns.

**Syntax**

```sql
flattenTuple(input)
```

**Parameters**

- `input`: Nested named tuple to flatten. [Tuple](../data-types/tuple).

**Returned value**

- `output` tuple whose elements are paths from the original `input`. [Tuple](../data-types/tuple).

**Example**

Query:

``` sql
CREATE TABLE t_flatten_tuple(t Tuple(t1 Nested(a UInt32, s String), b UInt32, t2 Tuple(k String, v UInt32))) ENGINE = Memory;
INSERT INTO t_flatten_tuple VALUES (([(1, 'a'), (2, 'b')], 3, ('c', 4)));
SELECT flattenTuple(t) FROM t_flatten_tuple;
```

Result:

``` text
┌─flattenTuple(t)───────────┐
│ ([1,2],['a','b'],3,'c',4) │
└───────────────────────────┘
```

## Distance functions

All supported functions are described in [distance functions documentation](../../sql-reference/functions/distance-functions.md).
