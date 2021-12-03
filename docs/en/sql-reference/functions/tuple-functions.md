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
‘N’ is the column index, starting from 1. N must be a constant. ‘N’ must be a constant. ‘N’ must be a strict postive integer no greater than the size of the tuple.
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

Type: [UInt8](../../sql-reference/data-types/int-uint.md).

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
