---
slug: /en/sql-reference/data-types/tuple
sidebar_position: 34
sidebar_label: Tuple(T1, T2, ...)
---

# Tuple(T1, T2, ...)

A tuple of elements, each having an individual [type](../../sql-reference/data-types/index.md#data_types). Tuple must contain at least one element.

Tuples are used for temporary column grouping. Columns can be grouped when an IN expression is used in a query, and for specifying certain formal parameters of lambda functions. For more information, see the sections [IN operators](../../sql-reference/operators/in.md) and [Higher order functions](../../sql-reference/functions/index.md#higher-order-functions).

Tuples can be the result of a query. In this case, for text formats other than JSON, values are comma-separated in brackets. In JSON formats, tuples are output as arrays (in square brackets).

## Creating Tuples

You can use a function to create a tuple:

``` sql
tuple(T1, T2, ...)
```

Example of creating a tuple:

``` sql
SELECT tuple(1, 'a') AS x, toTypeName(x)
```

``` text
┌─x───────┬─toTypeName(tuple(1, 'a'))─┐
│ (1,'a') │ Tuple(UInt8, String)      │
└─────────┴───────────────────────────┘
```

A Tuple can contain a single element

Example:

``` sql
SELECT tuple('a') AS x;
```

``` text
┌─x─────┐
│ ('a') │
└───────┘
```

Syntax `(tuple_element1, tuple_element2)` may be used to create a tuple of several elements without calling the `tuple()` function.

Example:

``` sql
SELECT (1, 'a') AS x, (today(), rand(), 'someString') AS y, ('a') AS not_a_tuple;
```

``` text
┌─x───────┬─y──────────────────────────────────────┬─not_a_tuple─┐
│ (1,'a') │ ('2022-09-21',2006973416,'someString') │ a           │
└─────────┴────────────────────────────────────────┴─────────────┘
```

## Data Type Detection

When creating tuples on the fly, ClickHouse interferes the type of the tuples arguments as the smallest types which can hold the provided argument value. If the value is [NULL](../../sql-reference/syntax.md#null-literal), the interfered type is [Nullable](../../sql-reference/data-types/nullable.md).

Example of automatic data type detection:

``` sql
SELECT tuple(1, NULL) AS x, toTypeName(x)
```

``` text
┌─x─────────┬─toTypeName(tuple(1, NULL))──────┐
│ (1, NULL) │ Tuple(UInt8, Nullable(Nothing)) │
└───────────┴─────────────────────────────────┘
```

## Referring to Tuple Elements

Tuple elements can be referred to by name or by index:

``` sql
CREATE TABLE named_tuples (`a` Tuple(s String, i Int64)) ENGINE = Memory;
INSERT INTO named_tuples VALUES (('y', 10)), (('x',-10));

SELECT a.s FROM named_tuples; -- by name
SELECT a.2 FROM named_tuples; -- by index
```

Result:

``` text
┌─a.s─┐
│ y   │
│ x   │
└─────┘

┌─tupleElement(a, 2)─┐
│                 10 │
│                -10 │
└────────────────────┘
```

## Comparison operations with Tuple

Two tuples are compared by sequentially comparing their elements from the left to the right. If first tuples element is greater (smaller) than the second tuples corresponding element, then the first tuple is greater (smaller) than the second, otherwise (both elements are equal), the next element is compared.

Example:

```sql
SELECT (1, 'z') > (1, 'a') c1, (2022, 01, 02) > (2023, 04, 02) c2, (1,2,3) = (3,2,1) c3;
```

``` text
┌─c1─┬─c2─┬─c3─┐
│  1 │  0 │  0 │
└────┴────┴────┘
```

Real world examples:

```sql
CREATE TABLE test
(
    `year` Int16,
    `month` Int8,
    `day` Int8
)
ENGINE = Memory AS
SELECT *
FROM values((2022, 12, 31), (2000, 1, 1));

SELECT * FROM test;

┌─year─┬─month─┬─day─┐
│ 2022 │    12 │  31 │
│ 2000 │     1 │   1 │
└──────┴───────┴─────┘

SELECT *
FROM test
WHERE (year, month, day) > (2010, 1, 1);

┌─year─┬─month─┬─day─┐
│ 2022 │    12 │  31 │
└──────┴───────┴─────┘


CREATE TABLE test
(
    `key` Int64,
    `duration` UInt32,
    `value` Float64
)
ENGINE = Memory AS
SELECT *
FROM values((1, 42, 66.5), (1, 42, 70), (2, 1, 10), (2, 2, 0));

SELECT * FROM test;

┌─key─┬─duration─┬─value─┐
│   1 │       42 │  66.5 │
│   1 │       42 │    70 │
│   2 │        1 │    10 │
│   2 │        2 │     0 │
└─────┴──────────┴───────┘

-- Let's find a value for each key with the biggest duration, if durations are equal, select the biggest value

SELECT
    key,
    max(duration),
    argMax(value, (duration, value))
FROM test
GROUP BY key
ORDER BY key ASC;

┌─key─┬─max(duration)─┬─argMax(value, tuple(duration, value))─┐
│   1 │            42 │                                    70 │
│   2 │             2 │                                     0 │
└─────┴───────────────┴───────────────────────────────────────┘
```
