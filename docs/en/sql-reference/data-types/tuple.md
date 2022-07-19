---
toc_priority: 54
toc_title: Tuple(T1, T2, ...)
---

# Tuple(t1, T2, …) {#tuplet1-t2}

A tuple of elements, each having an individual [type](../../sql-reference/data-types/index.md#data_types).

Tuples are used for temporary column grouping. Columns can be grouped when an IN expression is used in a query, and for specifying certain formal parameters of lambda functions. For more information, see the sections [IN operators](../../sql-reference/operators/in.md) and [Higher order functions](../../sql-reference/functions/index.md#higher-order-functions).

Tuples can be the result of a query. In this case, for text formats other than JSON, values are comma-separated in brackets. In JSON formats, tuples are output as arrays (in square brackets).

## Creating a Tuple {#creating-a-tuple}

You can use a function to create a tuple:

``` sql
tuple(T1, T2, ...)
```

Example of creating a tuple:

``` sql
SELECT tuple(1,'a') AS x, toTypeName(x)
```

``` text
┌─x───────┬─toTypeName(tuple(1, 'a'))─┐
│ (1,'a') │ Tuple(UInt8, String)      │
└─────────┴───────────────────────────┘
```

## Working with Data Types {#working-with-data-types}

When creating a tuple on the fly, ClickHouse automatically detects the type of each argument as the minimum of the types which can store the argument value. If the argument is [NULL](../../sql-reference/syntax.md#null-literal), the type of the tuple element is [Nullable](../../sql-reference/data-types/nullable.md).

Example of automatic data type detection:

``` sql
SELECT tuple(1, NULL) AS x, toTypeName(x)
```

``` text
┌─x────────┬─toTypeName(tuple(1, NULL))──────┐
│ (1,NULL) │ Tuple(UInt8, Nullable(Nothing)) │
└──────────┴─────────────────────────────────┘
```

## Addressing Tuple Elements {#addressing-tuple-elements}

It is possible to read elements of named tuples using indexes and names:

``` sql
CREATE TABLE named_tuples (`a` Tuple(s String, i Int64)) ENGINE = Memory;

INSERT INTO named_tuples VALUES (('y', 10)), (('x',-10));

SELECT a.s FROM named_tuples;

SELECT a.2 FROM named_tuples;
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

[Original article](https://clickhouse.tech/docs/en/data_types/tuple/) <!--hide-->
