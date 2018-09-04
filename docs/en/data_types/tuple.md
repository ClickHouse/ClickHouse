<a name="data_type-tuple"></a>

# Tuple(T1, T2, ...)

A tuple of elements of any [type](index.md#data_types). There can be one or more types of elements in a tuple.

You can't store tuples in tables (other than Memory tables). They are used for temporary column grouping. Columns can be grouped when an IN expression is used in a query, and for specifying certain formal parameters of lambda functions. For more information, see the sections [IN operators](../query_language/select.md#in_operators) and [Higher order functions](../query_language/functions/higher_order_functions.md#higher_order_functions).

Tuples can be the result of a query. In this case, for text formats other than JSON, values are comma-separated in brackets. In JSON formats, tuples are output as arrays (in square brackets).

## Creating a tuple

You can use a function to create a tuple

```
tuple(T1, T2, ...)
```

Example of creating a tuple:

```
:) SELECT tuple(1,'a') AS x, toTypeName(x)

SELECT
    (1, 'a') AS x,
    toTypeName(x)

┌─x───────┬─toTypeName(tuple(1, 'a'))─┐
│ (1,'a') │ Tuple(UInt8, String)      │
└─────────┴───────────────────────────┘

1 rows in set. Elapsed: 0.021 sec.
```

## Working with data types

When creating a tuple on the fly, ClickHouse automatically detects the type of each argument as the minimum of the types which can store the argument value. If the argument is [NULL](../query_language/syntax.md#null-literal), the type of the tuple element is [Nullable](nullable.md#data_type-nullable).

Example of automatic data type detection:

```
SELECT tuple(1,NULL) AS x, toTypeName(x)

SELECT
    (1, NULL) AS x,
    toTypeName(x)

┌─x────────┬─toTypeName(tuple(1, NULL))──────┐
│ (1,NULL) │ Tuple(UInt8, Nullable(Nothing)) │
└──────────┴─────────────────────────────────┘

1 rows in set. Elapsed: 0.002 sec.
```

