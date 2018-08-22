<a name="data_type-array"></a>

# Array(T)

Array of `T`-type items.

`T` can be anything, including an array. Use multi-dimensional arrays with caution. ClickHouse has limited support for multi-dimensional arrays. For example, they can't be stored in `MergeTree` tables.

## Creating an array

You can use a function to create an array:

```
array(T)
```

You can also use square brackets.

```
[]
```

Example of creating an array:

```
:) SELECT array(1, 2) AS x, toTypeName(x)

SELECT
    [1, 2] AS x,
    toTypeName(x)

┌─x─────┬─toTypeName(array(1, 2))─┐
│ [1,2] │ Array(UInt8)            │
└───────┴─────────────────────────┘

1 rows in set. Elapsed: 0.002 sec.

:) SELECT [1, 2] AS x, toTypeName(x)

SELECT
    [1, 2] AS x,
    toTypeName(x)

┌─x─────┬─toTypeName([1, 2])─┐
│ [1,2] │ Array(UInt8)       │
└───────┴────────────────────┘

1 rows in set. Elapsed: 0.002 sec.
```

## Working with data types

When creating an array on the fly, ClickHouse automatically defines the argument type as the narrowest data type that can store all the listed arguments. If there are any [NULL](../query_language/syntax.md#null-literal) or  [Nullable](nullable.md#data_type-nullable) type arguments, the type of array elements is [Nullable](nullable.md#data_type-nullable).

If ClickHouse couldn't determine the data type, it will generate an exception. For instance, this will happen when trying to create an array with strings and numbers simultaneously (`SELECT array(1, 'a')`).

Examples of automatic data type detection:

```
:) SELECT array(1, 2, NULL) AS x, toTypeName(x)

SELECT
    [1, 2, NULL] AS x,
    toTypeName(x)

┌─x──────────┬─toTypeName(array(1, 2, NULL))─┐
│ [1,2,NULL] │ Array(Nullable(UInt8))        │
└────────────┴───────────────────────────────┘

1 rows in set. Elapsed: 0.002 sec.
```

If you try to create an array of incompatible data types, ClickHouse throws an exception:

```
:) SELECT array(1, 'a')

SELECT [1, 'a']

Received exception from server (version 1.1.54388):
Code: 386. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: There is no supertype for types UInt8, String because some of them are String/FixedString and some of them are not.

0 rows in set. Elapsed: 0.246 sec.
```

