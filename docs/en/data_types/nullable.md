<a name="data_type-nullable"></a>

# Nullable(TypeName)

Allows you to work with the `TypeName` value or without it ([NULL](../query_language/syntax.md#null-literal)) in the same variable, including storage of `NULL` tables with the `TypeName` values. For example, a `Nullable(Int8)`  type column can store `Int8` type values, and the rows that don't have a value will store `NULL`.

For a `TypeName`, you can't use composite data types [Array](array.md#data_type is array) and [Tuple](tuple.md#data_type-tuple). Composite data types can contain `Nullable` type values, such as `Array(Nullable(Int8))`.

A `Nullable` type field can't be included in indexes.

`NULL` is the default value for the `Nullable` type, unless specified otherwise in the ClickHouse server configuration.

##Storage features

For storing `Nullable` type values, ClickHouse uses:

- A separate file with `NULL` masks (referred to as the mask).
- The file with the values.

The mask determines what is in a data cell: `NULL` or a value.

When the mask indicates that `NULL` is stored in a cell, the file with values stores the default value for the data type. So if the field has the type `Nullable(Int8)`, the cell will store the default value for `Int8`. This feature increases storage capacity.

!!! Note:
Using `Nullable` almost always reduces performance, so keep this in mind when designing your databases.

## Usage example

```
:) CREATE TABLE t_null(x Int8, y Nullable(Int8)) ENGINE TinyLog

CREATE TABLE t_null
(
    x Int8,
    y Nullable(Int8)
)
ENGINE = TinyLog

Ok.

0 rows in set. Elapsed: 0.012 sec.

:) INSERT INTO t_null VALUES (1, NULL)

INSERT INTO t_null VALUES

Ok.

1 rows in set. Elapsed: 0.007 sec.

:) SELECT x + y from t_null

SELECT x + y
FROM t_null

┌─plus(x, y)─┐
│       ᴺᵁᴸᴸ │
│          5 │
└────────────┘

2 rows in set. Elapsed: 0.144 sec.
```

