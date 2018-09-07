<a name="data_type-nullable"></a>

# Nullable(TypeName)

Allows to store special marker ([NULL](../query_language/syntax.md#null-literal)) that denotes "missing value" alongside normal values allowed by `TypeName`. For example, a `Nullable(Int8)` type column can store `Int8` type values, and the rows that don't have a value will store `NULL`.

For a `TypeName`, you can't use composite data types [Array](array.md#data_type is array) and [Tuple](tuple.md#data_type-tuple). Composite data types can contain `Nullable` type values, such as `Array(Nullable(Int8))`.

A `Nullable` type field can't be included in table indexes.

`NULL` is the default value for any `Nullable` type, unless specified otherwise in the ClickHouse server configuration.

## Storage features

To store `Nullable` type values in table column, ClickHouse uses a separate file with `NULL` masks in addition to normal file with values. Entries in masks file allow ClickHouse to distinguish between `NULL` and default value of corresponding data type for each table row. Because of additional file, `Nullable` column consumes additional storage space compared to similar normal one.

!!! info "Note"
    Using `Nullable` almost always negatively affects performance, keep this in mind when designing your databases.

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

:) SELECT x + y FROM t_null

SELECT x + y
FROM t_null

┌─plus(x, y)─┐
│       ᴺᵁᴸᴸ │
│          5 │
└────────────┘

2 rows in set. Elapsed: 0.144 sec.
```
