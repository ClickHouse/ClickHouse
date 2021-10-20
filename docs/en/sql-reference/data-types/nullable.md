---
toc_priority: 55
toc_title: Nullable
---

# Nullable(typename) {#data_type-nullable}

Allows to store special marker ([NULL](../../sql-reference/syntax.md)) that denotes “missing value” alongside normal values allowed by `TypeName`. For example, a `Nullable(Int8)` type column can store `Int8` type values, and the rows that do not have a value will store `NULL`.

For a `TypeName`, you can’t use composite data types [Array](../../sql-reference/data-types/array.md) and [Tuple](../../sql-reference/data-types/tuple.md). Composite data types can contain `Nullable` type values, such as `Array(Nullable(Int8))`.

A `Nullable` type field can’t be included in table indexes.

`NULL` is the default value for any `Nullable` type, unless specified otherwise in the ClickHouse server configuration.

## Storage Features {#storage-features}

To store `Nullable` type values in a table column, ClickHouse uses a separate file with `NULL` masks in addition to normal file with values. Entries in masks file allow ClickHouse to distinguish between `NULL` and a default value of corresponding data type for each table row. Because of an additional file, `Nullable` column consumes additional storage space compared to a similar normal one.

!!! info "Note"
    Using `Nullable` almost always negatively affects performance, keep this in mind when designing your databases.

## Finding NULL {#finding-null}

It is possible to find `NULL` values in a column by using `null` subcolumn without reading the whole column. It returns `1` if the corresponding value is `NULL` and `0` otherwise.

**Example**

Query:

``` sql
CREATE TABLE nullable (`n` Nullable(UInt32)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO nullable VALUES (1) (NULL) (2) (NULL);

SELECT n.null FROM nullable;
```

Result:

``` text
┌─n.null─┐
│      0 │
│      1 │
│      0 │
│      1 │
└────────┘
```

## Usage Example {#usage-example}

``` sql
CREATE TABLE t_null(x Int8, y Nullable(Int8)) ENGINE TinyLog
```

``` sql
INSERT INTO t_null VALUES (1, NULL), (2, 3)
```

``` sql
SELECT x + y FROM t_null
```

``` text
┌─plus(x, y)─┐
│       ᴺᵁᴸᴸ │
│          5 │
└────────────┘
```

[Original article](https://clickhouse.tech/docs/en/data_types/nullable/) <!--hide-->
