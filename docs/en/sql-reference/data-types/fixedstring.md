---
sidebar_position: 45
sidebar_label: FixedString(N)
---

# Fixedstring {#fixedstring}

A fixed-length string of `N` bytes (neither characters nor code points).

To declare a column of `FixedString` type, use the following syntax:

``` sql
<column_name> FixedString(N)
```

Where `N` is a natural number.

The `FixedString` type is efficient when data has the length of precisely `N` bytes. In all other cases, it is likely to reduce efficiency.

Examples of the values that can be efficiently stored in `FixedString`-typed columns:

-   The binary representation of IP addresses (`FixedString(16)` for IPv6).
-   Language codes (ru_RU, en_US … ).
-   Currency codes (USD, RUB … ).
-   Binary representation of hashes (`FixedString(16)` for MD5, `FixedString(32)` for SHA256).

To store UUID values, use the [UUID](../../sql-reference/data-types/uuid.md) data type.

When inserting the data, ClickHouse:

-   Complements a string with null bytes if the string contains fewer than `N` bytes.
-   Throws the `Too large value for FixedString(N)` exception if the string contains more than `N` bytes.

When selecting the data, ClickHouse does not remove the null bytes at the end of the string. If you use the `WHERE` clause, you should add null bytes manually to match the `FixedString` value. The following example illustrates how to use the `WHERE` clause with `FixedString`.

Let’s consider the following table with the single `FixedString(2)` column:

``` text
┌─name──┐
│ b     │
└───────┘
```

The query `SELECT * FROM FixedStringTable WHERE a = 'b'` does not return any data as a result. We should complement the filter pattern with null bytes.

``` sql
SELECT * FROM FixedStringTable
WHERE a = 'b\0'
```

``` text
┌─a─┐
│ b │
└───┘
```

This behaviour differs from MySQL for the `CHAR` type (where strings are padded with spaces, and the spaces are removed for output).

Note that the length of the `FixedString(N)` value is constant. The [length](../../sql-reference/functions/array-functions.md#array_functions-length) function returns `N` even if the `FixedString(N)` value is filled only with null bytes, but the [empty](../../sql-reference/functions/string-functions.md#empty) function returns `1` in this case.

[Original article](https://clickhouse.com/docs/en/data_types/fixedstring/) <!--hide-->
