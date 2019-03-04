# FixedString

A fixed-length string of `N` bytes (not characters or code points).

To declare the column of `FixedString` type use the following syntax:

```
<column_name> FixedString(N)
```

Where `N` is a natural number.

It is efficient to use `FixedString` in case when data has the length of `N` bytes exactly. In all other cases, you can get the loss of efficiency.

Examples of the values that can be efficiently stored in the `FixedString`-typed columns:

- Binary representation of IP addresses (`FixedString(16)` for IPv6).
- Language codes (ru_RU, en_US ... ).
- Currency codes (USD, RUB ... ).
- Binary representation of hashes (`FixedString(16)` for MD5, `FixedString(32)` for SHA256).

To store UUID values, use the [UUID](uuid.md) data type.

When inserting the data, ClickHouse:

- Complements a string with null bytes if the string contains fewer bytes than `N`.
- Throws the `Too large value for FixedString(N)` exception if the string contains more than `N` bytes.

When selecting the data, ClickHouse does not trim off the null bytes at the end of the string. If you use the `WHERE` clause, you should add null bytes manually to match the `FixedString` value.

Let's consider the following table with the single `FixedString(2)` column:

```
┌─name──┐
│ b     │
└───────┘
```

When selecting the data as follows:

```
SELECT * FROM FixedStringTable WHERE a = 'b'

Ok.

0 rows in set. Elapsed: 0.009 sec.
```
We don't get any data as a result. We should complement filter pattern with null bytes.
```
SELECT * FROM FixedStringTable
WHERE a = 'b\0'

┌─a─┐
│ b │
└───┘

1 rows in set. Elapsed: 0.002 sec.
```

This behavior differs from MySQL behavior for the `CHAR` type (where strings are padded with spaces, and the spaces are removed for output).

Note that the length of the `FixedString` value is constant even if it is filled with null bytes only. The [empty](../query_language/functions/string_functions.md#string_functions-empty) function returns `1` if the `FixedString` value contains only the null bytes.

[Original article](https://clickhouse.yandex/docs/en/data_types/fixedstring/) <!--hide-->
