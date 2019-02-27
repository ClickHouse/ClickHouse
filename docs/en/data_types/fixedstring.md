# FixedString

A fixed-length string of `N` bytes (not characters or code points).

To declare the column of `FixedString` type use the following syntax:

```
<column_name> FixedString(N)
```

Where `N` is a natural number.

It is efficient to use `FixedString` in case when data has the length of `N` bytes exactly. For example: IP addresses (`FixedString(16)` for IPv6), language codes (ru_RU, en_US ... ), currency codes (USD, RUB ... ), hashes (`FixedString(16)` for MD5, `FixedString(32)` for SHA256).  In all other cases, you can get the loss of efficiency. To store hashes or IP addresses use their binary representation. To store UUID values use the [UUID](uuid.md) data type.

When inserting the data, ClickHouse:

- Complements a string with null bytes if the string contains fewer bytes than `N`.
- Throws the `Too large value for FixedString(N)` exception if the string contains more than `N` bytes.

When selecting the data, ClickHouse does not trim off the null bytes at the end of the string. If you use the `WHERE` clause, you should add null bytes manually to match the `FixedString` value.

Example:

```
SELECT * FROM FixedStringTable

┌─a──┐
│ aa │
│ b  │
└────┘

SELECT * FROM FixedStringTable WHERE a = 'b'

Ok.

0 rows in set. Elapsed: 0.009 sec.

SELECT * FROM FixedStringTable
WHERE a = 'b\0'

┌─a─┐
│ b │
└───┘

1 rows in set. Elapsed: 0.002 sec.
```

This behavior differs from MySQL behavior for the `CHAR` type (where strings are padded with spaces, and the spaces are removed for output).

Note that the length of the `FixedString` value is constant even if it is filled with null bytes only. The [empty](../query_language/functions/string_functions.md#string_functions-empty) functions returns `1` if the `FixedString` value contains the null bytes only.

[Original article](https://clickhouse.yandex/docs/en/data_types/fixedstring/) <!--hide-->
