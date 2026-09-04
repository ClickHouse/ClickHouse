---
description: 'Documentation for the FixedString data type in ClickHouse'
sidebar_label: 'FixedString(N)'
sidebar_position: 10
slug: /sql-reference/data-types/fixedstring
title: 'FixedString(N)'
doc_type: 'reference'
---

# FixedString(N)

A fixed-length string of `N` bytes (neither characters nor code points).

To declare a column of `FixedString` type, use the following syntax:

```sql
<column_name> FixedString(N)
```

Where `N` is a natural number.

The `FixedString` type is efficient when data has the length of precisely `N` bytes. In all other cases, it is likely to reduce efficiency.

Examples of the values that can be efficiently stored in `FixedString`-typed columns:

- The binary representation of IP addresses (`FixedString(16)` for IPv6).
- Language codes (ru_RU, en_US ... ).
- Currency codes (USD, RUB ... ).
- Binary representation of hashes (`FixedString(16)` for MD5, `FixedString(32)` for SHA256).

To store UUID values, use the [UUID](../../sql-reference/data-types/uuid.md) data type.

When inserting the data, ClickHouse:

- Complements a string with null bytes if the string contains fewer than `N` bytes.
- Throws the `Too large value for FixedString(N)` exception if the string contains more than `N` bytes.

Let's consider the following table with the single `FixedString(2)` column:

```sql


INSERT INTO FixedStringTable VALUES ('a'), ('ab'), ('');
```

```sql
SELECT
    name,
    toTypeName(name),
    length(name),
    empty(name)
FROM FixedStringTable;
```

```text
┌─name─┬─toTypeName(name)─┬─length(name)─┬─empty(name)─┐
│ a    │ FixedString(2)   │            2 │           0 │
│ ab   │ FixedString(2)   │            2 │           0 │
│      │ FixedString(2)   │            2 │           1 │
└──────┴──────────────────┴──────────────┴─────────────┘
```

Note that the length of the `FixedString(N)` value is constant. The [length](/sql-reference/functions/array-functions#length) function returns `N` even if the `FixedString(N)` value is filled only with null bytes, but the [empty](/sql-reference/functions/array-functions#empty) function returns `1` in this case.

Selecting data with `WHERE` clause return various result depending on how the condition is specified:

- If equality operator `=` or `==` or `equals` function used, ClickHouse _doesn't_ take `\0` char into consideration, i.e. queries `SELECT * FROM FixedStringTable WHERE name = 'a';` and `SELECT * FROM FixedStringTable WHERE name = 'a\0';` return the same result.
- If `LIKE` clause is used, ClickHouse _does_ take `\0` char into consideration, so one may need to explicitly specify `\0` char in the filter condition.

```sql
SELECT name
FROM FixedStringTable
WHERE name = 'a'
FORMAT JSONStringsEachRow

{"name":"a\u0000"}


SELECT name
FROM FixedStringTable
WHERE name = 'a\0'
FORMAT JSONStringsEachRow

{"name":"a\u0000"}


SELECT name
FROM FixedStringTable
WHERE name = 'a'
FORMAT JSONStringsEachRow

Query id: c32cec28-bb9e-4650-86ce-d74a1694d79e

{"name":"a\u0000"}


SELECT name
FROM FixedStringTable
WHERE name LIKE 'a'
FORMAT JSONStringsEachRow

0 rows in set.


SELECT name
FROM FixedStringTable
WHERE name LIKE 'a\0'
FORMAT JSONStringsEachRow

{"name":"a\u0000"}
```
