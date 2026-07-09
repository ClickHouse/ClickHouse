---
description: 'ClickHouse FixedString 数据类型文档'
sidebar_label: 'FixedString(N)'
sidebar_position: 10
slug: /sql-reference/data-types/fixedstring
title: 'FixedString(N)'
doc_type: 'reference'
---

长度固定为 `N` 字节的字符串 (不是按字符数或码点数计算) 。

要声明 `FixedString` 类型的列，请使用以下语法：

```sql
<column_name> FixedString(N)
```

其中 `N` 是自然数。

当数据长度恰好为 `N` 字节时，`FixedString` 类型的效率较高。其他情况下，反而很可能会降低效率。

适合高效存储在 `FixedString` 类型列中的值示例：

* IP 地址的二进制表示 (IPv6 使用 `FixedString(16)`) 。
* 语言代码 (ru&#95;RU、en&#95;US ... ) 。
* 货币代码 (USD、RUB ... ) 。
* 哈希的二进制表示 (MD5 使用 `FixedString(16)`，SHA256 使用 `FixedString(32)`) 。

要存储 UUID 值，请使用 [UUID](../../sql-reference/data-types/uuid.md) 数据类型。

插入数据时，ClickHouse 会：

* 如果字符串包含的字节数少于 `N`，则用空字节补齐该字符串。
* 如果字符串包含的字节数超过 `N`，则抛出 `Too large value for FixedString(N)` 异常。

考虑下表，它只有一个 `FixedString(2)` 列：

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

请注意，`FixedString(N)` 值的长度是固定的。即使 `FixedString(N)` 值仅由 null byte 填充，[length](/zh/sql-reference/functions/array-functions#length) 函数也会返回 `N`，但在这种情况下，[empty](/zh/sql-reference/functions/array-functions#empty) 函数会返回 `1`。

使用 `WHERE` 子句选择数据时，返回结果会因条件的指定方式不同而有所差异：

* 如果使用等值运算符 `=`、`==` 或 `equals` 函数，ClickHouse *不会* 将 `\0` 字符考虑在内。也就是说，查询 `SELECT * FROM FixedStringTable WHERE name = 'a';` 和 `SELECT * FROM FixedStringTable WHERE name = 'a\0';` 会返回相同的结果。
* 如果使用 `LIKE` 子句，ClickHouse *会* 将 `\0` 字符考虑在内，因此可能需要在过滤条件中显式指定 `\0` 字符。

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