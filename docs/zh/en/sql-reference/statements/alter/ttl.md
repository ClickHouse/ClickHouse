---
description: '有关表生存时间 (TTL) 相关操作的文档'
sidebar_label: 'TTL'
sidebar_position: 44
slug: /sql-reference/statements/alter/ttl
title: '表生存时间 (TTL) 的相关操作'
doc_type: 'reference'
---

:::note
如果你想了解如何使用 生存时间 (TTL) 管理旧数据的详细信息，请参阅[使用 生存时间 (TTL) 管理数据](/zh/guides/developer/ttl.md)用户指南。以下内容演示了如何更改或删除现有的 生存时间 (TTL) 规则。
:::

<div id="modify-ttl">
  ## 修改 生存时间 (TTL)
</div>

你可以通过以下形式的请求来修改[表生存时间 (TTL)](../../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl)：

```sql
ALTER TABLE [db.]table_name [ON CLUSTER cluster] MODIFY TTL ttl_expression;
```

<div id="remove-ttl">
  ## REMOVE TTL
</div>

可使用以下查询从表中移除生存时间 (TTL) 属性：

```sql
ALTER TABLE [db.]table_name [ON CLUSTER cluster] REMOVE TTL
```

**示例**

考虑以下定义了表 `TTL` 的表：

```sql
CREATE TABLE table_with_ttl
(
    event_time DateTime,
    UserID UInt64,
    Comment String
)
ENGINE MergeTree()
ORDER BY tuple()
TTL event_time + INTERVAL 3 MONTH
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO table_with_ttl VALUES (now(), 1, 'username1');

INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
```

运行 `OPTIMIZE` 以强制触发 `TTL` 清理：

```sql
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```

表中的第二行已删除。

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
└───────────────────────┴─────────┴──────────────┘
```

现在执行以下查询以移除表 `TTL`：

```sql
ALTER TABLE table_with_ttl REMOVE TTL;
```

重新插入已删除的行，并使用 `OPTIMIZE` 再次强制触发 `TTL` 清理：

```sql
INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```

由于 `TTL` 已被移除，因此第二行不会被删除：

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
│   2020-08-11 12:44:57 │       2 │    username2 │
└───────────────────────┴─────────┴──────────────┘
```

**另请参阅**

* 有关 [TTL 表达式](../../../sql-reference/statements/create/table.md#ttl-expression)的更多信息。
* [修改带有 生存时间 (TTL) 的列](/zh/sql-reference/statements/alter/ttl)。