---
description: 'ALL 子句文档'
sidebar_label: 'ALL'
slug: /sql-reference/statements/select/all
title: 'ALL 子句'
doc_type: 'reference'
---

如果一个表中有多条匹配的行，`ALL` 会返回其中的所有行。`SELECT ALL` 与不使用 `DISTINCT` 的 `SELECT` 完全相同。如果同时指定 `ALL` 和 `DISTINCT`，则会抛出异常。

`ALL` 也可以在聚合函数中指定，不过对查询结果没有实际影响。

例如：

```sql
SELECT sum(ALL number) FROM numbers(10);
```

等同于：

```sql
SELECT sum(number) FROM numbers(10);
```