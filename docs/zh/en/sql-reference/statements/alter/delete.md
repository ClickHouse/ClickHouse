---
description: 'ALTER TABLE ... DELETE 语句文档'
sidebar_label: 'DELETE'
sidebar_position: 39
slug: /sql-reference/statements/alter/delete
title: 'ALTER TABLE ... DELETE 语句'
doc_type: 'reference'
---

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE WHERE filter_expr
```

删除与指定筛选表达式匹配的数据。作为一种 [变更](/zh/sql-reference/statements/alter/index.md#mutations) 来实现。

:::note
`ALTER TABLE` 前缀使这种语法不同于大多数其他支持 SQL 的系统。这样设计是为了表明，与 OLTP 数据库中的类似查询不同，这是一种不适合频繁使用的重型操作。`ALTER TABLE` 被视为一种重量级操作，要求在删除底层数据之前先将其合并。对于 MergeTree 表，建议使用 [`DELETE FROM` 查询](/zh/sql-reference/statements/delete.md)，它执行轻量级删除，而且通常会快得多。
:::

`filter_expr` 必须是 `UInt8` 类型。该查询会删除表中使此表达式结果为非零值的行。

一个查询可以包含多个由逗号分隔的命令。

查询处理的同步方式由 [mutations&#95;sync](/zh/operations/settings/settings.md/#mutations_sync) 设置定义。默认情况下，它是异步的。

**另请参阅**

* [变更](/zh/sql-reference/statements/alter/index.md#mutations)
* [ALTER 查询的同步方式](/zh/sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
* [mutations&#95;sync](/zh/operations/settings/settings.md/#mutations_sync) 设置

<div id="related-content">
  ## 相关内容
</div>

* 博客：[ClickHouse 中更新和删除操作的处理](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)