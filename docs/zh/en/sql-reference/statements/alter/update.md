---
description: 'ALTER TABLE ... UPDATE 语句说明'
sidebar_label: 'UPDATE'
sidebar_position: 40
slug: /sql-reference/statements/alter/update
title: 'ALTER TABLE ... UPDATE 语句'
doc_type: 'reference'
---

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_id] WHERE filter_expr
```

修改与指定过滤表达式匹配的数据。该操作通过 [变更](/zh/sql-reference/statements/alter/index.md#mutations) 实现。

:::note
`ALTER TABLE` 前缀使这种语法不同于大多数其他支持 SQL 的系统。这样设计是为了表明，与 OLTP 数据库中的类似查询不同，这是一项开销较大的操作，不适合频繁使用。
:::

`filter_expr` 必须为 `UInt8` 类型。此查询会将指定列的值更新为对应表达式的值，作用于 `filter_expr` 取非零值的那些行。值会使用 `CAST` 运算符转换为列类型。不支持更新用于计算主键或分区键的列。

一个查询可以包含多个用逗号分隔的命令。

查询处理的同步方式由 [mutations&#95;sync](/zh/operations/settings/settings.md/#mutations_sync) 设置决定。默认情况下，它是异步的。

**另请参见**

* [Mutations](/zh/sql-reference/statements/alter/index.md#mutations)
* [ALTER 查询的同步性](/zh/sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
* [mutations&#95;sync](/zh/operations/settings/settings.md/#mutations_sync) 设置
* [Lightweight `UPDATE`](/zh/sql-reference/statements/update) - 使用补丁分区片段的轻量级更新替代方案
* [`APPLY PATCHES`](/zh/sql-reference/statements/alter/apply-patches) - 手动应用轻量级更新产生的补丁

<div id="related-content">
  ## 相关内容
</div>

* 博客：[如何在 ClickHouse 中处理更新和删除](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)