---
sidebar_position: 40
sidebar_label: UPDATE
---

# ALTER TABLE … UPDATE 语句 {#alter-table-update-statements}

``` sql
ALTER TABLE [db.]table UPDATE column1 = expr1 [, ...] WHERE filter_expr
```

操作与指定过滤表达式相匹配的数据。作为一个[变更 mutation](../../../sql-reference/statements/alter/index.md#mutations)来实现.

!!! note "Note"
    `ALTER TABLE` 的前缀使这个语法与其他大多数支持SQL的系统不同。它的目的是表明，与OLTP数据库中的类似查询不同，这是一个繁重的操作，不是为频繁使用而设计。

`filter_expr`必须是`UInt8`类型。这个查询将指定列的值更新为行中相应表达式的值，对于这些行，`filter_expr`取值为非零。使用`CAST`操作符将数值映射到列的类型上。不支持更新用于计算主键或分区键的列。

一个查询可以包含几个由逗号分隔的命令。

查询处理的同步性由 [mutations_sync](../../../operations/settings/settings.md#mutations_sync) 设置定义。 默认情况下，它是异步操作。


**更多详情请参阅**

-   [变更 Mutations](../../../sql-reference/statements/alter/index.md#mutations)
-   [ALTER查询的同步性问题](../../../sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
-   [mutations_sync](../../../operations/settings/settings.md#mutations_sync) setting

