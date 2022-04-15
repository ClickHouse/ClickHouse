---
toc_priority: 39
toc_title: DELETE
---

# ALTER TABLE … DELETE 语句 {#alter-mutations}

``` sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE WHERE filter_expr
```

删除匹配指定过滤表达式的数据。实现为[突变](../../../sql-reference/statements/alter/index.md#mutations).

!!! note "备注"
    `ALTER TABLE`前缀使得这个语法不同于大多数其他支持SQL的系统。它的目的是表示，与OLTP数据库中的类似查询不同，这是一个不为经常使用而设计的繁重操作。

`filter_expr` 的类型必须是`UInt8`。该查询删除表中该表达式接受非零值的行。

一个查询可以包含多个用逗号分隔的命令。

查询处理的同步性由[mutations_sync](../../../operations/settings/settings.md#mutations_sync)设置定义。缺省情况下，是异步的。

**详见**

-   [突变](../../../sql-reference/statements/alter/index.md#mutations)
-   [ALTER查询的同步性](../../../sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
-   [mutations_sync](../../../operations/settings/settings.md#mutations_sync) setting
