---
toc_priority: 43
toc_title: 约束
---

# 操作约束 {#manipulations-with-constraints}

约束可以使用以下语法添加或删除:

``` sql
ALTER TABLE [db].name ADD CONSTRAINT constraint_name CHECK expression;
ALTER TABLE [db].name DROP CONSTRAINT constraint_name;
```

查看[constraints](../../../sql-reference/statements/create/table.md#constraints)。

查询将从表中添加或删除关于约束的元数据，因此它们将被立即处理。

!!! warning "警告"
    如果已有数据被添加，约束检查**将不会被执行**。

复制表上的所有更改都会被广播到ZooKeeper，并应用到其他副本上。