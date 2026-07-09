---
description: '关于管理约束的文档'
sidebar_label: 'CONSTRAINT'
sidebar_position: 43
slug: /sql-reference/statements/alter/constraint
title: '管理约束'
doc_type: 'reference'
---

可使用以下语法添加、修改或删除约束：

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD CONSTRAINT [IF NOT EXISTS] constraint_name {CHECK|ASSUME} expression;
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY CONSTRAINT [IF EXISTS] constraint_name {CHECK|ASSUME} expression;
ALTER TABLE [db].name [ON CLUSTER cluster] DROP CONSTRAINT [IF EXISTS] constraint_name;
```

与创建表类似，约束既可以声明为 `CHECK` (在 `INSERT` 时强制执行) ，也可以声明为 `ASSUME` (由优化器信任而不做检查) 。两者之间的区别请参见 [constraints](../../../sql-reference/statements/create/table.md#constraints)。

`MODIFY CONSTRAINT` 会替换现有约束的声明，同时保留其在表定义中的位置。它还可以更改约束类型 (例如，从 `CHECK` 改为 `ASSUME`) 。这相当于删除该约束后，再用新声明重新添加一次。如果该约束不存在，则查询会抛出错误，除非指定了 `IF EXISTS`。

有关 [constraints](../../../sql-reference/statements/create/table.md#constraints) 的更多信息，请参见该文档。

这些查询会添加、更改或删除表中与约束有关的元数据，因此会立即生效。

:::tip
如果约束是新增或修改的，**不会**对现有数据执行约束检查。
:::

对复制表所做的所有更改都会广播到 ZooKeeper，并同样应用到其他副本上。