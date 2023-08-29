---
sidebar_position: 41
sidebar_label: ORDER BY
---

# 操作排序键表达式 {#manipulations-with-key-expressions}

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY ORDER BY new_expression
```
该命令将表的[排序键](../../../engines/table-engines/mergetree-family/mergetree.md)更改为 `new_expression`(表达式或表达式元组)。主键保持不变。

从某种意义上说，该命令是轻量级的，它只更改元数据。要保持数据部分行按排序键表达式排序的属性，您不能向排序键添加包含现有列的表达式(仅在相同的`ALTER`查询中由`ADD COLUMN`命令添加的列，没有默认的列值)。


!!! note "备注"
    它只适用于[`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md)表族(包括[replicated](../../../engines/table-engines/mergetree-family/replication.md)表)。
