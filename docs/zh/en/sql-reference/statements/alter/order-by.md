---
description: '关于修改键表达式的文档'
sidebar_label: 'ORDER BY'
sidebar_position: 41
slug: /sql-reference/statements/alter/order-by
title: '修改键表达式'
doc_type: 'reference'
---

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY ORDER BY new_expression
```

该命令会将表的[排序键](../../../engines/table-engines/mergetree-family/mergetree.md)更改为 `new_expression` (一个表达式或由多个表达式组成的元组) 。主键保持不变。

该命令之所以是轻量级的，是因为它只会更改元数据。为了保持数据分区片段中的行按排序键表达式排序这一特性，不能向排序键中添加包含现有列的表达式 (只能添加在同一 `ALTER` 查询中通过 `ADD COLUMN` 命令新增的列，且该列没有默认值) 。

:::note
它仅适用于 [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) 家族中的表 (包括[复制](../../../engines/table-engines/mergetree-family/replication.md)表) 。
:::