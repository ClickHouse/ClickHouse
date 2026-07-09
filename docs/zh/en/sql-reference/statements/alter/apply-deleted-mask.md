---
description: '有关对已删除行应用掩码的文档'
sidebar_label: 'APPLY DELETED MASK'
sidebar_position: 46
slug: /sql-reference/statements/alter/apply-deleted-mask
title: '对已删除行应用掩码'
doc_type: 'reference'
---

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] APPLY DELETED MASK [IN PARTITION partition_id]
```

该命令会应用由[轻量级删除](/zh/sql-reference/statements/delete)创建的掩码，并强制从磁盘中删除已标记为已删除的行。该命令属于重量级变更，在语义上等同于查询 `ALTER TABLE [db].name DELETE WHERE _row_exists = 0`。

:::note
它仅适用于 [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) 家族中的表 (包括 [Replicated](../../../engines/table-engines/mergetree-family/replication.md) 表) 。
:::

**另请参见**

* [轻量级删除](/zh/sql-reference/statements/delete)
* [重量级删除](/zh/sql-reference/statements/alter/delete.md)