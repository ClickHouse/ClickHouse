---
description: 'Documentation for Apply mask of deleted rows'
sidebar_label: 'APPLY DELETED MASK'
sidebar_position: 46
slug: /sql-reference/statements/alter/apply-deleted-mask
title: 'Apply mask of deleted rows'
---

# Apply mask of deleted rows

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] APPLY DELETED MASK [IN PARTITION partition_id]
```

The command applies mask created by [lightweight delete](/sql-reference/statements/delete) and forcefully removes rows marked as deleted from disk. This command is a heavyweight mutation, and it semantically equals to query ```ALTER TABLE [db].name DELETE WHERE _row_exists = 0```.

:::note
It only works for tables in the [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) family (including [replicated](../../../engines/table-engines/mergetree-family/replication.md) tables).
:::

**See also**

- [Lightweight deletes](/sql-reference/statements/delete)
- [Heavyweight deletes](/sql-reference/statements/alter/delete.md)
