---
description: 'Documentation for Apply patches from lightweight updates'
sidebar_label: 'APPLY PATCHES'
sidebar_position: 47
slug: /sql-reference/statements/alter/apply-patches
title: 'Apply patches from lightweight updates'
doc_type: 'reference'
---

import BetaBadge from '@theme/badges/BetaBadge';

<BetaBadge/>

# Apply patches from lightweight updates

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] APPLY PATCHES [IN PARTITION partition_id]
```

The command manually triggers the materialization of patch parts created by [lightweight `UPDATE`](/sql-reference/statements/update) statements. It forcefully applies pending patches to the data parts by rewriting only the affected columns, similar to a mutation but more efficient.

:::note
- It only works for tables in the [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) family (including [replicated](../../../engines/table-engines/mergetree-family/replication.md) tables).
- This is a mutation operation and executes asynchronously in the background.
:::

## When to use APPLY PATCHES

Patch parts are normally applied automatically during merges when the [`apply_patches_on_merge`](/operations/settings/merge-tree-settings#apply_patches_on_merge) setting is enabled (default). However, you may want to manually trigger patch application in these scenarios:

- To reduce the overhead of applying patches during `SELECT` queries
- To consolidate multiple patch parts before they accumulate
- To prepare data for backup or export with patches already materialized
- When `apply_patches_on_merge` is disabled and you want to control when patches are applied

## Performance characteristics

Unlike heavyweight `ALTER TABLE ... UPDATE` mutations that rewrite entire data parts, `APPLY PATCHES`:
- Only rewrites the columns that were updated
- Is more efficient when only a small subset of columns have been modified
- Can be executed in parallel with other operations
- Does not block lightweight `UPDATE` statements

## Examples

Apply all pending patches for a table:
```sql
ALTER TABLE my_table APPLY PATCHES;
```

Apply patches only for a specific partition:
```sql
ALTER TABLE my_table APPLY PATCHES IN PARTITION '2024-01';
```

Combine with other operations:
```sql
ALTER TABLE my_table APPLY PATCHES, UPDATE column = value WHERE condition;
```

## Monitoring patch application

You can monitor the progress of patch application using the [`system.mutations`](/operations/system-tables/mutations) table:

```sql
SELECT * FROM system.mutations
WHERE table = 'my_table' AND command LIKE '%APPLY PATCHES%';
```

## See also

- [Lightweight `UPDATE`](/sql-reference/statements/update) - Create patch parts with lightweight updates
- [Heavyweight `UPDATE`](/sql-reference/statements/alter/update) - Traditional UPDATE mutations
- [`apply_patches_on_merge` setting](/operations/settings/merge-tree-settings#apply_patches_on_merge) - Control automatic patch application during merges
