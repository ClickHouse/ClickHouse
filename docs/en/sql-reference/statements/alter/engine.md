---
description: 'Documentation for changing the engine of a MergeTree-family table'
sidebar_label: 'ENGINE'
sidebar_position: 42
slug: /sql-reference/statements/alter/engine
title: 'Changing the Table Engine'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<ExperimentalBadge/>

## MODIFY ENGINE {#modify-engine}

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY ENGINE = new_engine
```

Changes the engine of a [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md)-family table, for example a plain `MergeTree` table into a [`ReplacingMergeTree`](../../../engines/table-engines/mergetree-family/replacingmergetree.md), [`SummingMergeTree`](../../../engines/table-engines/mergetree-family/summingmergetree.md), [`AggregatingMergeTree`](../../../engines/table-engines/mergetree-family/aggregatingmergetree.md), [`CollapsingMergeTree`](../../../engines/table-engines/mergetree-family/collapsingmergetree.md), [`VersionedCollapsingMergeTree`](../../../engines/table-engines/mergetree-family/versionedcollapsingmergetree.md) or [`GraphiteMergeTree`](../../../engines/table-engines/mergetree-family/graphitemergetree.md).

Only the merge semantics change. The columns, `ORDER BY`, `PARTITION BY`, `PRIMARY KEY`, `SAMPLE BY` and table settings are kept. Existing data is not rewritten: existing parts are re-merged under the new engine's semantics by ordinary background merges and by `OPTIMIZE TABLE ... FINAL`.

```sql
-- Turn a MergeTree table into a ReplacingMergeTree
ALTER TABLE my_table MODIFY ENGINE = ReplacingMergeTree;

-- With a version column
ALTER TABLE my_table MODIFY ENGINE = ReplacingMergeTree(version);
```

If the target engine requires a column that does not exist yet (the sign column for `CollapsingMergeTree` / `VersionedCollapsingMergeTree`, or the version column for `VersionedCollapsingMergeTree`), it can be added in the same statement, in a command placed before `MODIFY ENGINE`:

```sql
ALTER TABLE my_table
    ADD COLUMN sign Int8 DEFAULT 1,
    MODIFY ENGINE = CollapsingMergeTree(sign);
```

The new engine must use the same arguments that engine accepts in `CREATE TABLE` (for example, the sign column for `CollapsingMergeTree`, the optional version and `is_deleted` columns for `ReplacingMergeTree`, the columns to sum for `SummingMergeTree`). The required columns must exist after applying any column commands in the same `ALTER`, and have the types the engine expects.

:::note
This feature is experimental. Enable it with the [`allow_experimental_alter_modify_engine`](/operations/settings/settings#allow_experimental_alter_modify_engine) setting.

It is only supported for non-replicated [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md)-family tables. The engine family is fixed: a non-replicated table cannot be turned into a `Replicated*MergeTree` (or vice versa) with this command.

The new engine is written to the table's metadata and takes effect after the table is reloaded (for example with `DETACH TABLE` / `ATTACH TABLE`, or on the next server start).
:::
