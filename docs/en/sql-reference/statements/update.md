---
description: 'Lightweight updates simplify the process of updating data in the database using patch parts.'
keywords: ['update']
sidebar_label: 'UPDATE'
sidebar_position: 39
slug: /sql-reference/statements/update
title: 'The Lightweight UPDATE Statement'
doc_type: 'reference'
---

import BetaBadge from '@theme/badges/BetaBadge';

<BetaBadge/>

:::note
Lightweight updates are currently beta.
If you run into problems, kindly open an issue in the [ClickHouse repository](https://github.com/clickhouse/clickhouse/issues).
:::

The lightweight `UPDATE` statement updates rows in a table `[db.]table` that match the expression `filter_expr`.
It is called "lightweight update" to contrast it to the [`ALTER TABLE ... UPDATE`](/sql-reference/statements/alter/update) query, which is a heavyweight process that rewrites entire columns in data parts.
It is only available for the [`MergeTree`](/engines/table-engines/mergetree-family/mergetree) table engine family.

```sql
UPDATE [db.]table [ON CLUSTER cluster] SET column1 = expr1 [, ...] [IN PARTITION partition_expr] WHERE filter_expr;
```

The `filter_expr` must be of type `UInt8`. This query updates values of the specified columns to the values of the corresponding expressions in rows for which the `filter_expr` takes a non-zero value.
Values are cast to the column type using the `CAST` operator. Updating columns used in the calculation of the primary or partition keys is not supported.

## Examples {#examples}

```sql
UPDATE hits SET Title = 'Updated Title' WHERE EventDate = today();

UPDATE wikistat SET hits = hits + 1, time = now() WHERE path = 'ClickHouse';
```

## Lightweight updates do not update data immediately {#lightweight-update-does-not-update-data-immediately}

Lightweight `UPDATE` is implemented using **patch parts** - a special kind of data part that contains only the updated columns and rows.
A lightweight `UPDATE` creates patch parts but does not immediately modify the original data physically in storage.
The process of updating is similar to a `INSERT ... SELECT ...` query but the `UPDATE` query waits until the patch part creation is completed before returning.

The updated values are:
- **Immediately visible** in `SELECT` queries through patches application
- **Physically materialized** only during subsequent merges and mutations
- **Automatically cleaned up** once all active parts have the patches materialized
## Lightweight updates requirements {#lightweight-update-requirements}

Lightweight updates are supported for [`MergeTree`](/engines/table-engines/mergetree-family/mergetree), [`ReplacingMergeTree`](/engines/table-engines/mergetree-family/replacingmergetree), [`CollapsingMergeTree`](/engines/table-engines/mergetree-family/collapsingmergetree) engines and their [`Replicated`](/engines/table-engines/mergetree-family/replication.md) and [`Shared`](/cloud/reference/shared-merge-tree) versions.

To use lightweight updates, materialization of `_block_number` and `_block_offset` columns must be enabled using table settings [`enable_block_number_column`](/operations/settings/merge-tree-settings#enable_block_number_column) and [`enable_block_offset_column`](/operations/settings/merge-tree-settings#enable_block_offset_column).

## Lightweight deletes {#lightweight-delete}

A [lightweight `DELETE`](/sql-reference/statements/delete) query can be run as a lightweight `UPDATE` instead of a `ALTER UPDATE` mutation. The implementation of lightweight `DELETE` is controlled by setting [`lightweight_delete_mode`](/operations/settings/settings#lightweight_delete_mode).

## Performance considerations {#performance-considerations}

**Advantages of lightweight updates:**
- The latency of the update is comparable to the latency of the `INSERT ... SELECT ...` query
- Only updated columns and values are written, not entire columns in data parts
- No need to wait for currently running merges/mutations to complete, therefore the latency of an update is predictable
- Parallel execution of lightweight updates is possible

**Potential performance impacts:**
- Adds an overhead to `SELECT` queries that need to apply patches
- [Skipping indexes](/engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-data_skipping-indexes) will not be used for columns in data parts that have patches to be applied. [Projections](/engines/table-engines/mergetree-family/mergetree.md/#projections) will not be used if there are patch parts for table, including for data parts that don't have patches to be applied.
- Small updates which are too frequent may lead to a "too many parts" error. It is recommended to batch several updates into a single query, for example by putting ids for updates in a single `IN` clause in the `WHERE` clause
- Lightweight updates are designed to update small amounts of rows (up to about 10% of the table). If you need to update a larger amount, it is recommended to use the [`ALTER TABLE ... UPDATE`](/sql-reference/statements/alter/update) mutation

## Concurrent operations {#concurrent-operations}

Lightweight updates don't wait for currently running merges/mutations to complete unlike heavy mutations.
The consistency of concurrent lightweight updates is controlled by settings [`update_sequential_consistency`](/operations/settings/settings#update_sequential_consistency) and [`update_parallel_mode`](/operations/settings/settings#update_parallel_mode).

## Update permissions {#update-permissions}

`UPDATE` requires the `ALTER UPDATE` privilege. To enable `UPDATE` statements on a specific table for a given user, run:

```sql
GRANT ALTER UPDATE ON db.table TO username;
```

## Details of the implementation {#details-of-the-implementation}

Patch parts are the same as the regular parts, but contain only updated columns and several system columns:
- `_part` - the name of the original part
- `_part_offset` - the row number in the original part
- `_block_number` - the block number of the row in the original part
- `_block_offset` - the block offset of the row in the original part
- `_data_version` - the data version of the updated data (block number allocated for the `UPDATE` query)

On average it gives about 40 bytes (uncompressed data) of overhead per updated row in the patch parts.
System columns help to find rows in the original part which should be updated.
System columns are related to the [virtual columns](/engines/table-engines/mergetree-family/mergetree.md/#virtual-columns) in the original part, which are added for reading if patch parts should be applied.
Patch parts are sorted by `_part` and `_part_offset`.

Patch parts belong to different partitions than the original part.
The partition id of the patch part is `patch-<hash of column names in patch part>-<original_partition_id>`.
Therefore patch parts with different columns are stored in different partitions.
For example three updates `SET x = 1 WHERE <cond>`, `SET y = 1 WHERE <cond>` and `SET x = 1, y = 1 WHERE <cond>` will create three patch parts in three different partitions.

Patch parts can be merged among themselves to reduce the amount of applied patches on `SELECT` queries and reduce the overhead. Merging of patch parts uses the [replacing](/engines/table-engines/mergetree-family/replacingmergetree) merge algorithm with `_data_version` as a version column.
Therefore patch parts always store the latest version for each updated row in the part.

Lightweight updates don't wait for currently running merges and mutations to finish and always use a current snapshot of data parts to execute an update and produce a patch part.
Because of that there can be two cases of applying patch parts.

For example if we read part `A`, we need to apply patch part `X`:
- if `X` contains part `A` itself. It happens if `A` was not participating in merge when `UPDATE` was executed.
- if `X` contains part `B` and `C`, which are covered by part `A`. It happens if there was a merge (`B`, `C`) -> `A` running when `UPDATE` was executed.

For these two cases there are two ways to apply patch parts respectively:
- Using merge by sorted columns `_part`, `_part_offset`.
- Using join by `_block_number`, `_block_offset` columns.

The join mode is slower and requires more memory than the merge mode, but it is used less often.

## Related Content {#related-content}

- [`ALTER UPDATE`](/sql-reference/statements/alter/update) - Heavy `UPDATE` operations
- [Lightweight `DELETE`](/sql-reference/statements/delete) - Lightweight `DELETE` operations
