---
slug: /en/sql-reference/statements/delete
sidebar_position: 36
sidebar_label: DELETE
description: Lightweight deletes simplify the process of deleting data from the database.
keywords: [delete]
title: The Lightweight DELETE Statement
---

The lightweight `DELETE` statement removes rows from the table `[db.]table` that match the expression `expr`. It is only available for the *MergeTree table engine family.

``` sql
DELETE FROM [db.]table [ON CLUSTER cluster] [IN PARTITION partition_expr] WHERE expr;
```

It is called "lightweight `DELETE`" to contrast it to the [ALTER TABLE ... DELETE](/en/sql-reference/statements/alter/delete) command, which is a heavyweight process.

## Examples

```sql
-- Deletes all rows from the `hits` table where the `Title` column contains the text `hello`
DELETE FROM hits WHERE Title LIKE '%hello%';
```

## Lightweight `DELETE` does not delete data immediately

Lightweight `DELETE` is implemented as a [mutation](/en/sql-reference/statements/alter#mutations) that marks rows as deleted but does not immediately physically delete them.

By default, `DELETE` statements wait until marking the rows as deleted is completed before returning. This can take a long time if the amount of data is large. Alternatively, you can run it asynchronously in the background using the setting [`lightweight_deletes_sync`](/en/operations/settings/settings#lightweight_deletes_sync). If disabled, the `DELETE` statement is going to return immediately, but the data can still be visible to queries until the background mutation is finished.

The mutation does not physically delete the rows that have been marked as deleted, this will only happen during the next merge. As a result, it is possible that for an unspecified period, data is not actually deleted from storage and is only marked as deleted.

If you need to guarantee that your data is deleted from storage in a predictable time, consider using the table setting [`min_age_to_force_merge_seconds`](https://clickhouse.com/docs/en/operations/settings/merge-tree-settings#min_age_to_force_merge_seconds). Or you can use the [ALTER TABLE ... DELETE](/en/sql-reference/statements/alter/delete) command. Note that deleting data using `ALTER TABLE ... DELETE` may consume significant resources as it recreates all affected parts.

## Deleting large amounts of data

Large deletes can negatively affect ClickHouse performance. If you are attempting to delete all rows from a table, consider using the [`TRUNCATE TABLE`](/en/sql-reference/statements/truncate) command.

If you anticipate frequent deletes, consider using a [custom partitioning key](/en/engines/table-engines/mergetree-family/custom-partitioning-key). You can then use the [`ALTER TABLE ... DROP PARTITION`](/en/sql-reference/statements/alter/partition#drop-partitionpart) command to quickly drop all rows associated with that partition.

## Limitations of lightweight `DELETE`

### Lightweight `DELETE`s with projections

By default, `DELETE` does not work for tables with projections. This is because rows in a projection may be affected by a `DELETE` operation. But there is a [MergeTree setting](https://clickhouse.com/docs/en/operations/settings/merge-tree-settings) `lightweight_mutation_projection_mode` to change the behavior.

## Performance considerations when using lightweight `DELETE`

**Deleting large volumes of data with the lightweight `DELETE` statement can negatively affect SELECT query performance.**

The following can also negatively impact lightweight `DELETE` performance:

- A heavy `WHERE` condition in a `DELETE` query.
- If the mutations queue is filled with many other mutations, this can possibly lead to performance issues as all mutations on a table are executed sequentially.
- The affected table has a very large number of data parts.
- Having a lot of data in compact parts. In a Compact part, all columns are stored in one file.

## Delete permissions

`DELETE` requires the `ALTER DELETE` privilege. To enable `DELETE` statements on a specific table for a given user, run the following command:

```sql
GRANT ALTER DELETE ON db.table to username;
```

## How lightweight DELETEs work internally in ClickHouse

1. **A "mask" is applied to affected rows**

   When a `DELETE FROM table ...` query is executed, ClickHouse saves a mask where each row is marked as either “existing” or as “deleted”. Those “deleted” rows are omitted for subsequent queries. However, rows are actually only removed later by subsequent merges. Writing this mask is much more lightweight than what is done by an `ALTER TABLE ... DELETE` query.

   The mask is implemented as a hidden `_row_exists` system column that stores `True` for all visible rows and `False` for deleted ones. This column is only present in a part if some rows in the part were deleted. This column does not exist when a part has all values equal to `True`.

2. **`SELECT` queries are transformed to include the mask**

   When a masked column is used in a query, the `SELECT ... FROM table WHERE condition` query internally is extended by the predicate on `_row_exists` and is transformed to:
   ```sql
   SELECT ... FROM table PREWHERE _row_exists WHERE condition
   ```
   At execution time, the column `_row_exists` is read to determine which rows should not be returned. If there are many deleted rows, ClickHouse can determine which granules can be fully skipped when reading the rest of the columns.

3. **`DELETE` queries are transformed to `ALTER TABLE ... UPDATE` queries**

   The `DELETE FROM table WHERE condition` is translated into an `ALTER TABLE table UPDATE _row_exists = 0 WHERE condition` mutation.

   Internally, this mutation is executed in two steps:

   1. A `SELECT count() FROM table WHERE condition` command is executed for each individual part to determine if the part is affected.

   2. Based on the commands above, affected parts are then mutated, and hardlinks are created for unaffected parts. In the case of wide parts, the `_row_exists` column for each row is updated, and all other columns' files are hardlinked. For compact parts, all columns are re-written because they are all stored together in one file.

   From the steps above, we can see that lightweight `DELETE` using the masking technique improves performance over traditional `ALTER TABLE ... DELETE` because it does not re-write all the columns' files for affected parts.

## Related content

- Blog: [Handling Updates and Deletes in ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)
