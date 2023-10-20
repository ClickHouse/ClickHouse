---
slug: /en/sql-reference/statements/delete
sidebar_position: 36
sidebar_label: DELETE
description: Lightweight deletes simplify the process of deleting data from the database.
keywords: [delete]
title: DELETE Statement
---

The `DELETE` statement removes rows from the table `[db.]table` that match the expression `expr`.

``` sql
DELETE FROM [db.]table [ON CLUSTER cluster] WHERE expr;
```

Deleted rows are internally marked as deleted immediately and will be automatically filtered out of all subsequent queries. Cleanup of data happens asynchronously in the background. The `DELETE` statement is only available for the *MergeTree table engine family.

**Do not use the DELETE statement frequently as it can negatively affect ClickHouse performance.**

## Examples

```sql
-- Deletes all rows from the `hits` table where the `Title` column contains the text `hello`
DELETE FROM hits WHERE Title LIKE '%hello%';
```

## Handling large `DELETE`s

Large deletes can negatively affect ClickHouse performance. If you are attempting to delete all rows from a table, consider using the [`TRUNCATE TABLE`](/en/sql-reference/statements/truncate) command and then re-create the table.

If you anticipate frequent deletes, consider using a [customer partitioning key](/en/engines/table-engines/mergetree-family/custom-partitioning-key). You can then use the [`ALTER TABLE...DROP PARTITION`](/en/sql-reference/statements/alter/partition#drop-partitionpart) command to quickly drop all rows associated with that partition.

## Limitations of `DELETE`

### `DELETE`s are eventually consistent

`DELETE`s are asynchronous by default. This means that if you use `DELETE` in a multi-replica setup, your rows will not be deleted immediately. To change this behavior, set `mutations_sync` equal to 1 to wait for one replica to process the statement, or set `mutations_sync` to 2 to wait for all replicas.

### `DELETE`s do not work with projections

Currently, `DELETE` does not work for tables with projections. This is because rows in a projection may be affected by a `DELETE` operation and may require the projection to be rebuilt, negatively affecting `DELETE` performance.

## Delete permissions

`DELETE` requires the `ALTER DELETE` privilege. To enable `DELETE` statements on a specific table for a given user, run the following command:

```sql
GRANT ALTER DELETE ON db.table to username;
```

## How DELETEs work internally in ClickHouse

1. A "mask" is applied to affected rows

When a `DELETE FROM table ...` query is executed, ClickHouse saves a mask where each row is marked as either “existing” or as “deleted”. Those “deleted” rows are omitted for subsequent queries. However, rows are actually only removed later by subsequent merges. Writing this mask is much more lightweight than what is done by an `ALTER table DELETE ...` query.

The mask is implemented as a hidden `_row_exists` system column that stores `True` for all visible rows and `False` for deleted ones. This column is only present in a part if some rows in this part were deleted. This column does not exist when a part has all values equal to `True`.

2. `SELECT` queries are transformed to include the mask

When a masked column is used in a query, the `SELECT ... FROM table WHERE condition` query internally is extended by the predicate on `_row_exists` and is transformed to:
```sql
SELECT ... FROM table PREWHERE _row_exists WHERE condition
```
At execution time, the column `_row_exists` is read to determine which rows should not be returned. If there are many deleted rows, ClickHouse can determine which granules can be fully skipped when reading the rest of the columns.

3. `DELETE` queries are transformed to `ALTER table UPDATE` queries

The `DELETE FROM table WHERE condition` is translated into an `ALTER table UPDATE _row_exists = 0 WHERE condition` mutation.

Internally, this mutation is executed in two steps:

1. A `SELECT count() FROM table WHERE condition` command is executed for each individual part to determine if the part is affected.

2. Based on the commands above, affected parts are then mutated, and hardlinks are created for unaffected parts. In the case of wide parts, the `_row_exists` column for each row is updated and all other columns' files are hardlinked. For compact parts, all columns are re-written because they all are stored together in one file.

From the steps above, we can see that lightweight deletes using the masking technique improves performance over traditional `ALTER DELETE` commands because `ALTER DELETE` reads and re-writes all the columns' files for affected parts.

The following can negatively impact `DELETE` performance:

- A heavy `WHERE` condition in `DELETE` query
- If the mutations queue is filled with other mutations, this can possibly lead to performance issues as all mutations on a table are executed sequentially
- The affected table having a very large number of data parts
- Having a lot of data in compact parts. In a Compact part, all columns are stored in one file

## Related content

- Blog: [Handling Updates and Deletes in ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)
