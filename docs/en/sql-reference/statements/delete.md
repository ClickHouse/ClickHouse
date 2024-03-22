---
slug: /en/sql-reference/statements/delete
sidebar_position: 36
sidebar_label: DELETE
description: Lightweight deletes simplify the process of deleting data from the database.
keywords: [delete]
title: DELETE Statement
---

``` sql
DELETE FROM [db.]table [ON CLUSTER cluster] WHERE expr
```

`DELETE FROM` removes rows from the table `[db.]table` that match the expression `expr`. The deleted rows are marked as deleted immediately and will be automatically filtered out of all subsequent queries. Cleanup of data happens asynchronously in the background. This feature is only available for the MergeTree table engine family.

For example, the following query deletes all rows from the `hits` table where the `Title` column contains the text `hello`:

```sql
DELETE FROM hits WHERE Title LIKE '%hello%';
```

Lightweight deletes are asynchronous by default. Set `mutations_sync` equal to 1 to wait for one replica to process the statement, and set `mutations_sync` to 2 to wait for all replicas.

:::note
`DELETE FROM` requires the `ALTER DELETE` privilege:
```sql
grant ALTER DELETE ON db.table to username;
```
:::

## Lightweight Delete Internals

The idea behind Lightweight Delete is that when a `DELETE FROM table ...` query is executed ClickHouse only saves a mask where each row is marked as either “existing” or as “deleted”. Those “deleted” rows become invisible for subsequent queries, but physically the rows are removed only later by subsequent merges. Writing this mask is usually much more lightweight than what is done by `ALTER table DELETE ...` query.

### How it is implemented
The mask is implemented as a hidden `_row_exists` system column that stores True for all visible rows and False for deleted ones. This column is only present in a part if some rows in this part were deleted. In other words, the column is not persisted when it has all values equal to True.

## SELECT query
When the column is present `SELECT ... FROM table WHERE condition` query internally is extended by an additional predicate on `_row_exists` and becomes similar to
```sql
    SELECT ... FROM table PREWHERE _row_exists WHERE condition
```
At execution time the column `_row_exists` is read to figure out which rows are not visible and if there are many deleted rows it can figure out which granules can be fully skipped when reading the rest of the columns.

## DELETE query
`DELETE FROM table WHERE condition` is translated into `ALTER table UPDATE _row_exists = 0 WHERE condition` mutation. Internally this mutation is executed in 2 steps:
1. `SELECT count() FROM table WHERE condition` for each individual part to figure out if the part is affected.
2. Mutate affected parts, and make hardlinks for unaffected parts. Mutating a part in fact only writes `_row_exists` column and just hardlinks all other columns’ files in the case of Wide parts. But for Compact parts, all columns are rewritten because they all are stored together in one file.

So if we compare Lightweight Delete to `ALTER DELETE` in the first step they both do the same thing to figure out which parts are affected, but in the second step `ALTER DELETE` does much more work because it reads and rewrites all columns’ files for the affected parts.

With the described implementation now we can see what can negatively affect 'DELETE FROM' execution time:
- Heavy WHERE condition in DELETE query
- Mutations queue filled with other mutations, because all mutations on a table are executed sequentially
- Table having a very large number of data parts
- Having a lot of data in Compact parts—in a Compact part, all columns are stored in one file.

:::note
Currently, Lightweight delete does not work for tables with projection as rows in projection may be affected and require the projection to be rebuilt. Rebuilding projection makes the deletion not lightweight, so this is not supported. 
:::

## Related content

- Blog: [Handling Updates and Deletes in ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)
