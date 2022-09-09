---
slug: /en/sql-reference/statements/delete
sidebar_position: 36
sidebar_label: DELETE
---

# DELETE Statement

``` sql
DELETE FROM [db.]table [WHERE expr]
```

`DELETE FROM` removes rows from table `[db.]table` that match expression `expr`. The deleted rows are marked as deleted immediately and will be automatically filtered out of all subsequent queries. Cleanup of data happens asynchronously in background. This feature is only available for MergeTree table engine family.

For example, the following query deletes all rows from the `hits` table where the `Title` column contains the text `hello`:

```sql
DELETE FROM hits WHERE Title LIKE '%hello%';
```


:::note
This feature is experimental and requires you to set `allow_experimental_lightweight_delete` to true:

```sql
SET allow_experimental_lightweight_delete = true;
```

:::

An [alternative way to delete rows](./alter/delete.md) in ClickHouse is `ALTER TABLE ... DELETE`, which might be more efficient if you do bulk deletes only occasionally and don't need the operation to be applied instantly. In most use cases the new lightweight `DELETE FROM` behavior will be considerably faster.

:::info
Lightweight deletes are asynchronous by default. Set `mutations_sync` equal to 1 to wait for one replica to process the statement, and set `mutations_sync` to 2 to wait for all replicas.
:::

