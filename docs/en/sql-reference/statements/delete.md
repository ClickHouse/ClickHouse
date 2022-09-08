---
slug: /en/sql-reference/statements/delete
sidebar_position: 36
sidebar_label: DELETE
---

# DELETE Statement

``` sql
DELETE FROM [db.]table [WHERE expr]
```

For MergeTree tables, `DELETE FROM` performs a lightweight delete on the given table, which means that the deleted rows are marked as deleted immediately and deleted rows will be filtered out of all subsequent queries. The underlying data is permanently deleted whenever merges occur.

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

The [traditional way to delete rows](./alter/delete.md) in ClickHouse was to use `ALTER TABLE ... DELETE`, which is still a valid method for deleting rows. However, in most use cases the new lightweight `DELETE FROM` behavior will be considerably faster.

:::info
Lightweight deletes are asynchronous by default. Set `mutations_sync` equal to 1 to wait for one replica to process the statement, and set `mutations_sync` to 2 to wait for all replicas.
:::

