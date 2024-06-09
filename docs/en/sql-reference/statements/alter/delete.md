---
slug: /en/sql-reference/statements/alter/delete
sidebar_position: 39
sidebar_label: DELETE
---

# ALTER TABLE ... DELETE Statement

``` sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE WHERE filter_expr
```

Deletes data matching the specified filtering expression. Implemented as a [mutation](/docs/en/sql-reference/statements/alter/index.md#mutations).


:::note
The `ALTER TABLE` prefix makes this syntax different from most other systems supporting SQL. It is intended to signify that unlike similar queries in OLTP databases this is a heavy operation not designed for frequent use.  `ALTER TABLE` is considered a heavyweight operation that requires the underlying data to be merged before it is deleted. For MergeTree tables, consider using the [`DELETE FROM` query](/docs/en/sql-reference/statements/delete.md), which performs a lightweight delete and can be considerably faster.
:::

The `filter_expr` must be of type `UInt8`. The query deletes rows in the table for which this expression takes a non-zero value.

One query can contain several commands separated by commas.

The synchronicity of the query processing is defined by the [mutations_sync](/docs/en/operations/settings/settings.md/#mutations_sync) setting. By default, it is asynchronous.

**See also**

- [Mutations](/docs/en/sql-reference/statements/alter/index.md#mutations)
- [Synchronicity of ALTER Queries](/docs/en/sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
- [mutations_sync](/docs/en/operations/settings/settings.md/#mutations_sync) setting

## Related content

- Blog: [Handling Updates and Deletes in ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)
