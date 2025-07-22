---
description: 'Documentation for ALTER TABLE ... UPDATE Statements'
sidebar_label: 'UPDATE'
sidebar_position: 40
slug: /sql-reference/statements/alter/update
title: 'ALTER TABLE ... UPDATE Statements'
---

# ALTER TABLE ... UPDATE Statements

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_id] WHERE filter_expr
```

Manipulates data matching the specified filtering expression. Implemented as a [mutation](/sql-reference/statements/alter/index.md#mutations).

:::note    
The `ALTER TABLE` prefix makes this syntax different from most other systems supporting SQL. It is intended to signify that unlike similar queries in OLTP databases this is a heavy operation not designed for frequent use.
:::

The `filter_expr` must be of type `UInt8`. This query updates values of specified columns to the values of corresponding expressions in rows for which the `filter_expr` takes a non-zero value. Values are cast to the column type using the `CAST` operator. Updating columns that are used in the calculation of the primary or the partition key is not supported.

One query can contain several commands separated by commas.

The synchronicity of the query processing is defined by the [mutations_sync](/operations/settings/settings.md/#mutations_sync) setting. By default, it is asynchronous.

**See also**

- [Mutations](/sql-reference/statements/alter/index.md#mutations)
- [Synchronicity of ALTER Queries](/sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
- [mutations_sync](/operations/settings/settings.md/#mutations_sync) setting


## Related content {#related-content}

- Blog: [Handling Updates and Deletes in ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)
