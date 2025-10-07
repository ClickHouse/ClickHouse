---
description: 'Documentation for TRUNCATE Statements'
sidebar_label: 'TRUNCATE'
sidebar_position: 52
slug: /sql-reference/statements/truncate
title: 'TRUNCATE Statements'
doc_type: 'reference'
---

# TRUNCATE Statements

The `TRUNCATE` statement in ClickHouse is used to quickly remove all data from a table or database while preserving their structure.

## TRUNCATE TABLE {#truncate-table}
```sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
```
<br/>
| Parameter           | Description                                                                                       |
|---------------------|---------------------------------------------------------------------------------------------------|
| `IF EXISTS`         | Prevents an error if the table does not exist. If omitted, the query returns an error.            |
| `db.name`           | Optional database name.                                                                           |
| `ON CLUSTER cluster`| Runs the command across a specified cluster.                                                      |
| `SYNC`              | Makes the truncation synchronous across replicas when using replicated tables. If omitted, truncation happens asynchronously by default. |

You can use the [alter_sync](/operations/settings/settings#alter_sync) setting to set up waiting for actions to be executed on replicas.

You can specify how long (in seconds) to wait for inactive replicas to execute `TRUNCATE` queries with the [replication_wait_for_inactive_replica_timeout](/operations/settings/settings#replication_wait_for_inactive_replica_timeout) setting.

:::note    
If the `alter_sync` is set to `2` and some replicas are not active for more than the time, specified by the `replication_wait_for_inactive_replica_timeout` setting, then an exception `UNFINISHED` is thrown.
:::

The `TRUNCATE TABLE` query is **not supported** for the following table engines:

- [`View`](../../engines/table-engines/special/view.md)
- [`File`](../../engines/table-engines/special/file.md)
- [`URL`](../../engines/table-engines/special/url.md)
- [`Buffer`](../../engines/table-engines/special/buffer.md)
- [`Null`](../../engines/table-engines/special/null.md)

## TRUNCATE ALL TABLES {#truncate-all-tables}
```sql
TRUNCATE [ALL] TABLES FROM [IF EXISTS] db [LIKE | ILIKE | NOT LIKE '<pattern>'] [ON CLUSTER cluster]
```
<br/>
| Parameter                  | Description                                       |
|----------------------------|---------------------------------------------------|
| `ALL`                      | Removes data from all tables in the database.     |
| `IF EXISTS`                | Prevents an error if the database does not exist. |
| `db`                       | The database name.                                |
| `LIKE \| ILIKE \| NOT LIKE '<pattern>'` | Filters tables by pattern.           |
| `ON CLUSTER cluster`       | Runs the command across a cluster.                |

Removes all data from all tables in a database.

## TRUNCATE DATABASE {#truncate-database}
```sql
TRUNCATE DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```
<br/>
| Parameter            | Description                                       |
|----------------------|---------------------------------------------------|
| `IF EXISTS`          | Prevents an error if the database does not exist. |
| `db`                 | The database name.                                |
| `ON CLUSTER cluster` | Runs the command across a specified cluster.      |

Removes all tables from a database but keeps the database itself. When the clause `IF EXISTS` is omitted, the query returns an error if the database does not exist.

:::note
`TRUNCATE DATABASE` is not supported for `Replicated` databases. Instead, just `DROP` and `CREATE` the database.
:::