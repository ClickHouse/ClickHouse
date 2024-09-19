---
slug: /en/sql-reference/statements/truncate
sidebar_position: 52
sidebar_label: TRUNCATE
---

# TRUNCATE Statements

## TRUNCATE TABLE
``` sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Removes all data from a table. When the clause `IF EXISTS` is omitted, the query returns an error if the table does not exist.

The `TRUNCATE` query is not supported for [View](../../engines/table-engines/special/view.md), [File](../../engines/table-engines/special/file.md), [URL](../../engines/table-engines/special/url.md), [Buffer](../../engines/table-engines/special/buffer.md) and [Null](../../engines/table-engines/special/null.md) table engines.

You can use the [alter_sync](../../operations/settings/settings.md#alter-sync) setting to set up waiting for actions to be executed on replicas.

You can specify how long (in seconds) to wait for inactive replicas to execute `TRUNCATE` queries with the [replication_wait_for_inactive_replica_timeout](../../operations/settings/settings.md#replication-wait-for-inactive-replica-timeout) setting.

:::note    
If the `alter_sync` is set to `2` and some replicas are not active for more than the time, specified by the `replication_wait_for_inactive_replica_timeout` setting, then an exception `UNFINISHED` is thrown.
:::

## TRUNCATE ALL TABLES
``` sql
TRUNCATE ALL TABLES FROM [IF EXISTS] db [ON CLUSTER cluster]
```

Removes all data from all tables in a database.

## TRUNCATE DATABASE
``` sql
TRUNCATE DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

Removes all tables from a database but keeps the database itself. When the clause `IF EXISTS` is omitted, the query returns an error if the database does not exist.
