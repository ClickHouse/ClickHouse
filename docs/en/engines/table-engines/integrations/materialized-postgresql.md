---
slug: /en/engines/table-engines/integrations/materialized-postgresql
sidebar_position: 130
sidebar_label: MaterializedPostgreSQL
---

# [experimental] MaterializedPostgreSQL

Creates ClickHouse table with an initial data dump of PostgreSQL table and starts replication process, i.e. executes background job to apply new changes as they happen on PostgreSQL table in the remote PostgreSQL database.

:::note
This table engine is experimental. To use it, set `allow_experimental_materialized_postgresql_table` to 1 in your configuration files or by using the `SET` command:
```sql
SET allow_experimental_materialized_postgresql_table=1
```
:::


If more than one table is required, it is highly recommended to use the [MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md) database engine instead of the table engine and use the `materialized_postgresql_tables_list` setting, which specifies the tables to be replicated (will also be possible to add database `schema`). It will be much better in terms of CPU, fewer connections and fewer replication slots inside the remote PostgreSQL database.

## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_table', 'postgres_user', 'postgres_password')
PRIMARY KEY key;
```

**Engine Parameters**

- `host:port` — PostgreSQL server address.
- `database` — Remote database name.
- `table` — Remote table name.
- `user` — PostgreSQL user.
- `password` — User password.

## Requirements {#requirements}

1. The [wal_level](https://www.postgresql.org/docs/current/runtime-config-wal.html) setting must have a value `logical` and `max_replication_slots` parameter must have a value at least `2` in the PostgreSQL config file.

2. A table with `MaterializedPostgreSQL` engine must have a primary key — the same as a replica identity index (by default: primary key) of a PostgreSQL table (see [details on replica identity index](../../../engines/database-engines/materialized-postgresql.md#requirements)).

3. Only database [Atomic](https://en.wikipedia.org/wiki/Atomicity_(database_systems)) is allowed.

4. The `MaterializedPostgreSQL` table engine only works for PostgreSQL versions >= 11 as the implementation requires the [pg_replication_slot_advance](https://pgpedia.info/p/pg_replication_slot_advance.html) PostgreSQL function.

## Virtual columns {#virtual-columns}

- `_version` — Transaction counter. Type: [UInt64](../../../sql-reference/data-types/int-uint.md).

- `_sign` — Deletion mark. Type: [Int8](../../../sql-reference/data-types/int-uint.md). Possible values:
    - `1` — Row is not deleted,
    - `-1` — Row is deleted.

These columns do not need to be added when a table is created. They are always accessible in `SELECT` query.
`_version` column equals `LSN` position in `WAL`, so it might be used to check how up-to-date replication is.

``` sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;

SELECT key, value, _version FROM postgresql_db.postgresql_replica;
```

:::note
Replication of [**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html) values is not supported. The default value for the data type will be used.
:::
