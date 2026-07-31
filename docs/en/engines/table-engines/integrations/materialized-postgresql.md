---
description: 'Creates a ClickHouse table with an initial data dump of a PostgreSQL
  table and starts the replication process.'
sidebar_label: 'MaterializedPostgreSQL'
sidebar_position: 130
slug: /engines/table-engines/integrations/materialized-postgresql
title: 'MaterializedPostgreSQL table engine'
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# MaterializedPostgreSQL table engine

<ExperimentalBadge/>
<CloudNotSupportedBadge/>

:::note
ClickHouse Cloud users are recommended to use [ClickPipes](/integrations/clickpipes) for PostgreSQL replication to ClickHouse. This natively supports high-performance Change Data Capture (CDC) for PostgreSQL.
:::

Creates ClickHouse table with an initial data dump of PostgreSQL table and starts the replication process, i.e. it executes a background job to apply new changes as they happen on PostgreSQL table in the remote PostgreSQL database.

:::note
This table engine is experimental. To use it, set `allow_experimental_materialized_postgresql_table` to 1 in your configuration files or by using the `SET` command:

```sql
SET allow_experimental_materialized_postgresql_table=1
```
:::

If more than one table is required, it is highly recommended to use the [MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md) database engine instead of the table engine and use the `materialized_postgresql_tables_list` setting, which specifies the tables to be replicated (will also be possible to add database `schema`). It will be much better in terms of CPU, fewer connections and fewer replication slots inside the remote PostgreSQL database.

## Creating a table {#creating-a-table}

```sql
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

## High availability with Keeper coordination {#high-availability-with-keeper-coordination}

A PostgreSQL logical replication slot allows only one active consumer, so by default a `MaterializedPostgreSQL` table lives on a single ClickHouse server: its nested table is a plain `ReplacingMergeTree` and nothing takes over if that server goes away. Keeper coordination removes that limitation - several ClickHouse replicas create the same table on a shared Keeper path, exactly one of them (the "active worker") consumes the shared replication slot at a time, and the others stand by and take over automatically once the active worker's Keeper session ends. The standby replicas are not idle copies: the nested table is a replicated table engine, so they receive both the initial snapshot and the ongoing changes through ClickHouse replication and can be queried like the active worker.

To enable it, set [`materialized_postgresql_keeper_path`](#materialized-postgresql-keeper-path) together with a replicated nested table engine and create the same table on every participating replica:

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_table', 'postgres_user', 'postgres_password')
PRIMARY KEY key
SETTINGS materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree',
         materialized_postgresql_keeper_path = '/clickhouse/materialized_postgresql/{shard}/postgresql_replica';
```

### `materialized_postgresql_table_engine` {#materialized-postgresql-table-engine}

Engine used for the nested table that stores the replicated data. One of `ReplacingMergeTree` (default), `ReplicatedReplacingMergeTree`, `SharedReplacingMergeTree`. The replicated and shared variants require `materialized_postgresql_keeper_path` to be set, and coordination in turn requires one of them: with a plain `ReplacingMergeTree` the standby replicas would hold no data, so a takeover would lose every row replicated before the failover. `SharedReplacingMergeTree` is only available in ClickHouse Cloud. It must be specified at `CREATE` time and cannot be changed afterwards.

### `materialized_postgresql_keeper_path` {#materialized-postgresql-keeper-path}

Keeper (or ZooKeeper) path used to coordinate the PostgreSQL replication slot across ClickHouse replicas. Default: empty (coordination disabled). Keeper must be configured on the server; a coordinated `CREATE TABLE` without it is rejected at `CREATE` time rather than left retrying in the background.

The path supports the `{uuid}` and `{shard}` macros and **must resolve to the same value on every participating replica**, so a per-replica or per-server macro such as `{replica}` or `{server_uuid}` is rejected at `CREATE` time - put the per-replica part in [`materialized_postgresql_replica_name`](#materialized-postgresql-replica-name) instead. Coordination owns the shared slot and publication, so it cannot be combined with `materialized_postgresql_use_unique_replication_consumer_identifier` or with a user-managed `materialized_postgresql_replication_slot` / `materialized_postgresql_snapshot`.

All engines sharing a keeper path must agree on the settings that determine the derived names of the nested table, the shared replication slot and the publication, and must replicate the same PostgreSQL source database and table; the first one publishes that identity under the keeper path and a disagreeing engine is rejected. In particular a `MaterializedPostgreSQL` **table** and a `MaterializedPostgreSQL` **database** can never share one keeper path, because they derive different slot and publication names even for the same source table.

`DROP TABLE` on a coordinated table only removes the shared replication slot and publication from PostgreSQL together with the last remaining replica, and that last-replica decision is made in Keeper *before* the local data is deleted: a `DROP TABLE` while Keeper is unreachable fails instead of deleting the last copy of the data (retry it once Keeper is reachable), and a drop that is refused after replication has already been stopped rebuilds replication in the background so the replica rejoins the setup. (`TRUNCATE TABLE` is not supported by this table engine in any mode.)

See the [`MaterializedPostgreSQL` database engine](../../../engines/database-engines/materialized-postgresql.md) for the full description of the coordinated mode, including the leftover-state and schema-drift rules that apply here as well.

### `materialized_postgresql_replica_name` {#materialized-postgresql-replica-name}

Replica identity used for the coordination node and for the nested replicated table engine. Default: `{replica}`. Supports the `{uuid}`, `{shard}` and `{replica}` macros. It **must resolve to a distinct value on every replica** (a name already registered by another replica is rejected) and the expanded value must be a single Keeper node name - an empty value, or one containing `/`, is rejected.

Together with `materialized_postgresql_keeper_path` it forms the **coordination identity** of the replica, which must stay the same for the lifetime of the coordinated setup: both settings are re-expanded from the current server configuration on every startup, while the nested table keeps the expansion it was created with. A configuration-only change of a macro they expand through is therefore refused when the replica starts up, with an error naming both identities; restore the configuration, or drop the table on that replica and recreate it on the new coordination path. The table stays droppable in that state, and the drop tears down the coordination state the nested table was actually created with.

## Virtual columns {#virtual-columns}

- `_version` — Transaction counter. Type: [UInt64](../../../sql-reference/data-types/int-uint.md).

- `_sign` — Deletion mark. Type: [Int8](../../../sql-reference/data-types/int-uint.md). Possible values:
  - `1` — Row is not deleted,
  - `-1` — Row is deleted.

These columns do not need to be added when a table is created. They are always accessible in `SELECT` query.
`_version` column equals `LSN` position in `WAL`, so it might be used to check how up-to-date replication is.

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;

SELECT key, value, _version FROM postgresql_db.postgresql_replica;
```

:::note
Replication of [**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html) values is not supported. The default value for the data type will be used.
:::
