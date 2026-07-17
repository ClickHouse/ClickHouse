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

## TLS/SSL {#tls-ssl}

To connect to a PostgreSQL server that requires TLS (and optionally to verify its certificate), specify the `materialized_postgresql_ssl_mode`, `materialized_postgresql_ssl_root_cert`, `materialized_postgresql_ssl_cert` and `materialized_postgresql_ssl_key` settings in the `SETTINGS` clause. They are forwarded to `libpq` as `sslmode`, `sslrootcert`, `sslcert` and `sslkey`, and are described in the [MaterializedPostgreSQL database engine settings](/engines/database-engines/materialized-postgresql#settings).

The certificate and key files must be located inside the directory configured by the server's [user_files_path](/operations/server-configuration-parameters/settings.md#user_files_path); relative paths are resolved against it. The TLS/SSL settings are part of the PostgreSQL connection parameters, which are fixed when the table is created.

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_table', 'postgres_user', 'postgres_password')
PRIMARY KEY key
SETTINGS
    materialized_postgresql_ssl_mode = 'verify-full',
    materialized_postgresql_ssl_root_cert = '/var/lib/clickhouse/user_files/postgresql-ca.crt';
```

The same parameters can also be supplied through a [named collection](/operations/named-collections), using the `libpq` key names `sslmode`, `sslrootcert`, `sslcert` and `sslkey`.

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

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;

SELECT key, value, _version FROM postgresql_db.postgresql_replica;
```

:::note
Replication of [**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html) values is not supported. The default value for the data type will be used.
:::
