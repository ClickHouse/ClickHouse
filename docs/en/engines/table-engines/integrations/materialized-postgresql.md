---
toc_priority: 12
toc_title: MaterializedPostgreSQL
---

# MaterializedPostgreSQL {#materialize-postgresql}

## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;
```

## Requirements {#requirements}

1. The [wal_level](https://www.postgresql.org/docs/current/runtime-config-wal.html) setting must have a value `logical` and `max_replication_slots` parameter must have a value at least `2` in the PostgreSQL config file.

2. A table with `MaterializedPostgreSQL` engine must have a primary key — the same as a replica identity index (by default: primary key) of a PostgreSQL table (see [details on replica identity index](../../database-engines/materialized-postgresql.md#requirements)).

3. Only database [Atomic](https://en.wikipedia.org/wiki/Atomicity_(database_systems)) is allowed.

## Virtual columns {#virtual-columns}

-   `_version` (type: UInt64)

-   `_sign` (type: Int8)

These columns do not need to be added when a table is created. They are always accessible in `SELECT` query.
`_version` column equals `LSN` position in `WAL`, so it might be used to check how up-to-date replication is.

``` sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;

SELECT key, value, _version FROM postgresql_db.postgresql_replica;
```

!!! warning "Warning"
    Replication of [**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html) values is not supported. The default value for the data type will be used.
