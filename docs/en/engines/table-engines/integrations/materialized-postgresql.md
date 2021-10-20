---
toc_priority: 12
toc_title: MateriaziePostgreSQL
---

# MaterializedPostgreSQL {#materialize-postgresql}

## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE test.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;
```


## Requirements {#requirements}

- Setting `wal_level`to `logical` and `max_replication_slots` to at least `2` in the postgresql config file.

- A table with engine `MaterializedPostgreSQL` must have a primary key - the same as a replica identity index (default: primary key) of a postgres table (See [details on replica identity index](../../database-engines/materialized-postgresql.md#requirements)).

- Only database `Atomic` is allowed.


## Virtual columns {#creating-a-table}

- `_version` (`UInt64`)

- `_sign` (`Int8`)

These columns do not need to be added, when table is created. They are always accessible in `SELECT` query.
`_version` column equals `LSN` position in `WAL`, so it might be used to check how up-to-date replication is.

``` sql
CREATE TABLE test.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;

SELECT key, value, _version FROM test.postgresql_replica;
```


## Warning {#warning}

1. **TOAST** values convertion is not supported. Default value for the data type will be used.
