---
toc_priority: 12
toc_title: MateriaziePostgreSQL
---

# MaterializePostgreSQL {#materialize-postgresql}

## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE test.postgresql_replica (key UInt64, value UInt64, _sign Int8 MATERIALIZED 1, _version UInt64 MATERIALIZED 1)
ENGINE = MaterializePostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;
```


## Requirements {#requirements}

- Setting `wal_level`to `logical` and `max_replication_slots` to at least `2` in the postgresql config file.

- A table with engine `MaterializePostgreSQL` must have a primary key - the same as a replica identity index (default: primary key) of a postgres table (See [details on replica identity index](../../database-engines/materialize-postgresql.md#requirements)).

- Only database `Atomic` is allowed.


## Virtual columns {#creating-a-table}

- `_version`

- `_sign`

``` sql
CREATE TABLE test.postgresql_replica (key UInt64, value UInt64, _sign Int8 MATERIALIZED 1, _version UInt64 MATERIALIZED 1)
ENGINE = MaterializePostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;
```
