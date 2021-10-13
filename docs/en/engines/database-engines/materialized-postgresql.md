---
toc_priority: 30
toc_title: MaterializedPostgreSQL
---

# [experimental] MaterializedPostgreSQL {#materialize-postgresql}

Creates ClickHouse database with an initial data dump of PostgreSQL database tables and starts replication process, i.e. executes background job to apply new changes as they happen on PostgreSQL database tables in the remote PostgreSQL database.

ClickHouse server works as PostgreSQL replica. It reads WAL and performs DML queries. DDL is not replicated, but can be handled (described below).

## Creating a Database {#creating-a-database}

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MaterializedPostgreSQL('host:port', ['database' | database], 'user', 'password') [SETTINGS ...]
```

**Engine Parameters**

-   `host:port` — PostgreSQL server endpoint.
-   `database` — PostgreSQL database name.
-   `user` — PostgreSQL user.
-   `password` — User password.

## Settings {#settings}

-   [materialized_postgresql_max_block_size](../../operations/settings/settings.md#materialized-postgresql-max-block-size)

-   [materialized_postgresql_tables_list](../../operations/settings/settings.md#materialized-postgresql-tables-list)

-   [materialized_postgresql_allow_automatic_update](../../operations/settings/settings.md#materialized-postgresql-allow-automatic-update)

-   [materialized_postgresql_replication_slot](../../operations/settings/settings.md#materialized-postgresql-replication-slot)

-   [materialized_postgresql_snapshot](../../operations/settings/settings.md#materialized-postgresql-snapshot)

``` sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_max_block_size = 65536,
         materialized_postgresql_tables_list = 'table1,table2,table3';

SELECT * FROM database1.table1;
```

## Requirements {#requirements}

1. The [wal_level](https://www.postgresql.org/docs/current/runtime-config-wal.html) setting must have a value `logical` and `max_replication_slots` parameter must have a value at least `2` in the PostgreSQL config file.

2. Each replicated table must have one of the following [replica identity](https://www.postgresql.org/docs/10/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY):

-   primary key (by default)

-   index

``` bash
postgres# CREATE TABLE postgres_table (a Integer NOT NULL, b Integer, c Integer NOT NULL, d Integer, e Integer NOT NULL);
postgres# CREATE unique INDEX postgres_table_index on postgres_table(a, c, e);
postgres# ALTER TABLE postgres_table REPLICA IDENTITY USING INDEX postgres_table_index;
```

The primary key is always checked first. If it is absent, then the index, defined as replica identity index, is checked.
If the index is used as a replica identity, there has to be only one such index in a table.
You can check what type is used for a specific table with the following command:

``` bash
postgres# SELECT CASE relreplident
          WHEN 'd' THEN 'default'
          WHEN 'n' THEN 'nothing'
          WHEN 'f' THEN 'full'
          WHEN 'i' THEN 'index'
       END AS replica_identity
FROM pg_class
WHERE oid = 'postgres_table'::regclass;
```

!!! warning "Warning"
    Replication of [**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html) values is not supported. The default value for the data type will be used.

## Example of Use {#example-of-use}

``` sql
CREATE DATABASE postgresql_db
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password');

SELECT * FROM postgresql_db.postgres_table;
```

## Notes {#notes}

- Failover of the logical replication slot.

Logical Replication Slots which exist on the primary are not available on standby replicas.
So if there is a failover, new primary (the old physical standby) won’t be aware of any slots which were existing with old primary. This will lead to a broken replication from PostgreSQL.
A solution to this is to manage replication slots yourself and define a permanent replication slot (some information can be found [here](https://patroni.readthedocs.io/en/latest/SETTINGS.html)). You'll need to pass slot name via `materialized_postgresql_replication_slot` setting, and it has to be exported with `EXPORT SNAPSHOT` option. The snapshot identifier needs to be passed via `materialized_postgresql_snapshot` setting.
