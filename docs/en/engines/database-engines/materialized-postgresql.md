---
slug: /en/engines/database-engines/materialized-postgresql
sidebar_label: MaterializedPostgreSQL
sidebar_position: 60
---

# [experimental] MaterializedPostgreSQL

Creates a ClickHouse database with tables from PostgreSQL database. Firstly, database with engine `MaterializedPostgreSQL` creates a snapshot of PostgreSQL database and loads required tables. Required tables can include any subset of tables from any subset of schemas from specified database. Along with the snapshot database engine acquires LSN and once initial dump of tables is performed - it starts pulling updates from WAL. After database is created, newly added tables to PostgreSQL database are not automatically added to replication. They have to be added manually with `ATTACH TABLE db.table` query.

Replication is implemented with PostgreSQL Logical Replication Protocol, which does not allow to replicate DDL, but allows to know whether replication breaking changes happened (column type changes, adding/removing columns). Such changes are detected and according tables stop receiving updates. In this case you should use `ATTACH`/ `DETACH PERMANENTLY` queries to reload table completely. If DDL does not break replication (for example, renaming a column) table will still receive updates (insertion is done by position).

:::note
This database engine is experimental. To use it, set `allow_experimental_database_materialized_postgresql` to 1 in your configuration files or by using the `SET` command:
```sql
SET allow_experimental_database_materialized_postgresql=1
```
:::

## Creating a Database {#creating-a-database}

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MaterializedPostgreSQL('host:port', 'database', 'user', 'password') [SETTINGS ...]
```

**Engine Parameters**

- `host:port` — PostgreSQL server endpoint.
- `database` — PostgreSQL database name.
- `user` — PostgreSQL user.
- `password` — User password.

## Example of Use {#example-of-use}

``` sql
CREATE DATABASE postgres_db
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password');

SHOW TABLES FROM postgres_db;

┌─name───┐
│ table1 │
└────────┘

SELECT * FROM postgresql_db.postgres_table;
```

## Dynamically adding new tables to replication {#dynamically-adding-table-to-replication}

After `MaterializedPostgreSQL` database is created, it does not automatically detect new tables in according PostgreSQL database. Such tables can be added manually:

``` sql
ATTACH TABLE postgres_database.new_table;
```

:::warning
Before version 22.1, adding a table to replication left a non-removed temporary replication slot (named `{db_name}_ch_replication_slot_tmp`). If attaching tables in ClickHouse version before 22.1, make sure to delete it manually (`SELECT pg_drop_replication_slot('{db_name}_ch_replication_slot_tmp')`). Otherwise disk usage will grow. This issue is fixed in 22.1.
:::

## Dynamically removing tables from replication {#dynamically-removing-table-from-replication}

It is possible to remove specific tables from replication:

``` sql
DETACH TABLE postgres_database.table_to_remove PERMANENTLY;
```

## PostgreSQL schema {#schema}

PostgreSQL [schema](https://www.postgresql.org/docs/9.1/ddl-schemas.html) can be configured in 3 ways (starting from version 21.12).

1. One schema for one `MaterializedPostgreSQL` database engine. Requires to use setting `materialized_postgresql_schema`.
Tables are accessed via table name only:

``` sql
CREATE DATABASE postgres_database
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_schema = 'postgres_schema';

SELECT * FROM postgres_database.table1;
```

2. Any number of schemas with specified set of tables for one `MaterializedPostgreSQL` database engine. Requires to use setting `materialized_postgresql_tables_list`. Each table is written along with its schema.
Tables are accessed via schema name and table name at the same time:

``` sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_tables_list = 'schema1.table1,schema2.table2,schema1.table3',
         materialized_postgresql_tables_list_with_schema = 1;

SELECT * FROM database1.`schema1.table1`;
SELECT * FROM database1.`schema2.table2`;
```

But in this case all tables in `materialized_postgresql_tables_list` must be written with its schema name.
Requires `materialized_postgresql_tables_list_with_schema = 1`.

Warning: for this case dots in table name are not allowed.

3. Any number of schemas with full set of tables for one `MaterializedPostgreSQL` database engine. Requires to use setting `materialized_postgresql_schema_list`.

``` sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_schema_list = 'schema1,schema2,schema3';

SELECT * FROM database1.`schema1.table1`;
SELECT * FROM database1.`schema1.table2`;
SELECT * FROM database1.`schema2.table2`;
```

Warning: for this case dots in table name are not allowed.


## Requirements {#requirements}

1. The [wal_level](https://www.postgresql.org/docs/current/runtime-config-wal.html) setting must have a value `logical` and `max_replication_slots` parameter must have a value at least `2` in the PostgreSQL config file.

2. Each replicated table must have one of the following [replica identity](https://www.postgresql.org/docs/10/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY):

- primary key (by default)

- index

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

:::note
Replication of [**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html) values is not supported. The default value for the data type will be used.
:::

## Settings {#settings}

### `materialized_postgresql_tables_list` {#materialized-postgresql-tables-list}

    Sets a comma-separated list of PostgreSQL database tables, which will be replicated via [MaterializedPostgreSQL](../../engines/database-engines/materialized-postgresql.md) database engine.

    Each table can have subset of replicated columns in brackets. If subset of columns is omitted, then all columns for table will be replicated.

    ``` sql
    materialized_postgresql_tables_list = 'table1(co1, col2),table2,table3(co3, col5, col7)
    ```

    Default value: empty list — means whole PostgreSQL database will be replicated.

### `materialized_postgresql_schema` {#materialized-postgresql-schema}

    Default value: empty string. (Default schema is used)

### `materialized_postgresql_schema_list` {#materialized-postgresql-schema-list}

    Default value: empty list. (Default schema is used)

### `materialized_postgresql_max_block_size` {#materialized-postgresql-max-block-size}

    Sets the number of rows collected in memory before flushing data into PostgreSQL database table.

    Possible values:

    - Positive integer.

    Default value: `65536`.

### `materialized_postgresql_replication_slot` {#materialized-postgresql-replication-slot}

    A user-created replication slot. Must be used together with `materialized_postgresql_snapshot`.

### `materialized_postgresql_snapshot` {#materialized-postgresql-snapshot}

    A text string identifying a snapshot, from which [initial dump of PostgreSQL tables](../../engines/database-engines/materialized-postgresql.md) will be performed. Must be used together with `materialized_postgresql_replication_slot`.

    ``` sql
    CREATE DATABASE database1
    ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
    SETTINGS materialized_postgresql_tables_list = 'table1,table2,table3';

    SELECT * FROM database1.table1;
    ```

    The settings can be changed, if necessary, using a DDL query. But it is impossible to change the setting `materialized_postgresql_tables_list`. To update the list of tables in this setting use the `ATTACH TABLE` query.

    ``` sql
    ALTER DATABASE postgres_database MODIFY SETTING materialized_postgresql_max_block_size = <new_size>;
    ```

### `materialized_postgresql_use_unique_replication_consumer_identifier` {#materialized_postgresql_use_unique_replication_consumer_identifier}

Use a unique replication consumer identifier for replication. Default: `0`.
If set to `1`, allows to setup several `MaterializedPostgreSQL` tables pointing to the same `PostgreSQL` table.

## Notes {#notes}

### Failover of the logical replication slot {#logical-replication-slot-failover}

Logical Replication Slots which exist on the primary are not available on standby replicas.
So if there is a failover, new primary (the old physical standby) won’t be aware of any slots which were existing with old primary. This will lead to a broken replication from PostgreSQL.
A solution to this is to manage replication slots yourself and define a permanent replication slot (some information can be found [here](https://patroni.readthedocs.io/en/latest/SETTINGS.html)). You'll need to pass slot name via `materialized_postgresql_replication_slot` setting, and it has to be exported with `EXPORT SNAPSHOT` option. The snapshot identifier needs to be passed via `materialized_postgresql_snapshot` setting.

Please note that this should be used only if it is actually needed. If there is no real need for that or full understanding why, then it is better to allow the table engine to create and manage its own replication slot.

**Example (from [@bchrobot](https://github.com/bchrobot))**

1. Configure replication slot in PostgreSQL.

    ```yaml
    apiVersion: "acid.zalan.do/v1"
    kind: postgresql
    metadata:
      name: acid-demo-cluster
    spec:
      numberOfInstances: 2
      postgresql:
        parameters:
          wal_level: logical
      patroni:
        slots:
          clickhouse_sync:
            type: logical
            database: demodb
            plugin: pgoutput
    ```

2. Wait for replication slot to be ready, then begin a transaction and export the transaction snapshot identifier:

    ```sql
    BEGIN;
    SELECT pg_export_snapshot();
    ```

3. In ClickHouse create database:

    ```sql
    CREATE DATABASE demodb
    ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
    SETTINGS
      materialized_postgresql_replication_slot = 'clickhouse_sync',
      materialized_postgresql_snapshot = '0000000A-0000023F-3',
      materialized_postgresql_tables_list = 'table1,table2,table3';
    ```

4. End the PostgreSQL transaction once replication to ClickHouse DB is confirmed. Verify that replication continues after failover:

    ```bash
    kubectl exec acid-demo-cluster-0 -c postgres -- su postgres -c 'patronictl failover --candidate acid-demo-cluster-1 --force'
    ```

### Required permissions

1. [CREATE PUBLICATION](https://postgrespro.ru/docs/postgresql/14/sql-createpublication) -- create query privilege.

2. [CREATE_REPLICATION_SLOT](https://postgrespro.ru/docs/postgrespro/10/protocol-replication#PROTOCOL-REPLICATION-CREATE-SLOT) -- replication privilege.

3. [pg_drop_replication_slot](https://postgrespro.ru/docs/postgrespro/9.5/functions-admin#functions-replication) -- replication privilege or superuser.

4. [DROP PUBLICATION](https://postgrespro.ru/docs/postgresql/10/sql-droppublication) -- owner of publication (`username` in MaterializedPostgreSQL engine itself).

It is possible to avoid executing `2` and `3` commands and having those permissions. Use settings `materialized_postgresql_replication_slot` and `materialized_postgresql_snapshot`. But with much care.

Access to tables:

1. pg_publication

2. pg_replication_slots

3. pg_publication_tables
