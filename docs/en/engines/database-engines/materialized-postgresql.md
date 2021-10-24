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

## Dynamically adding new tables to replication

``` sql
ATTACH TABLE postgres_database.new_table;
```

It will work as well if there is a setting `materialized_postgresql_tables_list`.

## Dynamically removing tables from replication

``` sql
DETACH TABLE postgres_database.table_to_remove;
```

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

It is also possible to change settings at run time.

``` sql
ALTER DATABASE postgres_database MODIFY SETTING materialized_postgresql_max_block_size = <new_size>;
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

### Failover of the logical replication slot {#logical-replication-slot-failover}

Logical Replication Slots which exist on the primary are not available on standby replicas.
So if there is a failover, new primary (the old physical standby) won’t be aware of any slots which were existing with old primary. This will lead to a broken replication from PostgreSQL.
A solution to this is to manage replication slots yourself and define a permanent replication slot (some information can be found [here](https://patroni.readthedocs.io/en/latest/SETTINGS.html)). You'll need to pass slot name via [materialized_postgresql_replication_slot](../../operations/settings/settings.md#materialized-postgresql-replication-slot) setting, and it has to be exported with `EXPORT SNAPSHOT` option. The snapshot identifier needs to be passed via [materialized_postgresql_snapshot](../../operations/settings/settings.md#materialized-postgresql-snapshot) setting.

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
