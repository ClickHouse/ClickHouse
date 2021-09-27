---
toc_priority: 30
toc_title: MaterializedPostgreSQL
---

# MaterializedPostgreSQL {#materialize-postgresql}

## Creating a Database {#creating-a-database}

``` sql
CREATE DATABASE test_database
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password'

SELECT * FROM test_database.postgres_table;
```


## Settings {#settings}

1. `materialized_postgresql_max_block_size` - Number of rows collected before flushing data into table. Default: `65536`.

2. `materialized_postgresql_tables_list` - List of tables for MaterializedPostgreSQL database engine. Default: `whole database`.

3. `materialized_postgresql_allow_automatic_update` - Allow to reload table in the background, when schema changes are detected. Default: `0` (`false`).

-   [materialized_postgresql_replication_slot](../../operations/settings/settings.md#materialized-postgresql-replication-slot)

-   [materialized_postgresql_snapshot](../../operations/settings/settings.md#materialized-postgresql-snapshot)

``` sql
CREATE DATABASE test_database
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password'
SETTINGS materialized_postgresql_max_block_size = 65536,
         materialized_postgresql_tables_list = 'table1,table2,table3';

SELECT * FROM test_database.table1;
```


## Requirements {#requirements}

- Setting `wal_level`to `logical` and `max_replication_slots` to at least `2` in the postgresql config file.

- Each replicated table must have one of the following **replica identity**:

1. **default** (primary key)

2. **index**

``` bash
postgres# CREATE TABLE postgres_table (a Integer NOT NULL, b Integer, c Integer NOT NULL, d Integer, e Integer NOT NULL);
postgres# CREATE unique INDEX postgres_table_index on postgres_table(a, c, e);
postgres# ALTER TABLE postgres_table REPLICA IDENTITY USING INDEX postgres_table_index;
```


Primary key is always checked first. If it is absent, then index, defined as replica identity index, is checked.
If index is used as replica identity, there has to be only one such index in a table.
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


## Warning {#warning}

1. **TOAST** values convertion is not supported. Default value for the data type will be used.
