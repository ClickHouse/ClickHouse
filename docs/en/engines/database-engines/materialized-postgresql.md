---
toc_priority: 30
toc_title: MaterializedPostgreSQL
---

# MaterializedPostgreSQL {#materialize-postgresql}

## Creating a Database {#creating-a-database}

``` sql
CREATE DATABASE test_database
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password');

SELECT * FROM test_database.postgres_table;
```

## Settings {#settings}

1. `materialized_postgresql_max_block_size` — Number of rows collected before flushing data into table. Default: `65536`.

2. `materialized_postgresql_tables_list` — List of tables for MaterializedPostgreSQL database engine. Default: `whole database`.

3. `materialized_postgresql_allow_automatic_update` — Allow to reload table in the background, when schema changes are detected. Default: `0` (`false`).

``` sql
CREATE DATABASE test_database
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_max_block_size = 65536,
         materialized_postgresql_tables_list = 'table1,table2,table3';

SELECT * FROM test_database.table1;
```

## Requirements {#requirements}

1. Setting [wal_level](https://www.postgresql.org/docs/current/runtime-config-wal.html) to `logical` and `max_replication_slots` to at least `2` in the PostgreSQL config file.

2. Each replicated table must have one of the following **replica identity**:

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
    **TOAST** values conversion is not supported. Default value for the data type will be used.
