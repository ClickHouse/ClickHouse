---
sidebar_label: PostgreSQL Database Engine
sidebar_position: 20
keywords: [clickhouse, postgres, postgresql, connect, integrate]
---

# Connecting ClickHouse to PostgreSQL using the MaterializedPostgreSQL database engine

The PostgreSQL database engine uses the PostgreSQL replication features to create a replica of the database with all or a subset of schemas and tables. 
This article is to illustrate basic methods of integration using one database, one schema and one table.

***In the following procedures, the PostgreSQL CLI (psql) and the ClickHouse CLI (clickhouse-client) are used. The PostgreSQL server is installed on linux. The following has minimum settings if the postgresql database is new test install***

## 1. In PostgreSQL
1.  In `postgresql.conf`, set minimum listen levels, replication wal level and replication slots:

add the following entries:
```
listen_addresses = '*' 
max_replication_slots = 10
wal_level = logical
```
_*ClickHouse needs minimum of `logical` wal level and minimum `2` replication slots_

2. Using an admin account, create a user to connect from ClickHouse:
```sql
CREATE ROLE clickhouse_user SUPERUSER LOGIN PASSWORD 'ClickHouse_123';
```
_*for demonstration purposes, full superuser rights have been granted._


3. create a new database:
```sql
CREATE DATABASE db1;
```

4. connect to the new database in `psql`:
```
\connect db1
```

5. create a new table:
```sql
CREATE TABLE table1 (
    id         integer primary key,
    column1    varchar(10)
);
```

6. add initial rows:
```sql
INSERT INTO table1
(id, column1)
VALUES
(1, 'abc'),
(2, 'def');
```

7. Configure the PostgreSQLto allow connections to the new database with the new user for replication:
below is the minimum entry to add to the `pg_hba.conf` file:
```
# TYPE  DATABASE        USER            ADDRESS                 METHOD
host    db1             clickhouse_user 192.168.1.0/24          password
```
_*for demonstration purposes, this is using clear text password authentication method. update the address line with either the subnet or the address of the server per PostgreSQL documentation_

8. reload the `pg_hba.conf` configuration with something like this (adjust for your version):
```
/usr/pgsql-12/bin/pg_ctl reload
```

9. Test the login with new `clickhouse_user`:
```
 psql -U clickhouse_user -W -d db1 -h <your_postgresql_host>
```

## 2. In ClickHouse
1. log into the ClickHouse CLI
```
clickhouse-client --user default --password ClickHouse123!
```

2. Enable the PosgreSQL experimental feature for the database engine:
```sql
SET allow_experimental_database_materialized_postgresql=1
```

3. Create the new database to be replicated and define the intitial table:
```sql
CREATE DATABASE db1_postgres
ENGINE = MaterializedPostgreSQL('postgres-host.domain.com:5432', 'db1', 'clickhouse_user', 'ClickHouse_123')
SETTINGS materialized_postgresql_tables_list = 'table1';
```
minimum options:

|parameter|Description                 |example              |
|---------|----------------------------|---------------------|
|host:port|hostname or IP and port     |postgres-host.domain.com:5432|
|database |PostgreSQL database name         |db1                  |
|user     |username to connect to postgres|clickhouse_user     |
|password |password to connect to postgres|ClickHouse_123       |
|settings |additional settings for the engine| materialized_postgresql_tables_list = 'table1'|

**For complete guide to the PostgreSQL database engine, refer to: **
https://clickhouse.com/docs/en/engines/database-engines/materialized-postgresql/#settings

4. Verify initial table has data
```sql
ch_env_2 :) select * from db1_postgres.table1;

SELECT *
FROM db1_postgres.table1

Query id: df2381ac-4e30-4535-b22e-8be3894aaafc

┌─id─┬─column1─┐
│  1 │ abc     │
└────┴─────────┘
┌─id─┬─column1─┐
│  2 │ def     │
└────┴─────────┘
```

## 3. Test basic replication
1. In PostgreSQL, add new rows:
```sql
INSERT INTO table1
(id, column1)
VALUES
(3, 'ghi'),
(4, 'jkl');
```

2. In ClickHouse, verify new rows are visible:
```sql
ch_env_2 :) select * from db1_postgres.table1;

SELECT *
FROM db1_postgres.table1

Query id: b0729816-3917-44d3-8d1a-fed912fb59ce

┌─id─┬─column1─┐
│  1 │ abc     │
└────┴─────────┘
┌─id─┬─column1─┐
│  4 │ jkl     │
└────┴─────────┘
┌─id─┬─column1─┐
│  3 │ ghi     │
└────┴─────────┘
┌─id─┬─column1─┐
│  2 │ def     │
└────┴─────────┘
```

## 4. Summary
This integration guide focused on a simple example on how to replicate a database with a table, however, there exist more advanced options which include replicating the whole database or adding new tables and schemas to the existing replications. Although DDL commands are not supported for this replication, the engine can be set to detect changes and reload the tables when there are structural changes made.

**For more features available for advanced options, please see the reference documenation:**
https://clickhouse.com/docs/en/engines/database-engines/materialized-postgresql/
