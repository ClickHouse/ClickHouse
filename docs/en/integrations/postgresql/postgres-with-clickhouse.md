---
sidebar_label: Connect PostgreSQL and ClickHouse
sidebar_position: 10
keywords: [clickhouse, postgres, postgresql, connect, integrate, table, engine]
---

# Connecting ClickHouse to PostgreSQL using the PostgreSQL Table Engine

The `PostgreSQL` table engine allows **SELECT** and **INSERT** operations on data stored on the remote PostgreSQL server from ClickHouse.
This article is to illustrate basic methods of integration using one table.

## 1. Setting up PostgreSQL
1.  In `postgresql.conf`, add the following entry to enable PostgreSQL to listen on the network interfaces:
  ```
  listen_addresses = '*' 
  ```

2. Create a user to connect from ClickHouse. For demonstration purposes, this example grants full superuser rights.
  ```sql
  CREATE ROLE clickhouse_user SUPERUSER LOGIN PASSWORD 'ClickHouse_123';
  ```

3. Create a new database in PostgreSQL:
  ```sql
  CREATE DATABASE db_in_psg;
  ```

4. Create a new table:
  ```sql
  CREATE TABLE table1 (
      id         integer primary key,
      column1    varchar(10)
  );
  ```

6. Let's add a few rows for testing:
  ```sql
  INSERT INTO table1
    (id, column1)
  VALUES
    (1, 'abc'),
    (2, 'def');
  ```

7. To configure PostgreSQL to allow connections to the new database with the new user for replication, add the following entry to the `pg_hba.conf` file. Update the address line with either the subnet or IP address of your PostgreSQL server:
  ```
  # TYPE  DATABASE        USER            ADDRESS                 METHOD
  host    db_in_psg             clickhouse_user 192.168.1.0/24          password
  ```

8. Reload the `pg_hba.conf` configuration (adjust this command depending on your version):
  ```
  /usr/pgsql-12/bin/pg_ctl reload
  ```

9. Verify the new `clickhouse_user` can login:
  ```
  psql -U clickhouse_user -W -d db_in_psg -h <your_postgresql_host>
  ```

## 2. Define a Table in ClickHouse
1. Login to the `clickhouse-client`:
  ```
  clickhouse-client --user default --password ClickHouse123!
  ```

2. Let's create a new database:
  ```sql
  CREATE DATABASE db_in_ch;
  ```

3. Create a table that uses the `PostgreSQL`:
  ```sql
  CREATE TABLE db_in_ch.table1
  (
      id UInt64,
      column1 String
  ) 
  ENGINE = PostgreSQL('postgres-host.domain.com:5432', 'db_in_psg', 'table1', 'clickhouse_user', 'ClickHouse_123');
  ```

  The minimum parameters needed are:

  |parameter|Description                 |example              |
  |---------|----------------------------|---------------------|
  |host:port|hostname or IP and port     |postgres-host.domain.com:5432|
  |database |PostgreSQL database name         |db_in_psg                  |
  |user     |username to connect to postgres|clickhouse_user     |
  |password |password to connect to postgres|ClickHouse_123       |

  :::note
  View the [PostgreSQL table engine](../../en/engines/table-engines/integrations/postgresql.md) doc page for a complete list of parameters. 
  :::


## 3 Test the Integration

1. In ClickHouse, view initial rows:
  ```sql
  SELECT * FROM db_in_ch.table1
  ```

  The ClickHouse table should automatically be populated with the two rows that already existed in the table in PostgreSQL:
  ```response
  Query id: 34193d31-fe21-44ac-a182-36aaefbd78bf

  ┌─id─┬─column1─┐
  │  1 │ abc     │
  │  2 │ def     │
  └────┴─────────┘
  ```

2. Back in PostgreSQL, add a couple of rows to the table:
  ```sql
  INSERT INTO table1 
    (id, column1) 
  VALUES 
    (3, 'ghi'),
    (4, 'jkl');
  ```

4. Those two new rows should appear in your ClickHouse table:
  ```sql
  SELECT * FROM db_in_ch.table1
  ```

  The response should be:
  ```response
  Query id: 86fa2c62-d320-4e47-b564-47ebf3d5d27b

  ┌─id─┬─column1─┐
  │  1 │ abc     │
  │  2 │ def     │
  │  3 │ ghi     │
  │  4 │ jkl     │
  └────┴─────────┘
  ```

5. Let's see what happens when you add rows to the ClickHouse table:
  ```sql
  INSERT INTO db_in_ch.table1
    (id, column1)
  VALUES
    (5, 'mno'),
    (6, 'pqr');
  ```

6. The rows added in ClickHouse should appear in the table in PostgreSQL:
  ```sql
  db_in_psg=# SELECT * FROM table1;
  id | column1
  ----+---------
    1 | abc
    2 | def
    3 | ghi
    4 | jkl
    5 | mno
    6 | pqr
  (6 rows)
  ```

## Summary
This example demonstrated the basic integration between PostgreSQL and ClickHouse using the `PostrgeSQL` table engine.
Check out the [doc page for the PostgreSQL table engine](../../en/engines/table-engines/integrations/postgresql.md) for more features, such as specifying schemas, returning only a subset of columns, and connecting to multiple replicas.
