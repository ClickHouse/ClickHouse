---
sidebar_label: Replicate a MySQL Database in ClickHouse
sidebar_position: 20
description: The MaterializedMySQL database engine allows you to define a database in ClickHouse that contains all the existing tables in a MySQL database, along with all the data in those tables. 
keywords: [clickhouse, mysql, connect, integrate, replicate, database, MaterializedMySQL]
---

# Replicate a MySQL Database in ClickHouse

The `MaterializedMySQL` database engine allows you to define a database in ClickHouse that contains all the existing tables in a MySQL database, along with all the data in those tables. On the MySQL side, DDL and DML operations can continue to made and ClickHouse detects the changes and acts as a replica to MySQL database.

This article demonstrates how to configure MySQL and ClickHouse to implement this replication. 

## 1. Configure MySQL

1.  Configure the MySQL database to allow for replication and native authentication. ClickHouse only works with native password authentication. Add the following entries to `/etc/my.cnf`:
  ```
  default-authentication-plugin = mysql_native_password
  gtid-mode = ON
  enforce-gtid-consistency = ON
  ```

2. Create a user to connect from ClickHouse:
  ```sql
  CREATE USER clickhouse_user IDENTIFIED BY 'ClickHouse_123';
  ```

3. Grant the needed permissions to the new user. For demonstration purposes, full admin rights have been granted here:
  ```sql
  GRANT ALL PRIVILEGES ON *.* TO 'clickhouse_user'@'%';
  ```

  :::note
  The minimal permissions needed for the MySQL user are **RELOAD**, **REPLICATION SLAVE**, **REPLICATION CLIENT** and **SELECT PRIVILEGE**.
  :::

4.  Create a database in MySQL:
  ```sql
  CREATE DATABASE db1;
  ```

5. Create a table:
  ```sql
  CREATE TABLE db1.table_1 (
      id INT,
      column1 VARCHAR(10),
      PRIMARY KEY (`id`)
  ) ENGINE = InnoDB;
  ```

6. Insert a few sample rows:
  ```sql
  INSERT INTO db1.table_1 
    (id, column1) 
  VALUES 
    (1, 'abc'),
    (2, 'def'),
    (3, 'ghi');
  ```

## 2. Configure ClickHouse

1. Set parameter to allow use of experimental feature:
  ```sql
  set allow_experimental_database_materialized_mysql = 1;
  ```

2. Create a database that uses the `MaterializedMySQL` database engine:
  ```sql
  CREATE DATABASE db1_mysql 
  ENGINE = MaterializedMySQL(
    'mysql-host.domain.com:3306', 
    'db1', 
    'clickhouse_user', 
    'ClickHouse_123'
  );
  ```

  The minimum parameters are:

  |parameter|Description                 |example              |
  |---------|----------------------------|---------------------|
  |host:port|hostname or IP and port     |mysql-host.domain.com|
  |database |mysql database name         |db1                  |
  |user     |username to connect to mysql|clickhouse_user    |
  |password |password to connect to mysql|ClickHouse_123       |

  :::note
  View the [MaterializedMySQL database engine](../../en/engines/database-engines/materialized-mysql.md) doc page for a complete list of parameters. 
  :::

## 3. Test the Integration

1. In MySQL, insert a sample row:
  ```sql
  INSERT INTO db1.table_1 
    (id, column1) 
  VALUES 
    (4, 'jkl');
  ```

2. Notice the new row appears in the ClickHouse table:
  ```sql
  SELECT
      id,
      column1
  FROM db1_mysql.table_1
  ```

  The response looks like:
  ```response
  Query id: d61a5840-63ca-4a3d-8fac-c93235985654

  ┌─id─┬─column1─┐
  │  1 │ abc     │
  └────┴─────────┘
  ┌─id─┬─column1─┐
  │  4 │ jkl     │
  └────┴─────────┘
  ┌─id─┬─column1─┐
  │  2 │ def     │
  └────┴─────────┘
  ┌─id─┬─column1─┐
  │  3 │ ghi     │
  └────┴─────────┘

  4 rows in set. Elapsed: 0.030 sec.
  ```

3. Suppose the table in MySQL is modified. Let's a column to `db1.table_1` in MySQL:
  ```sql
  alter table db1.table_1 add column column2 varchar(10) after column1;
  ```

4. Now let's insert a row to the modified table:
  ```sql
  INSERT INTO db1.table_1 
    (id, column1, column2) 
  VALUES 
    (5, 'mno', 'pqr');
  ```

5. Notice the talbe in ClickHouse now has the new column and the new row:
  ```sql
  SELECT
      id,
      column1,
      column2
  FROM db1_mysql.table_1
  ```

  The previous rows will have `NULL` for `column2`:
  ```response
  Query id: 2c32fd15-3c83-480b-9bfc-cba5d932d674

  Connecting to localhost:9000 as user default.
  Connected to ClickHouse server version 22.2.2 revision 54455.

  ┌─id─┬─column1─┬─column2─┐
  │  3 │ ghi     │ ᴺᵁᴸᴸ    │
  └────┴─────────┴─────────┘
  ┌─id─┬─column1─┬─column2─┐
  │  2 │ def     │ ᴺᵁᴸᴸ    │
  └────┴─────────┴─────────┘
  ┌─id─┬─column1─┬─column2─┐
  │  1 │ abc     │ ᴺᵁᴸᴸ    │
  │  5 │ mno     │ pqr     │
  └────┴─────────┴─────────┘
  ┌─id─┬─column1─┬─column2─┐
  │  4 │ jkl     │ ᴺᵁᴸᴸ    │
  └────┴─────────┴─────────┘

  5 rows in set. Elapsed: 0.017 sec.
  ```


## Summary

That's it! The `MaterializedMySQL` database engine will keep the MySQL database synced on ClickHouse. There are a few details and limitations, so be sure to read the [doc page for MaterializedMySQL](../../en/engines/database-engines/materialized-postgresql.md) for more details.


:::note
If you just want to move data between MySQL and ClickHouse, check out the [MySQL table engine](../mysql/mysql-with-clickhouse.md).
:::