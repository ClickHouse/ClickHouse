---
sidebar_label: Connect MySQL and ClickHouse
sidebar_position: 10
description: The MySQL table engine allows you to connect ClickHouse to MySQL.
keywords: [clickhouse, mysql, connect, integrate, table, engine]
---

# Connecting ClickHouse to MySQL using the MySQL Table Engine

The `MySQL` table engine allows you to connect ClickHouse to MySQL. **SELECT** and **INSERT** statements can be made in either ClickHouse or in the MySQL table. This article illustrates the basic methods of how to use the `MySQL` table engine.

## 1. Configure MySQL

1.  Create a database in MySQL:
  ```sql
  CREATE DATABASE db1;
  ```

2. Create a table:
  ```sql
  CREATE TABLE db1.table1 (
    id INT,
    column1 VARCHAR(255)
  );
  ```

3. Insert sample rows:
  ```sql
  INSERT INTO db1.table1 
    (id, column1) 
  VALUES 
    (1, 'abc'),
    (2, 'def'),
    (3, 'ghi');
  ```

4. Create a user to connect from ClickHouse:
  ```sql
  CREATE USER 'mysql_clickhouse'@'%' IDENTIFIED BY 'Password123!';
  ```

5. Grant privileges as needed. (For demonstration purposes, the `mysql_clickhouse` user is granted admin prvileges.)
  ```sql
  GRANT ALL PRIVILEGES ON *.* TO 'mysql_clickhouse'@'%';
  ```

## 2. Define a Table in ClickHouse

1. Now let's create a ClickHouse table that uses the `MySQL` table engine:
  ```sql
  CREATE TABLE mysql_table1 (
    id UInt64,
    column1 String
  )
  ENGINE = MySQL('mysql-host.domain.com','db1','table1','mysql_clickhouse','Password123!')
  ```

  The minimum parameters are:

  |parameter|Description        |example              |
  |---------|----------------------------|---------------------|
  |host     |hostname or IP              |mysql-host.domain.com|
  |database |mysql database name         |db1                  |
  |table    |mysql table name            |table1               |
  |user     |username to connect to mysql|mysql_clickhouse     |
  |password |password to connect to mysql|Password123!         |

  :::note
  View the [MySQL table engine](../../en/engines/table-engines/integrations/mysql.md) doc page for a complete list of parameters. 
  :::

## 3. Test the Integration

1. In MySQL, insert a sample row:
  ```sql
  INSERT INTO db1.table1 
    (id, column1) 
  VALUES 
    (4, 'jkl');
  ```

2. Notice the existing rows from the MySQL table are in the ClickHouse table, along with the new row you just added:
  ```sql
  SELECT
      id,
      column1
  FROM mysql_table1
  ```

  You should see 4 rows:
  ```response
  Query id: 6d590083-841e-4e95-8715-ef37d3e95197

  ┌─id─┬─column1─┐
  │  1 │ abc     │
  │  2 │ def     │
  │  3 │ ghi     │
  │  4 │ jkl     │
  └────┴─────────┘

  4 rows in set. Elapsed: 0.044 sec.
  ```

3. Let's add a row to the ClickHouse table:
  ```sql
  INSERT INTO mysql_table1
    (id, column1)
  VALUES
    (5,'mno')
  ```

4.  Notice the new row appears in MySQL:
  ```bash
  mysql> select id,column1 from db1.table1;
  ```

  You should see the new row:
  ```response
  +------+---------+
  | id   | column1 |
  +------+---------+
  |    1 | abc     |
  |    2 | def     |
  |    3 | ghi     |
  |    4 | jkl     |
  |    5 | mno     |
  +------+---------+
  5 rows in set (0.01 sec)
  ```

## Summary

The `MySQL` table engine allows you to connect ClickHouse to MySQL to exchange data back and forth. For more details, be sure to check out the documentation page for the [MySQL table engine](../../en/engines/table-engines/integrations/mysql.md).
