# MySQL

Allows to connect to databases on a remote MySQL server and perform `INSERT` and `SELECT` queries with tables to exchange data between ClickHouse and MySQL.

The `MySQL` database engine translate queries to the MySQL server so you can perform operations such as `SHOW TABLES` or `SHOW CREATE TABLE`.

You cannot perform the following queries:

- `ATTACH`/`DETACH`
- `DROP`
- `RENAME`
- `CREATE TABLE`
- `ALTER`


## Creating a Database

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MySQL('host:port', 'database', 'user', 'password')
```

**Engine Parameters**

- `host:port` — MySQL server address.
- `database` — Remote database name.
- `user` — MySQL user.
- `password` — User password.


## Data Types Support

MySQL | ClickHouse
------|------------
UNSIGNED TINYINT | [UInt8](../data_types/int_uint.md)
TINYINT | [Int8](../data_types/int_uint.md)
UNSIGNED SMALLINT | [UInt16](../data_types/int_uint.md)
SMALLINT | [Int16](../data_types/int_uint.md)
UNSIGNED INT, UNSIGNED MEDIUMINT | [UInt32](../data_types/int_uint.md)
INT, MEDIUMINT | [Int32](../data_types/int_uint.md)
UNSIGNED BIGINT | [UInt64](../data_types/int_uint.md)
BIGINT | [Int64](../data_types/int_uint.md)
FLOAT | [Float32](../data_types/float.md)
DOUBLE | [Float64](../data_types/float.md)
DATE | [Date](../data_types/date.md)
DATETIME, TIMESTAMP | [DateTime](../data_types/datetime.md)
BINARY | [FixedString](../data_types/fixedstring.md)

All other MySQL data types are converted into [String](../data_types/string.md).

[Nullable](../data_types/nullable.md) is supported.


## Examples of Use

Table in MySQL:

```text
mysql> USE test;
Database changed

mysql> CREATE TABLE `mysql_table` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `float` FLOAT NOT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into mysql_table (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from mysql_table;
+--------+-------+
| int_id | value |
+--------+-------+
|      1 |     2 |
+--------+-------+
1 row in set (0,00 sec)
```

Database in ClickHouse, exchanging data with the MySQL server:

```sql
CREATE DATABASE mysql_db ENGINE = MySQL('localhost:3306', 'test', 'my_user', 'user_password')
```
```sql
SHOW DATABASES
```
```text
┌─name─────┐
│ default  │
│ mysql_db │
│ system   │
└──────────┘
```
```sql
SHOW TABLES FROM mysql_db
```
```text
┌─name─────────┐
│  mysql_table │
└──────────────┘
```
```sql
SELECT * FROM mysql_db.mysql_table
```
```text
┌─int_id─┬─value─┐
│      1 │     2 │
└────────┴───────┘
```
```sql
INSERT INTO mysql_db.mysql_table VALUES (3,4)
```
```sql
SELECT * FROM mysql_db.mysql_table
```
```text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```

[Original article](https://clickhouse.yandex/docs/en/database_engines/mysql/) <!--hide-->
