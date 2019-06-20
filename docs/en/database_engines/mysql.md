#MySQL

Manages data on the remote MySQL server.

The engine connects to the remote MySQL database, creates the table structure and proxy queries to tables.


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

All other data types are converted into [String](../data_types/string.md).

[Nullable](../data_types/nullable.md) data type is supported.

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

## Usage

MySQL database engine doesn't support:

- Attaching/Detaching tables.
- Removing tables.
- Renaming tables.
- Creating tables.
- Altering tables.


## Example


Table in MySQL:

```
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `int_nullable` INT NULL DEFAULT NULL,
    ->   `float` FLOAT NOT NULL,
    ->   `float_nullable` FLOAT NULL DEFAULT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into test (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from test;
+--------+--------------+-------+----------------+
| int_id | int_nullable | float | float_nullable |
+--------+--------------+-------+----------------+
|      1 |         NULL |     2 |           NULL |
+--------+--------------+-------+----------------+
1 row in set (0,00 sec)
```

Database in ClickHouse, exchanging data with the MySQL server:

```sql
CREATE DATABASE mysql_db ENGINE = MySQL('localhost:3306', 'test', 'bayonet', '123')
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
SHOW CREATE DATABASE mysql_db
```
```text
┌─statement───────────────────────────────────────────────────────────────────────────┐
│ CREATE DATABASE mysql_db ENGINE = MySQL('localhost:3306', 'test', 'bayonet', '123') │
└─────────────────────────────────────────────────────────────────────────────────────┘
```
```sql
SHOW TABLES FROM mysql_db
```
```text
┌─name─┐
│ test │
└──────┘
```
```sql
SELECT * FROM mysql_db.test
```
```text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴────────────────┘
```
```sql
INSERT INTO mysql_db.test VALUES (1,2,3,4)
```
