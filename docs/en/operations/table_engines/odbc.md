# ODBC {#table_engine-odbc}

Allows ClickHouse to connect to external databases via [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

To implement ODBC connection, ClickHouse uses the separate program `clickhouse-odbc-bridge`. ClickHouse starts this program automatically when it is required. The ODBC bridge program is installed by the same package as the `clickhouse-server`. ClickHouse uses the ODBC bridge program because it is unsafe to load ODBC driver. In case of problems with ODBC driver it can crash the `clickhouse-server`.

This engine supports the [Nullable](../../data_types/nullable.md) data type.

## Creating a Table

```
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
)
ENGINE = ODBC(connection_settings, external_database, external_table)
```

See the detailed description of the [CREATE TABLE](../../query_language/create.md#create-table-query) query.

The table structure can be not the same as the source table structure:

- Names of columns should be the same as in the source table, but you can use just some of these columns in any order.
- Types of columns may differ from the types in the source table. ClickHouse tries to [cast](../../query_language/functions/type_conversion_functions.md#type_conversion_function-cast) values into the ClickHouse data types.

**Engine Parameters**

- `connection_settings` — Name of the section with connection settings in the `odbc.ini` file.
- `external_database` — Name of a database in an external DBMS.
- `external_table` — Name of a table in the `external_database`.

## Usage Example

**Getting data from the local MySQL installation via ODBC**

This example is for linux Ubuntu 18.04 and MySQL server 5.7.

Ensure that there are unixODBC and MySQL Connector are installed.

By default (if installed from packages) ClickHouse starts on behalf of the user `clickhouse`. Thus, you need to create and configure this user at MySQL server.

```
sudo mysql
mysql> CREATE USER 'clickhouse'@'localhost' IDENTIFIED BY 'clickhouse';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'clickhouse'@'clickhouse' WITH GRANT OPTION;
```

Then configure the connection in `/etc/odbc.ini`.

```
$ cat /etc/odbc.ini
[mysqlconn]
DRIVER = /usr/local/lib/libmyodbc5w.so
SERVER = 127.0.0.1
PORT = 3306
DATABASE = test
USERNAME = clickhouse
PASSWORD = clickhouse
```

You can check the connection by the `isql` utility from the unixODBC installation.

```
isql -v mysqlconn
+---------------------------------------+
| Connected!                            |
|                                       |
...
```

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

Table in ClickHouse, getting data from the MySQL table:

```sql
CREATE TABLE odbc_t
(
    `int_id` Int32,
    `float_nullable` Nullable(Float32)
)
ENGINE = ODBC('DSN=mysqlconn', 'test', 'test')
```
```sql
SELECT * FROM odbc_t
```
```text
┌─int_id─┬─float_nullable─┐
│      1 │           ᴺᵁᴸᴸ │
└────────┴────────────────┘
```

## See Also

- [ODBC external dictionaries](../../query_language/dicts/external_dicts_dict_sources.md#dicts-external_dicts_dict_sources-odbc)
- [ODBC table function](../../query_language/table_functions/odbc.md).

[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/jdbc/) <!--hide-->
