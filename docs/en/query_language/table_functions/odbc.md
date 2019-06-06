# odbc {#table_functions-odbc}

Returns table that is connected via [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

```
odbc(connection_settings, external_database, external_table)
```

Parameters:

- `connection_settings` — Name of the section with connection settings in the `odbc.ini` file.
- `external_database` — Name of a database in an external DBMS.
- `external_table` — Name of a table in the `external_database`.

To implement ODBC connection safely, ClickHouse uses the separate program `clickhouse-odbc-bridge`. If the ODBC driver is loaded directly from the `clickhouse-server` program, the problems in the driver can crash the ClickHouse server. ClickHouse starts the `clickhouse-odbc-bridge` program automatically when it is required. The ODBC bridge program is installed by the same package as the `clickhouse-server`.

The fields with the `NULL` values from the external table are converted into the default values for the base data type. For example, if a remote MySQL table field has the `INT NULL` type it is converted to 0 (the default value for ClickHouse `Int32` data type).

## Usage example

**Getting data from the local MySQL installation via ODBC**

This example is for linux Ubuntu 18.04 and MySQL server 5.7.

Ensure that there are unixODBC and MySQL Connector are installed.

By default (if installed from packages) ClickHouse starts on behalf of the user `clickhouse`. Thus you need to create and configure this user at MySQL server.

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

Getting data from the MySQL table:

```sql
SELECT * FROM odbc('DSN=mysqlconn', 'test', 'test')
```
```text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │            0 │     2 │              0 │
└────────┴──────────────┴───────┴────────────────┘
```

## See Also

- [ODBC external dictionaries](../../query_language/dicts/external_dicts_dict_sources.md#dicts-external_dicts_dict_sources-odbc)
- [ODBC table engine](../../operations/table_engines/odbc.md).

[Original article](https://clickhouse.yandex/docs/en/query_language/table_functions/jdbc/) <!--hide-->
