---
sidebar_position: 44
sidebar_label: odbc
---

# odbc {#table-functions-odbc}

返回通过 [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity) 连接的表。

``` sql
odbc(connection_settings, external_database, external_table)
```

参数:

-   `connection_settings` — 在 `odbc.ini` 文件中连接设置的部分的名称。
-   `external_database` — 外部DBMS的数据库名。
-   `external_table` —  `external_database` 数据库中的表名。

为了安全地实现ODBC连接，ClickHouse使用单独的程序 `clickhouse-odbc-bridge`。 如果ODBC驱动程序直接从 `clickhouse-server` 加载，则驱动程序问题可能会导致ClickHouse服务器崩溃。 当需要时，ClickHouse自动启动 `clickhouse-odbc-bridge`。 ODBC桥程序是从与 `clickhouse-server` 相同的软件包安装的。

外部表中字段包含的 `NULL` 值将转换为基本据类型的默认值。 例如，如果远程MySQL表字段包含 `INT NULL` 类型，则将被转换为0（ClickHouse`Int32` 数据类型的默认值）。

## 用法示例 {#usage-example}

**通过ODBC从本地安装的MySQL获取数据**

这个例子检查Ubuntu Linux18.04和MySQL服务器5.7。

确保已经安装了unixODBC和MySQL连接器。

默认情况下（如果从软件包安装），ClickHouse以用户 `clickhouse` 启动。 因此，您需要在MySQL服务器中创建和配置此用户。

``` bash
$ sudo mysql
```

``` sql
mysql> CREATE USER 'clickhouse'@'localhost' IDENTIFIED BY 'clickhouse';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'clickhouse'@'clickhouse' WITH GRANT OPTION;
```

然后在 `/etc/odbc.ini` 中配置连接。

``` bash
$ cat /etc/odbc.ini
[mysqlconn]
DRIVER = /usr/local/lib/libmyodbc5w.so
SERVER = 127.0.0.1
PORT = 3306
DATABASE = test
USERNAME = clickhouse
PASSWORD = clickhouse
```

您可以使用unixODBC安装的 `isql` 实用程序检查连接。

``` bash
$ isql -v mysqlconn
+-------------------------+
| Connected!                            |
|                                       |
...
```

MySQL中的表:

``` text
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
+------+----------+-----+----------+
| int_id | int_nullable | float | float_nullable |
+------+----------+-----+----------+
|      1 |         NULL |     2 |           NULL |
+------+----------+-----+----------+
1 row in set (0,00 sec)
```

从ClickHouse中的MySQL表中检索数据:

``` sql
SELECT * FROM odbc('DSN=mysqlconn', 'test', 'test')
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │            0 │     2 │              0 │
└────────┴──────────────┴───────┴────────────────┘
```

## 另请参阅 {#see-also}

-   [ODBC外部字典](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-odbc)
-   [ODBC表引擎](../../engines/table-engines/integrations/odbc.md).

[原始文章](https://clickhouse.com/docs/en/query_language/table_functions/jdbc/) <!--hide-->
