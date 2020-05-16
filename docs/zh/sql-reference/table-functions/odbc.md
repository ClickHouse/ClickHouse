---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: odbc
---

# odbc {#table-functions-odbc}

返回通过连接的表 [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

``` sql
odbc(connection_settings, external_database, external_table)
```

参数:

-   `connection_settings` — Name of the section with connection settings in the `odbc.ini` 文件
-   `external_database` — Name of a database in an external DBMS.
-   `external_table` — Name of a table in the `external_database`.

为了安全地实现ODBC连接，ClickHouse使用单独的程序 `clickhouse-odbc-bridge`. 如果直接从ODBC驱动程序加载 `clickhouse-server`，驱动程序问题可能会导致ClickHouse服务器崩溃。 ClickHouse自动启动 `clickhouse-odbc-bridge` 当它是必需的。 ODBC桥程序是从相同的软件包作为安装 `clickhouse-server`.

与字段 `NULL` 外部表中的值将转换为基数据类型的默认值。 例如，如果远程MySQL表字段具有 `INT NULL` 键入它将转换为0（ClickHouse的默认值 `Int32` 数据类型）。

## 用法示例 {#usage-example}

**通过ODBC从本地MySQL安装获取数据**

此示例检查Ubuntu Linux18.04和MySQL服务器5.7。

确保安装了unixODBC和MySQL连接器。

默认情况下（如果从软件包安装），ClickHouse以用户身份启动 `clickhouse`. 因此，您需要在MySQL服务器中创建和配置此用户。

``` bash
$ sudo mysql
```

``` sql
mysql> CREATE USER 'clickhouse'@'localhost' IDENTIFIED BY 'clickhouse';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'clickhouse'@'clickhouse' WITH GRANT OPTION;
```

然后配置连接 `/etc/odbc.ini`.

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

您可以使用 `isql` unixodbc安装中的实用程序。

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

[原始文章](https://clickhouse.tech/docs/en/query_language/table_functions/jdbc/) <!--hide-->
