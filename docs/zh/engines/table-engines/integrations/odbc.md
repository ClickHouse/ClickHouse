---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: ODBC
---

# ODBC {#table-engine-odbc}

允许ClickHouse通过以下方式连接到外部数据库 [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

为了安全地实现ODBC连接，ClickHouse使用单独的程序 `clickhouse-odbc-bridge`. 如果直接从ODBC驱动程序加载 `clickhouse-server`，驱动程序问题可能会导致ClickHouse服务器崩溃。 ClickHouse自动启动 `clickhouse-odbc-bridge` 当它是必需的。 ODBC桥程序是从相同的软件包作为安装 `clickhouse-server`.

该引擎支持 [可为空](../../../sql-reference/data-types/nullable.md) 数据类型。

## 创建表 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1],
    name2 [type2],
    ...
)
ENGINE = ODBC(connection_settings, external_database, external_table)
```

请参阅的详细说明 [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) 查询。

表结构可以与源表结构不同:

-   列名应与源表中的列名相同，但您可以按任何顺序使用其中的一些列。
-   列类型可能与源表中的列类型不同。 ClickHouse尝试 [投](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) ClickHouse数据类型的值。

**发动机参数**

-   `connection_settings` — Name of the section with connection settings in the `odbc.ini` 文件
-   `external_database` — Name of a database in an external DBMS.
-   `external_table` — Name of a table in the `external_database`.

## 用法示例 {#usage-example}

**通过ODBC从本地MySQL安装中检索数据**

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

ClickHouse中的表，从MySQL表中检索数据:

``` sql
CREATE TABLE odbc_t
(
    `int_id` Int32,
    `float_nullable` Nullable(Float32)
)
ENGINE = ODBC('DSN=mysqlconn', 'test', 'test')
```

``` sql
SELECT * FROM odbc_t
```

``` text
┌─int_id─┬─float_nullable─┐
│      1 │           ᴺᵁᴸᴸ │
└────────┴────────────────┘
```

## 另请参阅 {#see-also}

-   [ODBC外部字典](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-odbc)
-   [ODBC表函数](../../../sql-reference/table-functions/odbc.md)

[原始文章](https://clickhouse.tech/docs/en/operations/table_engines/odbc/) <!--hide-->
