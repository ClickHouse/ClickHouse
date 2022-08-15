---
toc_priority: 35
toc_title: ODBC
---

# ODBC {#table-engine-odbc}

允许 ClickHouse 通过 [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity) 方式连接到外部数据库.

为了安全地实现 ODBC 连接，ClickHouse 使用了一个独立程序 `clickhouse-odbc-bridge`. 如果ODBC驱动程序是直接从 `clickhouse-server`中加载的，那么驱动问题可能会导致ClickHouse服务崩溃。 当有需要时，ClickHouse会自动启动 `clickhouse-odbc-bridge`。 ODBC桥梁程序与`clickhouse-server`来自相同的安装包.

该引擎支持 [Nullable](../../../sql-reference/data-types/nullable.md) 数据类型。

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

详情请见 [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) 查询。

表结构可以与源表结构不同:

-   列名应与源表中的列名相同，但您可以按任何顺序使用其中的一些列。
-   列类型可能与源表中的列类型不同。 ClickHouse尝试将数值[映射](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) 到ClickHouse的数据类型。
-   设置 `external_table_functions_use_nulls` 来定义如何处理 Nullable 列. 默认值是 true, 当设置为 false 时 - 表函数将不会使用 nullable 列，而是插入默认值来代替 null. 这同样适用于数组数据类型中的 null 值.

**引擎参数**

-   `connection_settings` — 在 `odbc.ini` 配置文件中，连接配置的名称.
-   `external_database` — 在外部 DBMS 中的数据库名.
-   `external_table` — `external_database`中的表名.

## 用法示例 {#usage-example}

**通过ODBC从本地安装的MySQL中检索数据**

本示例已经在 Ubuntu Linux 18.04 和 MySQL server 5.7 上测试通过。

请确保已经安装了 unixODBC 和 MySQL Connector。

默认情况下（如果从软件包安装），ClickHouse以用户`clickhouse`的身份启动. 因此，您需要在MySQL服务器中创建并配置此用户。

``` bash
$ sudo mysql
```

``` sql
mysql> CREATE USER 'clickhouse'@'localhost' IDENTIFIED BY 'clickhouse';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'clickhouse'@'clickhouse' WITH GRANT OPTION;
```

然后在`/etc/odbc.ini`中配置连接.

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

您可以从安装的 unixodbc 中使用 `isql` 实用程序来检查连接情况。

``` bash
$ isql -v mysqlconn
+---------------------------------------+
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
+--------+--------------+-------+----------------+
| int_id | int_nullable | float | float_nullable |
+--------+--------------+-------+----------------+
|      1 |         NULL |     2 |           NULL |
+--------+--------------+-------+----------------+
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

-   [ODBC 外部字典](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-odbc)
-   [ODBC 表函数](../../../sql-reference/table-functions/odbc.md)

[原始文章](https://clickhouse.tech/docs/en/operations/table_engines/odbc/) <!--hide-->
