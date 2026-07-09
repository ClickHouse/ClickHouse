---
description: '允许 ClickHouse 通过 ODBC 连接外部数据库。'
sidebar_label: 'ODBC'
sidebar_position: 150
slug: /engines/table-engines/integrations/odbc
title: 'ODBC 表引擎'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="odbc-table-engine">
  # ODBC 表引擎
</div>

<CloudNotSupportedBadge />

允许 ClickHouse 通过 [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity) 连接到外部数据库。

为安全实现 ODBC 连接，ClickHouse 使用单独的程序 `clickhouse-odbc-bridge`。如果直接从 `clickhouse-server` 加载 ODBC 驱动程序，驱动程序问题可能导致 ClickHouse 服务器崩溃。ClickHouse 会在需要时自动启动 `clickhouse-odbc-bridge`。ODBC bridge 程序与 `clickhouse-server` 来自同一个软件包。

此引擎支持 [Nullable](../../../sql-reference/data-types/nullable.md) 数据类型。

<div id="creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1],
    name2 [type2],
    ...
)
ENGINE = ODBC(datasource, external_database, external_table)
```

参见 [CREATE TABLE](/zh/sql-reference/statements/create/table) 查询的详细说明。

表结构可以与源表的结构不同：

* 列名应与源表中的列名相同，但你也可以只使用其中部分列，且顺序可以任意。
* 列类型可以与源表中的列类型不同。ClickHouse 会尝试将值 [cast](/zh/sql-reference/functions/type-conversion-functions#CAST) 为 ClickHouse 数据类型。
* [external&#95;table&#95;functions&#95;use&#95;nulls](/zh/operations/settings/settings#external_table_functions_use_nulls) 设置定义了如何处理 Nullable 列。默认值：1。如果为 0，则表函数不会创建 Nullable 列，而是插入默认值来代替 null 值。这同样适用于数组中的 NULL 值。

**引擎参数**

* `datasource` — `odbc.ini` 文件中包含连接设置的节名称。
* `external_database` — 外部 DBMS 中数据库的名称。
* `external_table` — `external_database` 中表的名称。

这些参数也可以通过[命名集合](/zh/operations/named-collections.md)传递。

<div id="usage-example">
  ## 使用示例
</div>

**通过 ODBC 从本地安装的 MySQL 获取数据**

此示例已在 Ubuntu Linux 18.04 和 MySQL 服务器 5.7 上验证通过。

请确保已安装 unixODBC 和 MySQL Connector。

默认情况下 (如果通过软件包安装) ，ClickHouse 会以用户 `clickhouse` 的身份启动。因此，您需要在 MySQL 服务器中创建并配置该用户。

```bash
$ sudo mysql
```

```sql
mysql> CREATE USER 'clickhouse'@'localhost' IDENTIFIED BY 'clickhouse';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'clickhouse'@'localhost' WITH GRANT OPTION;
```

然后在 `/etc/odbc.ini` 中配置连接。

```bash
$ cat /etc/odbc.ini
[mysqlconn]
DRIVER = /usr/local/lib/libmyodbc5w.so
SERVER = 127.0.0.1
PORT = 3306
DATABASE = test
USER = clickhouse
PASSWORD = clickhouse
```

您可以使用 unixODBC 安装包中的 `isql` 工具来检查连接是否正常。

```bash
$ isql -v mysqlconn
+-------------------------+
| Connected!                            |
|                                       |
...
```

MySQL 中的表：

```text
mysql> CREATE DATABASE test;
Query OK, 1 row affected (0,01 sec)

mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `int_nullable` INT NULL DEFAULT NULL,
    ->   `float` FLOAT NOT NULL,
    ->   `float_nullable` FLOAT NULL DEFAULT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into test.test (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from test.test;
+------+----------+-----+----------+
| int_id | int_nullable | float | float_nullable |
+------+----------+-----+----------+
|      1 |         NULL |     2 |           NULL |
+------+----------+-----+----------+
1 row in set (0,00 sec)
```

ClickHouse 中从 MySQL 表检索数据的表：

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

<div id="see-also">
  ## 另见
</div>

* [ODBC 字典](/zh/sql-reference/statements/create/dictionary/sources/odbc)
* [ODBC 表函数](../../../sql-reference/table-functions/odbc.md)