---
machine_translated: true
machine_translated_rev: b111334d6614a02564cf32f379679e9ff970d9b1
toc_priority: 34
toc_title: JDBC
---

# JDBC {#table-engine-jdbc}

允许ClickHouse通过以下方式连接到外部数据库 [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity).

要实现JDBC连接，ClickHouse使用单独的程序 [ﾂ暗ｪﾂ氾环催ﾂ団ﾂ法ﾂ人](https://github.com/alex-krash/clickhouse-jdbc-bridge) 这应该作为守护进程运行。

该引擎支持 [可为空](../../../sql-reference/data-types/nullable.md) 数据类型。

## 创建表 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    columns list...
)
ENGINE = JDBC(dbms_uri, external_database, external_table)
```

**发动机参数**

-   `dbms_uri` — URI of an external DBMS.

    格式: `jdbc:<driver_name>://<host_name>:<port>/?user=<username>&password=<password>`.
    Mysql的示例: `jdbc:mysql://localhost:3306/?user=root&password=root`.

-   `external_database` — Database in an external DBMS.

-   `external_table` — Name of the table in `external_database`.

## 用法示例 {#usage-example}

通过直接与它的控制台客户端连接在MySQL服务器中创建一个表:

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

在ClickHouse服务器中创建表并从中选择数据:

``` sql
CREATE TABLE jdbc_table
(
    `int_id` Int32,
    `int_nullable` Nullable(Int32),
    `float` Float32,
    `float_nullable` Nullable(Float32)
)
ENGINE JDBC('jdbc:mysql://localhost:3306/?user=root&password=root', 'test', 'test')
```

``` sql
SELECT *
FROM jdbc_table
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴────────────────┘
```

## 另请参阅 {#see-also}

-   [JDBC表函数](../../../sql-reference/table-functions/jdbc.md).

[原始文章](https://clickhouse.tech/docs/en/operations/table_engines/jdbc/) <!--hide-->
