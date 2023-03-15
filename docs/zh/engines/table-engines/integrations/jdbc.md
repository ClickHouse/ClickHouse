---
toc_priority: 34
toc_title: JDBC表引擎
---

# JDBC {#table-engine-jdbc}

允许CH通过 [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity) 连接到外部数据库。


要实现JDBC连接，CH需要使用以后台进程运行的程序 [clickhouse-jdbc-bridge](https://github.com/ClickHouse/clickhouse-jdbc-bridge)。

该引擎支持 [Nullable](../../../sql-reference/data-types/nullable.md) 数据类型。


## 建表 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    columns list...
)
ENGINE = JDBC(datasource_uri, external_database, external_table)
```

**引擎参数**

-   `datasource_uri` — 外部DBMS的URI或名字.

    URI格式: `jdbc:<driver_name>://<host_name>:<port>/?user=<username>&password=<password>`.
    MySQL示例: `jdbc:mysql://localhost:3306/?user=root&password=root`.

-   `external_database` — 外部DBMS的数据库名.

-   `external_table` — `external_database`中的外部表名或类似`select * from table1 where column1=1`的查询语句.

## 用法示例 {#usage-example}

通过mysql控制台客户端来创建表

Creating a table in MySQL server by connecting directly with it’s console client:

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

在CH服务端创建表，并从中查询数据：

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

``` sql
INSERT INTO jdbc_table(`int_id`, `float`)
SELECT toInt32(number), toFloat32(number * 1.0)
FROM system.numbers
```

## 参见 {#see-also}

-   [JDBC表函数](../../../sql-reference/table-functions/jdbc.md).

[原始文档](https://clickhouse.com/docs/en/operations/table_engines/jdbc/) <!--hide-->
