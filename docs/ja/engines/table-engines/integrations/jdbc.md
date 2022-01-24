---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 34
toc_title: JDBC
---

# JDBC {#table-engine-jdbc}

ClickHouseが外部データベースに接続できるようにする [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity).

JDBC接続を実装するには、ClickHouseは別のプログラムを使用します [clickhouse-jdbc-bridge](https://github.com/ClickHouse/clickhouse-jdbc-bridge) うにしてくれました。

このエンジンは [Null可能](../../../sql-reference/data-types/nullable.md) データ型。

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    columns list...
)
ENGINE = JDBC(datasource_uri, external_database, external_table)
```

**エンジン変数**

-   `datasource_uri` — URI or name of an external DBMS.

    URI形式: `jdbc:<driver_name>://<host_name>:<port>/?user=<username>&password=<password>`.
    MySQLの例: `jdbc:mysql://localhost:3306/?user=root&password=root`.

-   `external_database` — Database in an external DBMS.

-   `external_table` — Name of the table in `external_database` or a select query like `select * from table1 where column1=1`.

## 使用例 {#usage-example}

コンソールクライアントに直接接続してMySQL serverでテーブルを作成する:

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

ClickHouse serverでテーブルを作成し、そこからデータを選択する:

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

## も参照。 {#see-also}

-   [JDBCテーブル関数](../../../sql-reference/table-functions/jdbc.md).

[元の記事](https://clickhouse.com/docs/en/operations/table_engines/jdbc/) <!--hide-->
