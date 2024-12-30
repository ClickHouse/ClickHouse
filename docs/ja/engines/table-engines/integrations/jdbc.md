---
slug: /ja/engines/table-engines/integrations/jdbc
sidebar_position: 100
sidebar_label: JDBC
---

# JDBC

:::note
clickhouse-jdbc-bridgeにはエクスペリメンタルなコードが含まれており、もはやサポートされていません。信頼性の問題やセキュリティの脆弱性を含む可能性があります。使用する際は自己責任でお願いします。
ClickHouseでは、Postgres、MySQL、MongoDBなどのアドホックなクエリシナリオに対するより良い代替手段を提供するClickHouseに内蔵されたテーブル関数を使用することを推奨しています。
:::

ClickHouseが外部データベースに[**JDBC**](https://en.wikipedia.org/wiki/Java_Database_Connectivity)経由で接続することを可能にします。

JDBC接続を実装するために、ClickHouseはデーモンとして実行される別個のプログラム[clickhouse-jdbc-bridge](https://github.com/ClickHouse/clickhouse-jdbc-bridge)を使用します。

このエンジンは[Nullable](../../../sql-reference/data-types/nullable.md)データ型をサポートしています。

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    columns list...
)
ENGINE = JDBC(datasource_uri, external_database, external_table)
```

**エンジンパラメータ**


- `datasource_uri` — 外部DBMSのURIまたは名前。

    URI形式: `jdbc:<driver_name>://<host_name>:<port>/?user=<username>&password=<password>`。
    MySQLの例: `jdbc:mysql://localhost:3306/?user=root&password=root`。

- `external_database` — 外部DBMSのデータベース。

- `external_table` — `external_database`内のテーブル名、または`select * from table1 where column1=1`のようなselect クエリ。

## 使用例 {#usage-example}

MySQLサーバーのコンソールクライアントに直接接続してテーブルを作成します:

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

ClickHouseサーバーでテーブルを作成しデータを選択します:

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

## 関連項目 {#see-also}

- [JDBCテーブル関数](../../../sql-reference/table-functions/jdbc.md)。
