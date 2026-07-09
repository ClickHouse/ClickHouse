---
description: 'ClickHouse から JDBC 経由で外部データベースに接続できるようにします。'
sidebar_label: 'JDBC'
sidebar_position: 100
slug: /engines/table-engines/integrations/jdbc
title: 'JDBC テーブルエンジン'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="jdbc-table-engine">
  # JDBC テーブルエンジン
</div>

<CloudNotSupportedBadge />

:::note
clickhouse-jdbc-bridge には Experimental なコードが含まれており、現在はサポートされていません。信頼性の問題やセキュリティ上の脆弱性が存在する可能性があります。使用は自己責任で行ってください。
ClickHouse では、アドホックなクエリの用途には、ClickHouse に組み込まれているテーブル関数 (Postgres、MySQL、MongoDB など) の使用を推奨しています。こちらのほうがより適した代替手段です。
:::

ClickHouse が [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity) 経由で外部データベースに接続できるようにします。

JDBC 接続を実装するために、ClickHouse は別プログラム [clickhouse-jdbc-bridge](https://github.com/ClickHouse/clickhouse-jdbc-bridge) を使用します。これはデーモンとして実行する必要があります。

このエンジンは [Nullable](../../../sql-reference/data-types/nullable.md) データ型をサポートしています。

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    columns list...
)
ENGINE = JDBC(datasource, external_database, external_table)
```

**エンジンパラメータ**

* `datasource` — 外部DBMSのURIまたは名前。

  URIのフォーマット: `jdbc:<driver_name>://<host_name>:<port>/?user=<username>&password=<password>`.
  MySQLの例: `jdbc:mysql://localhost:3306/?user=root&password=root`.

* `external_database` — 外部DBMS内のデータベース名、または明示的に定義されたテーブルスキーマ (例を参照) 。

* `external_table` — 外部データベース内のテーブル名、または `select * from table1 where column1=1` のようなselectクエリ。

* これらのパラメータは、[名前付きコレクション](/ja/operations/named-collections.md)を使って渡すこともできます。

<div id="usage-example">
  ## 使用例
</div>

MySQLサーバーのコンソールクライアントに直接接続してテーブルを作成する例:

```text
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

ClickHouse server でテーブルを作成し、そのテーブルからデータを取得します:

```sql
CREATE TABLE jdbc_table
(
    `int_id` Int32,
    `int_nullable` Nullable(Int32),
    `float` Float32,
    `float_nullable` Nullable(Float32)
)
ENGINE JDBC('jdbc:mysql://localhost:3306/?user=root&password=root', 'test', 'test')
```

```sql
SELECT *
FROM jdbc_table
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴────────────────┘
```

```sql
INSERT INTO jdbc_table(`int_id`, `float`)
SELECT toInt32(number), toFloat32(number * 1.0)
FROM system.numbers
```

<div id="see-also">
  ## 関連項目
</div>

* [JDBC テーブル関数](../../../sql-reference/table-functions/jdbc.md).