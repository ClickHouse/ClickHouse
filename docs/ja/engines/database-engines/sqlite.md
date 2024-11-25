---
slug: /ja/engines/database-engines/sqlite
sidebar_position: 55
sidebar_label: SQLite
---

# SQLite

[SQLite](https://www.sqlite.org/index.html) データベースに接続し、ClickHouse と SQLite の間でデータを交換するために `INSERT` と `SELECT` クエリを実行できます。

## データベースの作成 {#creating-a-database}

``` sql
    CREATE DATABASE sqlite_database
    ENGINE = SQLite('db_path')
```

**エンジンパラメータ**

- `db_path` — SQLite データベースのファイルへのパス。

## データ型のサポート {#data_types-support}

|  SQLite   | ClickHouse                                              |
|---------------|---------------------------------------------------------|
| INTEGER       | [Int32](../../sql-reference/data-types/int-uint.md)     |
| REAL          | [Float32](../../sql-reference/data-types/float.md)      |
| TEXT          | [String](../../sql-reference/data-types/string.md)      |
| BLOB          | [String](../../sql-reference/data-types/string.md)      |

## 特記および推奨事項 {#specifics-and-recommendations}

SQLite はホストマシン上でデータベース全体（定義、テーブル、インデックス、およびデータ自体）を単一のクロスプラットフォームファイルとして保存します。書き込み中は SQLite がデータベースファイル全体をロックするため、書き込み操作は順次実行されます。読み取り操作はマルチタスク可能です。
SQLite は（起動スクリプトなどの）サービス管理や `GRANT` やパスワードに基づくアクセス制御を必要としません。アクセス制御は、データベースファイル自体に与えられたファイルシステムパーミッションによって管理されます。

## 使用例 {#usage-example}

ClickHouse で、SQLite に接続されたデータベース:

``` sql
CREATE DATABASE sqlite_db ENGINE = SQLite('sqlite.db');
SHOW TABLES FROM sqlite_db;
```

``` text
┌──name───┐
│ table1  │
│ table2  │
└─────────┘
```

テーブルを表示:

``` sql
SELECT * FROM sqlite_db.table1;
```

``` text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```
ClickHouse のテーブルから SQLite テーブルにデータを挿入:

``` sql
CREATE TABLE clickhouse_table(`col1` String,`col2` Int16) ENGINE = MergeTree() ORDER BY col2;
INSERT INTO clickhouse_table VALUES ('text',10);
INSERT INTO sqlite_db.table1 SELECT * FROM clickhouse_table;
SELECT * FROM sqlite_db.table1;
```

``` text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
│ text  │   10 │
└───────┴──────┘
```
