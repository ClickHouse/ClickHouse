---
description: 'SQLite データベースに接続して、`INSERT` および `SELECT`
  クエリを実行し、ClickHouse と SQLite 間でデータをやり取りできます。'
sidebar_label: 'SQLite'
sidebar_position: 55
slug: /engines/database-engines/sqlite
title: 'SQLite'
doc_type: 'reference'
---

[SQLite](https://www.sqlite.org/index.html) データベースに接続して、`INSERT` および `SELECT` クエリを実行し、ClickHouse と SQLite 間でデータをやり取りできます。

<div id="creating-a-database">
  ## データベースの作成
</div>

```sql
    CREATE DATABASE sqlite_database
    ENGINE = SQLite('db_path')
```

**エンジンパラメータ**

* `db_path` — SQLite データベースのファイルへのパス。

<div id="data_types-support">
  ## データ型のサポート
</div>

以下の表は、ClickHouse が SQLite からスキーマを自動的に推論する際のデフォルトの型マッピングを示しています。

| SQLite  | ClickHouse                                          |
| ------- | --------------------------------------------------- |
| INTEGER | [Int32](../../sql-reference/data-types/int-uint.md) |
| REAL    | [Float32](../../sql-reference/data-types/float.md)  |
| TEXT    | [String](../../sql-reference/data-types/string.md)  |
| TEXT    | [UUID](../../sql-reference/data-types/uuid.md)      |
| BLOB    | [String](../../sql-reference/data-types/string.md)  |

[SQLite table engine](../../engines/table-engines/integrations/sqlite.md) を使用して、特定の ClickHouse 型でテーブルを明示的に定義した場合、以下の ClickHouse 型は SQLite の TEXT カラムからパースできます。

* [Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md)
* [DateTime](../../sql-reference/data-types/datetime.md), [DateTime64](../../sql-reference/data-types/datetime64.md)
* [UUID](../../sql-reference/data-types/uuid.md)
* [Enum8, Enum16](../../sql-reference/data-types/enum.md)
* [Decimal32, Decimal64, Decimal128, Decimal256](../../sql-reference/data-types/decimal.md)
* [FixedString](../../sql-reference/data-types/fixedstring.md)
* すべての整数型 ([UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64](../../sql-reference/data-types/int-uint.md)) 
* [Float32, Float64](../../sql-reference/data-types/float.md)

SQLite は動的型付けを採用しており、型アクセス関数では自動的に型変換が行われます。たとえば、TEXT カラムを整数として読み取ると、そのテキストを数値としてパースできない場合は 0 が返されます。つまり、ClickHouse テーブルが基になる SQLite カラムとは異なる型で定義されていると、エラーになる代わりに値が暗黙的に型変換される可能性があります。

<div id="specifics-and-recommendations">
  ## 特徴と推奨事項
</div>

SQLite は、データベース全体 (定義、テーブル、インデックス、およびデータそのもの) を、ホストマシン上の単一のクロスプラットフォームなファイルとして保存します。SQLite は書き込み時にデータベースファイル全体をロックするため、書き込み操作は順次実行されます。読み取り操作は並行して実行できます。
SQLite では、サービス管理 (起動スクリプトなど) や、`GRANT` とパスワードに基づくアクセス制御は必要ありません。アクセス制御は、データベースファイル自体に設定されたファイルシステム権限によって行われます。

<div id="usage-example">
  ## 使用例
</div>

SQLite に接続された ClickHouse のデータベース:

```sql
CREATE DATABASE sqlite_db ENGINE = SQLite('sqlite.db');
SHOW TABLES FROM sqlite_db;
```

```text
┌──name───┐
│ table1  │
│ table2  │
└─────────┘
```

テーブルが表示されます：

```sql
SELECT * FROM sqlite_db.table1;
```

```text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```

ClickHouseテーブルからSQLiteテーブルへデータを挿入する:

```sql
CREATE TABLE clickhouse_table(`col1` String,`col2` Int16) ENGINE = MergeTree() ORDER BY col2;
INSERT INTO clickhouse_table VALUES ('text',10);
INSERT INTO sqlite_db.table1 SELECT * FROM clickhouse_table;
SELECT * FROM sqlite_db.table1;
```

```text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
│ text  │   10 │
└───────┴──────┘
```