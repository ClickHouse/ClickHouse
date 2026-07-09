---
description: 'リモートの PostgreSQL サーバー上のデータベースに接続できます。'
sidebar_label: 'PostgreSQL'
sidebar_position: 40
slug: /engines/database-engines/postgresql
title: 'PostgreSQL'
doc_type: 'guide'
---

リモートの [PostgreSQL](https://www.postgresql.org) サーバー上のデータベースに接続できます。ClickHouse と PostgreSQL の間でデータをやり取りするための、読み取りおよび書き込み操作 (`SELECT` および `INSERT` クエリ) をサポートしています。

`SHOW TABLES` および `DESCRIBE TABLE` クエリにより、リモート PostgreSQL 上のテーブル一覧とテーブル構造にリアルタイムでアクセスできます。

テーブル構造の変更 (`ALTER TABLE ... ADD|DROP COLUMN`) をサポートしています。`use_table_cache` パラメータ (下記のエンジンパラメータを参照) が `1` に設定されている場合、テーブル構造はキャッシュされ、変更の有無は確認されませんが、`DETACH` および `ATTACH` クエリで更新できます。

<div id="creating-a-database">
  ## データベースの作成
</div>

```sql
CREATE DATABASE test_database
ENGINE = PostgreSQL('host:port', 'database', 'user', 'password'[, `schema`, `use_table_cache`]);
```

**エンジンパラメータ**

* `host:port` — PostgreSQL サーバーのアドレス。
* `database` — リモートデータベース名。
* `user` — PostgreSQL ユーザー。
* `password` — ユーザーのパスワード。
* `schema` — PostgreSQL スキーマ。
* `use_table_cache` — データベースのテーブル構造をキャッシュするかどうかを定義します。省略可能です。デフォルト値: `0`。

<div id="data_types-support">
  ## サポートされるデータ型
</div>

| PostgreSQL       | ClickHouse                                                      |
| ---------------- | --------------------------------------------------------------- |
| DATE             | [Date](../../sql-reference/data-types/date.md)                  |
| TIMESTAMP        | [DateTime](../../sql-reference/data-types/datetime.md)          |
| REAL             | [Float32](../../sql-reference/data-types/float.md)              |
| DOUBLE           | [Float64](../../sql-reference/data-types/float.md)              |
| DECIMAL, NUMERIC | [Decimal](../../sql-reference/data-types/decimal.md) (以下の注を参照)  |
| SMALLINT         | [Int16](../../sql-reference/data-types/int-uint.md)             |
| INTEGER          | [Int32](../../sql-reference/data-types/int-uint.md)             |
| BIGINT           | [Int64](../../sql-reference/data-types/int-uint.md)             |
| SERIAL           | [UInt32](../../sql-reference/data-types/int-uint.md)            |
| BIGSERIAL        | [UInt64](../../sql-reference/data-types/int-uint.md)            |
| TEXT, CHAR       | [String](../../sql-reference/data-types/string.md)              |
| INTEGER          | Nullable([Int32](../../sql-reference/data-types/int-uint.md))   |
| ARRAY            | [Array](../../sql-reference/data-types/array.md)                |

:::note
精度 `p` が 76 (`Decimal256` でサポートされる最大値) を超える PostgreSQL の `numeric(p, 0)` (たとえば、256 ビット整数の格納によく使われる `numeric(78, 0)`) は、`Decimal` ではなく [`Int256`](../../sql-reference/data-types/int-uint.md) にマッピングされます。`Int256` の範囲に収まらない値はエラーとなります。
:::

<div id="examples-of-use">
  ## 使用例
</div>

PostgreSQL サーバーとデータをやり取りする ClickHouse 内のデータベース:

```sql
CREATE DATABASE test_database
ENGINE = PostgreSQL('postgres1:5432', 'test_database', 'postgres', 'mysecretpassword', 'schema_name',1);
```

```sql
SHOW DATABASES;
```

```text
┌─name──────────┐
│ default       │
│ test_database │
│ system        │
└───────────────┘
```

```sql
SHOW TABLES FROM test_database;
```

```text
┌─name───────┐
│ test_table │
└────────────┘
```

PostgreSQLのテーブルからデータを読み取る:

```sql
SELECT * FROM test_database.test_table;
```

```text
┌─id─┬─value─┐
│  1 │     2 │
└────┴───────┘
```

PostgreSQLテーブルへのデータ書き込み:

```sql
INSERT INTO test_database.test_table VALUES (3,4);
SELECT * FROM test_database.test_table;
```

```text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```

PostgreSQLでテーブル構造が変更されたとします:

```sql
postgre> ALTER TABLE test_table ADD COLUMN data Text
```

データベースの作成時に `use_table_cache` パラメータが `1` に設定されていたため、ClickHouse のテーブル構造はキャッシュされており、そのため変更されませんでした。

```sql
DESCRIBE TABLE test_database.test_table;
```

```text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
└────────┴───────────────────┘
```

テーブルをデタッチして再度アタッチすると、構造が更新されました。

```sql
DETACH TABLE test_database.test_table;
ATTACH TABLE test_database.test_table;
DESCRIBE TABLE test_database.test_table;
```

```text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
│ data   │ Nullable(String)  │
└────────┴───────────────────┘
```

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [ClickHouse and PostgreSQL - データ界で最高の組み合わせ - 第1部](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)
* ブログ: [ClickHouse and PostgreSQL - データ界で最高の組み合わせ - 第2部](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres-part-2)