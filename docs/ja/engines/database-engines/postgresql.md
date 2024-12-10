---
slug: /ja/engines/database-engines/postgresql
sidebar_position: 40
sidebar_label: PostgreSQL
---

# PostgreSQL

リモートの [PostgreSQL](https://www.postgresql.org) サーバー上のデータベースに接続できるようにします。ClickHouse と PostgreSQL の間でデータを交換するための読み書き操作（`SELECT` と `INSERT` クエリ）をサポートしています。

`SHOW TABLES` および `DESCRIBE TABLE` クエリを使用して、リモート PostgreSQL からのテーブルリストとテーブル構造にリアルタイムでアクセスできます。

テーブル構造の変更（`ALTER TABLE ... ADD|DROP COLUMN`）をサポートします。`use_table_cache` パラメーター（以下のエンジンパラメーターを参照）が `1` に設定されている場合、テーブル構造はキャッシュされ、変更がチェックされませんが、`DETACH` および `ATTACH` クエリで更新できます。

## データベースの作成 {#creating-a-database}

``` sql
CREATE DATABASE test_database
ENGINE = PostgreSQL('host:port', 'database', 'user', 'password'[, `schema`, `use_table_cache`]);
```

**エンジンパラメーター**

- `host:port` — PostgreSQL サーバーのアドレス。
- `database` — リモートデータベース名。
- `user` — PostgreSQL ユーザー。
- `password` — ユーザーパスワード。
- `schema` — PostgreSQL スキーマ。
- `use_table_cache` — データベースのテーブル構造をキャッシュするかどうかを定義します。オプション。デフォルト値: `0`。

## データ型のサポート {#data_types-support}

| PostgreSQL       | ClickHouse                                                   |
|------------------|--------------------------------------------------------------|
| DATE             | [Date](../../sql-reference/data-types/date.md)               |
| TIMESTAMP        | [DateTime](../../sql-reference/data-types/datetime.md)       |
| REAL             | [Float32](../../sql-reference/data-types/float.md)           |
| DOUBLE           | [Float64](../../sql-reference/data-types/float.md)           |
| DECIMAL, NUMERIC | [Decimal](../../sql-reference/data-types/decimal.md)         |
| SMALLINT         | [Int16](../../sql-reference/data-types/int-uint.md)          |
| INTEGER          | [Int32](../../sql-reference/data-types/int-uint.md)          |
| BIGINT           | [Int64](../../sql-reference/data-types/int-uint.md)          |
| SERIAL           | [UInt32](../../sql-reference/data-types/int-uint.md)         |
| BIGSERIAL        | [UInt64](../../sql-reference/data-types/int-uint.md)         |
| TEXT, CHAR       | [String](../../sql-reference/data-types/string.md)           |
| INTEGER          | Nullable([Int32](../../sql-reference/data-types/int-uint.md))|
| ARRAY            | [Array](../../sql-reference/data-types/array.md)             |

## 使用例 {#examples-of-use}

ClickHouse でのデータベースが、PostgreSQL サーバーとデータを交換します:

``` sql
CREATE DATABASE test_database
ENGINE = PostgreSQL('postgres1:5432', 'test_database', 'postgres', 'mysecretpassword', 'schema_name',1);
```

``` sql
SHOW DATABASES;
```

``` text
┌─name──────────┐
│ default       │
│ test_database │
│ system        │
└───────────────┘
```

``` sql
SHOW TABLES FROM test_database;
```

``` text
┌─name───────┐
│ test_table │
└────────────┘
```

PostgreSQL テーブルからデータを読み取ります:

``` sql
SELECT * FROM test_database.test_table;
```

``` text
┌─id─┬─value─┐
│  1 │     2 │
└────┴───────┘
```

PostgreSQL テーブルにデータを書き込みます:

``` sql
INSERT INTO test_database.test_table VALUES (3,4);
SELECT * FROM test_database.test_table;
```

``` text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```

PostgreSQL でテーブル構造が変更されたとします:

``` sql
postgre> ALTER TABLE test_table ADD COLUMN data Text
```

データベースを作成するときに `use_table_cache` パラメーターが `1` に設定されていたため、ClickHouse のテーブル構造はキャッシュされ、変更されていませんでした:

``` sql
DESCRIBE TABLE test_database.test_table;
```
``` text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
└────────┴───────────────────┘
```

テーブルをデタッチして再びアタッチした後、構造が更新されました:

``` sql
DETACH TABLE test_database.test_table;
ATTACH TABLE test_database.test_table;
DESCRIBE TABLE test_database.test_table;
```
``` text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
│ data   │ Nullable(String)  │
└────────┴───────────────────┘
```

## 関連コンテンツ

- ブログ: [ClickHouse and PostgreSQL - a match made in data heaven - part 1](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)
- ブログ: [ClickHouse and PostgreSQL - a Match Made in Data Heaven - part 2](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres-part-2)
