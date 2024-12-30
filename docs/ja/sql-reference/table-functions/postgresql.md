---
slug: /ja/sql-reference/table-functions/postgresql
sidebar_position: 160
sidebar_label: postgresql
---

# postgresql

リモートのPostgreSQLサーバーに保存されているデータに対して`SELECT`および`INSERT`クエリを実行することができます。

**構文**

``` sql
postgresql({host:port, database, table, user, password[, schema, [, on_conflict]] | named_collection[, option=value [,..]]})
```

**パラメータ**

- `host:port` — PostgreSQLサーバーのアドレス。
- `database` — リモートデータベース名。
- `table` — リモートテーブル名。
- `user` — PostgreSQLユーザー。
- `password` — ユーザーパスワード。
- `schema` — デフォルトでないテーブルスキーマ。オプション。
- `on_conflict` — コンフリクト解決戦略。例: `ON CONFLICT DO NOTHING`。オプション。

引数は[名前付きコレクション](/docs/ja/operations/named-collections.md)を使用して渡すこともできます。この場合、`host`と`port`は別々に指定する必要があります。この方法は本番環境に推奨されます。

**返される値**

元のPostgreSQLテーブルと同じカラムを持つテーブルオブジェクト。

:::note
テーブル関数`postgresql(...)`をテーブル名とカラム名リストから区別するために、`INSERT`クエリではキーワード`FUNCTION`または`TABLE FUNCTION`を使用しなければなりません。以下の例をご覧ください。
:::

## 実装の詳細

PostgreSQL側での`SELECT`クエリは、読み取り専用PostgreSQLトランザクション内で`COPY (SELECT ...) TO STDOUT`として実行され、各`SELECT`クエリ後にコミットされます。

`=`, `!=`, `>`, `>=`, `<`, `<=`, および`IN`などの単純な`WHERE`句はPostgreSQLサーバーで実行されます。

すべてのジョイン、集計、ソート、`IN [ array ]`条件、および`LIMIT`サンプリング制約は、PostgreSQLへのクエリ終了後にClickHouseのみで実行されます。

PostgreSQL側での`INSERT`クエリは、PostgreSQLトランザクション内で自動コミットで各`INSERT`文後に`COPY "table_name" (field1, field2, ... fieldN) FROM STDIN`として実行されます。

PostgreSQL配列型はClickHouseの配列に変換されます。

:::note
注意が必要です。PostgreSQLでは、データ型カラムが異なる次元の配列を含むことができるが、ClickHouseではすべての行で同じ次元の多次元配列のみが許可されます。
:::

複数のレプリカをサポートし、`|`でリストする必要があります。例えば：

```sql
SELECT name FROM postgresql(`postgres{1|2|3}:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

または

```sql
SELECT name FROM postgresql(`postgres1:5431|postgres2:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

PostgreSQL Dictionaryソースのレプリカ優先度をサポートします。マップ内の数字が大きいほど優先度が低くなります。最も高い優先度は`0`です。

**例**

PostgreSQLのテーブル:

``` text
postgres=# CREATE TABLE "public"."test" (
"int_id" SERIAL,
"int_nullable" INT NULL DEFAULT NULL,
"float" FLOAT NOT NULL,
"str" VARCHAR(100) NOT NULL DEFAULT '',
"float_nullable" FLOAT NULL DEFAULT NULL,
PRIMARY KEY (int_id));

CREATE TABLE

postgres=# INSERT INTO test (int_id, str, "float") VALUES (1,'test',2);
INSERT 0 1

postgresql> SELECT * FROM test;
  int_id | int_nullable | float | str  | float_nullable
 --------+--------------+-------+------+----------------
       1 |              |     2 | test |
(1 row)
```

ClickHouseからのデータ選択（プレーン引数使用）:

```sql
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password') WHERE str IN ('test');
```

または[名前付きコレクション](/docs/ja/operations/named-collections.md)を使用:

```sql
CREATE NAMED COLLECTION mypg AS
        host = 'localhost',
        port = 5432,
        database = 'test',
        user = 'postgresql_user',
        password = 'password';
SELECT * FROM postgresql(mypg, table='test') WHERE str IN ('test');
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

挿入:

```sql
INSERT INTO TABLE FUNCTION postgresql('localhost:5432', 'test', 'test', 'postgrsql_user', 'password') (int_id, float) VALUES (2, 3);
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password');
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
│      2 │         ᴺᵁᴸᴸ │     3 │      │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

デフォルトでないスキーマの使用:

```text
postgres=# CREATE SCHEMA "nice.schema";

postgres=# CREATE TABLE "nice.schema"."nice.table" (a integer);

postgres=# INSERT INTO "nice.schema"."nice.table" SELECT i FROM generate_series(0, 99) as t(i)
```

```sql
CREATE TABLE pg_table_schema_with_dots (a UInt32)
        ENGINE PostgreSQL('localhost:5432', 'clickhouse', 'nice.table', 'postgrsql_user', 'password', 'nice.schema');
```

**関連項目**

- [PostgreSQLテーブルエンジン](../../engines/table-engines/integrations/postgresql.md)
- [PostgreSQLをDictionaryのソースとして使用](../../sql-reference/dictionaries/index.md#dictionary-sources#dicts-external_dicts_dict_sources-postgresql)

## 関連コンテンツ

- ブログ: [ClickHouseとPostgreSQL - データ天国で結ばれた関係 - パート1](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)
- ブログ: [ClickHouseとPostgreSQL - データ天国で結ばれた関係 - パート2](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres-part-2)

### PeerDBを用いたPostgresデータのレプリケーションまたは移行

> テーブル関数に加えて、[PeerDB](https://docs.peerdb.io/introduction)を使用してPostgresからClickHouseへの継続的なデータパイプラインを設定することもできます。PeerDBは、変更データキャプチャ（CDC）を使用してPostgresからClickHouseにデータをレプリケートするために設計されたツールです。
