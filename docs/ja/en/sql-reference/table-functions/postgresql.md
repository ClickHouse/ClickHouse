---
description: 'リモートの PostgreSQL サーバーに保存されているデータに対して、`SELECT` および `INSERT` クエリを実行できます。'
sidebar_label: 'postgresql'
sidebar_position: 160
slug: /sql-reference/table-functions/postgresql
title: 'postgresql'
doc_type: 'reference'
---

リモートの PostgreSQL サーバーに保存されているデータに対して、`SELECT` および `INSERT` クエリを実行できます。

<div id="syntax">
  ## 構文
</div>

```sql
postgresql({host:port, database, table, user, password[, schema, [, on_conflict]] | named_collection[, option=value [,..]]})
```

<div id="arguments">
  ## 引数
</div>

| 引数            | 説明                                                                                  |
| ------------- | ----------------------------------------------------------------------------------- |
| `host:port`   | PostgreSQL サーバーのアドレス。                                                               |
| `database`    | リモートデータベースの名前。                                                                      |
| `table`       | リモートテーブルの名前、または PostgreSQL にそのまま渡されるクエリ ([テーブル名の代わりにクエリを渡す](#passing-a-query)を参照) 。 |
| `user`        | PostgreSQL ユーザー。                                                                    |
| `password`    | ユーザーのパスワード。                                                                         |
| `schema`      | デフォルト以外のテーブルスキーマ。省略可能。                                                              |
| `on_conflict` | 競合解決戦略。例: `ON CONFLICT DO NOTHING`。省略可能。                                            |

引数は [named collections](/ja/operations/named-collections.md) を使用して渡すこともできます。この場合、`host` と `port` は別々に指定する必要があります。この方法は本番環境での使用に推奨されます。

<div id="returned_value">
  ## 戻り値
</div>

元の PostgreSQL テーブルと同じカラムを持つテーブルオブジェクト。

:::note
カラム名のリストを伴うテーブル名と テーブル関数 `postgresql(...)` を `INSERT` クエリ内で区別するには、キーワード `FUNCTION` または `TABLE FUNCTION` を使用する必要があります。以下の例を参照してください。
:::

<div id="implementation-details">
  ## 実装の詳細
</div>

PostgreSQL 側の `SELECT` クエリは、読み取り専用の PostgreSQL トランザクション内で `COPY (SELECT ...) TO STDOUT` として実行され、各 `SELECT` クエリの後にコミットされます。

`=`, `!=`, `>`, `>=`, `<`, `<=`, `IN` などの単純な `WHERE` 句は、PostgreSQL サーバー上で実行されます。

すべての JOIN、集計、ソート、`IN [ array ]` 条件、および `LIMIT` のサンプリング制約は、PostgreSQL へのクエリが完了した後にのみ ClickHouse で実行されます。

<div id="passing-a-query">
  ## テーブル名の代わりにクエリを渡す
</div>

テーブル名の代わりに、第 3 引数には PostgreSQL にそのまま渡される `SELECT` クエリを指定できます。生成されるテーブルの構造は、クエリ結果から推論されます。クエリは、サブクエリとして記述することも、`query` 関数でラップすることもできます。

```sql
SELECT * FROM postgresql('localhost:5432', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
SELECT * FROM postgresql('localhost:5432', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

これは、JOIN、集計、そのほかの処理を PostgreSQL にプッシュダウンする場合に便利です。このようなテーブルは読み取り専用であり、これに対する `INSERT` は許可されません。同じ構文は、[`PostgreSQL`](/ja/engines/table-engines/integrations/postgresql) テーブルエンジンでもサポートされています。

:::note
サブクエリ形式 `(SELECT ...)` は ClickHouse によって解析され、サーバーに送信される前に PostgreSQL の SQL方言 (PostgreSQL の識別子のクォートと文字列リテラルのエスケープ) で再シリアライズされます。そのため、有効な ClickHouse SQL である必要があります。ClickHouse が解析しない PostgreSQL 固有の構文を渡すには、`query('...')` 形式を使用してください。この形式のテキストは、そのまま PostgreSQL に送信されます。

周囲の ClickHouse クエリにある外側の `WHERE`、`LIMIT`、集計などは、渡されたクエリには**プッシュダウンされません**。これらは完全なクエリ結果を取得した後に ClickHouse 側で適用されます。PostgreSQL から読み取るデータを制限するには、渡すクエリの中にフィルターを含めてください。[`external_table_strict_query = 1`](/ja/operations/settings/settings#external_table_strict_query) を指定すると、プッシュダウンできない外側のフィルターはローカルで適用される代わりに、例外として拒否されます。
:::

PostgreSQL 側の `INSERT` クエリは、各 `INSERT` ステートメントの後に自動コミットされる PostgreSQL トランザクション内で、`COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` として実行されます。

PostgreSQL の Array 型は ClickHouse の Array に変換されます。

:::note
注意してください。PostgreSQL では、Integer[] のような配列データ型のカラムに、行ごとに異なる次元の配列を含めることができますが、ClickHouse では、すべての行で同じ次元を持つ多次元配列しか許可されません。
:::

複数のレプリカをサポートしており、それらは `|` で区切って列挙する必要があります。例:

```sql
SELECT name FROM postgresql(`postgres{1|2|3}:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

または

```sql
SELECT name FROM postgresql(`postgres1:5431|postgres2:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

PostgreSQL Dictionary ソースのレプリカの優先度設定をサポートしています。`map` 内の数値が大きいほど、優先度は低くなります。最も高い優先度は `0` です。

<div id="examples">
  ## 例
</div>

PostgreSQLのテーブル：

```text
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

通常の引数を使用してClickHouseからデータを取得する:

```sql
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password') WHERE str IN ('test');
```

または、[named collections](/ja/operations/named-collections.md)を使用します。

```sql
CREATE NAMED COLLECTION mypg AS
        host = 'localhost',
        port = 5432,
        database = 'test',
        user = 'postgresql_user',
        password = 'password';
SELECT * FROM postgresql(mypg, table='test') WHERE str IN ('test');
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

データの挿入:

```sql
INSERT INTO TABLE FUNCTION postgresql('localhost:5432', 'test', 'test', 'postgrsql_user', 'password') (int_id, float) VALUES (2, 3);
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password');
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
│      2 │         ᴺᵁᴸᴸ │     3 │      │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

デフォルト以外のスキーマを使用する場合:

```text
postgres=# CREATE SCHEMA "nice.schema";

postgres=# CREATE TABLE "nice.schema"."nice.table" (a integer);

postgres=# INSERT INTO "nice.schema"."nice.table" SELECT i FROM generate_series(0, 99) as t(i)
```

```sql
CREATE TABLE pg_table_schema_with_dots (a UInt32)
        ENGINE PostgreSQL('localhost:5432', 'clickhouse', 'nice.table', 'postgrsql_user', 'password', 'nice.schema');
```

<div id="related">
  ## 関連
</div>

* [PostgreSQL テーブルエンジン](../../engines/table-engines/integrations/postgresql.md)
* [PostgreSQL を Dictionary ソースとして使用する方法](/ja/sql-reference/statements/create/dictionary/sources/postgresql)

<div id="replicating-or-migrating-postgres-data-with-peerdb">
  ### PeerDB を使用した Postgres データのレプリケーションまたは移行
</div>

> テーブル関数に加えて、ClickHouse の [PeerDB](https://docs.peerdb.io/introduction) を使えば、Postgres から ClickHouse への継続的なデータパイプラインをいつでも構築できます。PeerDB は、CDC (変更データキャプチャ) を使用して Postgres から ClickHouse にデータをレプリケートするために特化して設計されたツールです。