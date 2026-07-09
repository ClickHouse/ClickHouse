---
description: 'PostgreSQL エンジンでは、リモートの PostgreSQL サーバーに保存されたデータに対して `SELECT` および `INSERT` クエリを実行できます。'
sidebar_label: 'PostgreSQL'
sidebar_position: 160
slug: /engines/table-engines/integrations/postgresql
title: 'PostgreSQL テーブルエンジン'
doc_type: 'guide'
---

PostgreSQL エンジンでは、リモートの PostgreSQL サーバーに保存されたデータに対して `SELECT` および `INSERT` クエリを実行できます。

:::note
現在、テーブルエンジンでサポートされているのは PostgreSQL バージョン 12 以降のみです。
:::

:::tip
[Managed Postgres](/ja/docs/cloud/managed-postgres) サービスもぜひご利用ください。コンピュートと物理的に同一配置された NVMe ストレージを基盤としており、EBS のようなネットワーク接続型ストレージを使用する代替手段と比べて、ディスク I/O がボトルネックになるワークロードで最大 10 倍高速なパフォーマンスを発揮します。また、ClickPipes の Postgres CDC (変更データキャプチャ) コネクタを使用して、Postgres のデータを ClickHouse にレプリケートできます。
:::

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 type1 [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 type2 [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = PostgreSQL({host:port, database, table, user, password[, schema, [, on_conflict]] | named_collection[, option=value [,..]]})
```

[CREATE TABLE](/ja/sql-reference/statements/create/table) クエリの詳細な説明を参照してください。

テーブル構造は、元の PostgreSQL テーブル構造と異なる場合があります。

* カラム名は元の PostgreSQL テーブルと同じである必要がありますが、その一部のカラムだけを任意の順序で使用できます。
* カラム型は、元の PostgreSQL テーブルのものと異なっていてもかまいません。ClickHouse は値を ClickHouse のデータ型に [キャスト](../../../engines/database-engines/postgresql.md#data_types-support) しようとします。
* [external&#95;table&#95;functions&#95;use&#95;nulls](/ja/operations/settings/settings#external_table_functions_use_nulls) 設定は、Nullable カラムの扱い方を定義します。デフォルト値は 1 です。0 の場合、テーブル関数は Nullable カラムを作成せず、null の代わりにデフォルト値を挿入します。これは配列内の NULL 値にも適用されます。

**エンジンパラメータ**

* `host:port` — PostgreSQL server のアドレス。
* `database` — リモートのデータベース名。
* `table` — リモートのテーブル名、または PostgreSQL にそのまま渡されるクエリ ([Passing a query instead of a table name](#passing-a-query) を参照) 。
* `user` — PostgreSQL ユーザー。
* `password` — ユーザーのパスワード。
* `schema` — デフォルト以外のテーブルスキーマ。省略可能です。
* `on_conflict` — 競合解決戦略。例: `ON CONFLICT DO NOTHING`。省略可能です。注意: このオプションを追加すると、挿入の効率が低下します。

本番環境では、[Named collections](/ja/operations/named-collections.md) (バージョン 21.11 以降で利用可能) の使用を推奨します。以下に例を示します。

```xml
<named_collections>
    <postgres_creds>
        <host>localhost</host>
        <port>5432</port>
        <user>postgres</user>
        <password>****</password>
        <schema>schema1</schema>
    </postgres_creds>
</named_collections>
```

一部のパラメーターは、キーと値の引数で上書きできます:

```sql
SELECT * FROM postgresql(postgres_creds, table='table1');
```

<div id="implementation-details">
  ## 実装の詳細
</div>

PostgreSQL側の`SELECT`クエリは、読み取り専用のPostgreSQLトランザクション内で`COPY (SELECT ...) TO STDOUT`として実行され、各`SELECT`クエリの後にコミットされます。

`=`, `!=`, `>`, `>=`, `<`, `<=`, および `IN` などの単純な`WHERE`句は、PostgreSQLサーバー上で実行されます。

JOIN、集計、ソート、`IN [ array ]` 条件、および`LIMIT`によるサンプリング制約は、いずれもPostgreSQLへのクエリが完了した後にのみ、ClickHouseで実行されます。

<div id="passing-a-query">
  ## テーブル名の代わりにクエリを渡す
</div>

テーブル名の代わりに、`table` 引数には、そのまま PostgreSQL に渡される `SELECT` クエリを指定できます。テーブルの構造は、クエリ結果から推論されます。クエリは、サブクエリとして記述することも、`query` 関数でラップすることもできます。

```sql
CREATE TABLE pg_table ENGINE = PostgreSQL('localhost:5432', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
CREATE TABLE pg_table ENGINE = PostgreSQL('localhost:5432', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

これは、JOIN、集計、その他の処理を PostgreSQL にプッシュダウンする際に便利です。このようなテーブルは読み取り専用で、これに対する `INSERT` は許可されていません。同じ構文は [`postgresql`](/ja/sql-reference/table-functions/postgresql) テーブル関数でもサポートされています。

:::note
サブクエリ形式 `(SELECT ...)` は ClickHouse によって解析され、サーバーに送信される前に PostgreSQL の方言 (PostgreSQL の識別子のクォートと文字列リテラルのエスケープ) で再シリアライズされます。そのため、有効な ClickHouse SQL である必要があります。ClickHouse が解析しない PostgreSQL 固有の構文を渡すには、`query('...')` 形式を使用してください。この形式のテキストは、そのまま PostgreSQL に送信されます。

周囲の ClickHouse クエリの外側にある `WHERE`、`LIMIT`、集計などは、渡されたクエリには**プッシュダウンされません**。代わりに、クエリ結果全体を取得した後に ClickHouse 側で適用されます。PostgreSQL から読み取るデータを制限するには、渡すクエリの中にフィルターを含めてください。[`external_table_strict_query = 1`](/ja/operations/settings/settings#external_table_strict_query) を使用すると、プッシュダウンできない外側のフィルターは、ローカルで適用される代わりに例外として拒否されます。
:::

PostgreSQL 側の `INSERT` クエリは、PostgreSQL のトランザクション内で `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` として実行され、各 `INSERT` ステートメントの後に自動コミットされます。

PostgreSQL の `Array` 型は ClickHouse の配列に変換されます。

:::note
注意してください。PostgreSQL では、`type_name[]` のように作成された配列データには、同じカラム内でも行ごとに次元数の異なる多次元配列を含めることができます。しかし ClickHouse では、同じカラム内のすべての行で次元数が同じ多次元配列しか許可されていません。
:::

複数のレプリカをサポートしており、それらは `|` で列挙する必要があります。例えば:

```sql
CREATE TABLE test_replicas (id UInt32, name String) ENGINE = PostgreSQL(`postgres{2|3|4}:5432`, 'clickhouse', 'test_replicas', 'postgres', 'mysecretpassword');
```

PostgreSQL の Dictionary ソースでは、レプリカの優先度がサポートされています。map 内の数値が大きいほど、優先度は低くなります。最も高い優先度は `0` です。

以下の例では、レプリカ `example01-1` の優先度が最も高くなっています:

```xml
<postgresql>
    <port>5432</port>
    <user>clickhouse</user>
    <password>qwerty</password>
    <replica>
        <host>example01-1</host>
        <priority>1</priority>
    </replica>
    <replica>
        <host>example01-2</host>
        <priority>2</priority>
    </replica>
    <db>db_name</db>
    <table>table_name</table>
    <where>id=10</where>
    <invalidate_query>SQL_QUERY</invalidate_query>
</postgresql>
</source>
```

<div id="usage-example">
  ## 使用例
</div>

<div id="table-in-postgresql">
  ### PostgreSQL のテーブル
</div>

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

<div id="creating-table-in-clickhouse-and-connecting-to--postgresql-table-created-above">
  ### ClickHouseでテーブルを作成し、上で作成したPostgreSQLテーブルに接続する
</div>

この例では、[PostgreSQL テーブルエンジン](/ja/engines/table-engines/integrations/postgresql.md)を使用してClickHouseテーブルをPostgreSQLテーブルに接続し、PostgreSQLデータベースに対してSELECT文とINSERT文の両方を使用します。

```sql
CREATE TABLE default.postgresql_table
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = PostgreSQL('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password');
```

<div id="inserting-initial-data-from-postgresql-table-into-clickhouse-table-using-a-select-query">
  ### SELECTクエリを使用して、PostgreSQLテーブルからClickHouseテーブルに初期データを挿入する
</div>

[postgresql テーブル関数](/ja/sql-reference/table-functions/postgresql.md) は、PostgreSQL から ClickHouse へデータをコピーします。これは、PostgreSQL ではなく ClickHouse でデータのクエリや分析を実行することでクエリパフォーマンスを向上させる目的でよく使用されるほか、PostgreSQL から ClickHouse へのデータ移行にも利用できます。ここでは PostgreSQL から ClickHouse にデータをコピーするため、ClickHouse で MergeTree テーブルエンジン を使用し、これを postgresql&#95;copy と呼びます:

```sql
CREATE TABLE default.postgresql_copy
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = MergeTree
ORDER BY (int_id);
```

```sql
INSERT INTO default.postgresql_copy
SELECT * FROM postgresql('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password');
```

<div id="inserting-incremental-data-from-postgresql-table-into-clickhouse-table">
  ### PostgreSQLテーブルからClickHouseテーブルにインクリメンタルデータを挿入する
</div>

初回の挿入後もPostgreSQLテーブルとClickHouseテーブルの継続的な同期を行う場合は、ClickHouseのWHERE句を使用して、timestampまたは一意のシーケンスIDに基づき、PostgreSQLに追加されたデータだけを挿入できます。

そのためには、たとえば次のように、前回までに追加した最大IDまたはtimestampを記録しておく必要があります。

```sql
SELECT max(`int_id`) AS maxIntID FROM default.postgresql_copy;
```

その後、PostgreSQLテーブルから最大値を超える値を挿入します

```sql
INSERT INTO default.postgresql_copy
SELECT * FROM postgresql('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password')
WHERE int_id > (SELECT max(int_id) FROM default.postgresql_copy);
```

<div id="selecting-data-from-the-resulting-clickhouse-table">
  ### 生成された ClickHouse テーブルからデータを取得する
</div>

```sql
SELECT * FROM postgresql_copy WHERE str IN ('test');
```

```text
┌─float_nullable─┬─str──┬─int_id─┐
│           ᴺᵁᴸᴸ │ test │      1 │
└────────────────┴──────┴────────┘
```

<div id="using-non-default-schema">
  ### デフォルト以外のスキーマを使用する
</div>

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

* [`postgresql` テーブル関数](../../../sql-reference/table-functions/postgresql.md)
* [PostgreSQL を Dictionary ソースとして使用する](../../../sql-reference/statements/create/dictionary/sources/postgresql)

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [ClickHouse と PostgreSQL - データ界の理想的な組み合わせ - 第1部](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)
* ブログ: [ClickHouse と PostgreSQL - データ界の理想的な組み合わせ - 第2部](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres-part-2)