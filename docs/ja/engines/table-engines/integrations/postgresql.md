---
slug: /ja/engines/table-engines/integrations/postgresql
title: PostgreSQL テーブルエンジン
sidebar_position: 160
sidebar_label: PostgreSQL
---

PostgreSQLエンジンを使用すると、リモートのPostgreSQLサーバーに保存されているデータに対して`SELECT`および`INSERT`クエリを実行できます。

:::note
現在、PostgreSQLバージョン12以上のみがサポートされています。
:::

:::note Postgresデータのレプリケーションまたは移行にPeerDBを使用する
> Postgresテーブルエンジンに加えて、ClickHouseによる[PeerDB](https://docs.peerdb.io/introduction)を使用して、PostgresからClickHouseへの継続的なデータパイプラインを設定できます。PeerDBは、変化データキャプチャ（CDC）を使用してPostgresからClickHouseにデータをレプリケートするために設計されたツールです。
:::

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 type1 [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 type2 [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = PostgreSQL({host:port, database, table, user, password[, schema, [, on_conflict]] | named_collection[, option=value [,..]]})
```

[CREATE TABLE](../../../sql-reference/statements/create/table.md#create-table-query) クエリの詳細な説明を参照してください。

テーブル構造は、オリジナルのPostgreSQLテーブル構造と異なる場合があります：

- カラム名はオリジナルのPostgreSQLテーブルと同じである必要がありますが、これらのカラムの一部を任意の順序で使用できます。
- カラムタイプは、オリジナルのPostgreSQLテーブルのタイプと異なる場合があります。ClickHouseは値をClickHouseデータタイプに[キャスト](../../../engines/database-engines/postgresql.md#data_types-support)しようとします。
- [external_table_functions_use_nulls](../../../operations/settings/settings.md#external-table-functions-use-nulls)設定は、Nullableカラムをどのように処理するかを定義します。デフォルト値: 1。0の場合、テーブル関数はNullableカラムを作成せず、nullの代わりにデフォルト値を挿入します。これは配列内のNULL値にも適用されます。

**エンジンパラメータ**

- `host:port` — PostgreSQLサーバーのアドレス。
- `database` — リモートのデータベース名。
- `table` — リモートのテーブル名。
- `user` — PostgreSQLユーザー。
- `password` — ユーザーパスワード。
- `schema` — デフォルト以外のテーブルスキーマ。オプション。
- `on_conflict` — 衝突解決戦略。例: `ON CONFLICT DO NOTHING`。オプション。このオプションを追加すると挿入が非効率になります。

プロダクション環境では[名前付きコレクション](/docs/ja/operations/named-collections.md)（バージョン21.11以降で利用可能）が推奨されます。以下は例です：

```
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

一部のパラメータはキー値引数によって上書き可能です：
``` sql
SELECT * FROM postgresql(postgres_creds, table='table1');
```

## 実装の詳細 {#implementation-details}

PostgreSQL側の`SELECT`クエリは、読み取り専用のPostgreSQLトランザクション内で`COPY (SELECT ...) TO STDOUT`として実行され、各`SELECT`クエリの後にコミットされます。

`=`, `!=`, `>`, `>=`, `<`, `<=`, `IN`のような単純な`WHERE`句はPostgreSQLサーバーで実行されます。

すべての結合、集計、並べ替え、`IN [ array ]`条件、および`LIMIT`サンプリング制約は、PostgreSQLへのクエリが終了した後にのみClickHouseで実行されます。

PostgreSQL側の`INSERT`クエリは、各`INSERT`ステートメントの後に自動コミットされるPostgreSQLトランザクション内で`COPY "table_name" (field1, field2, ... fieldN) FROM STDIN`として実行されます。

PostgreSQLの`Array`型はClickHouseの配列に変換されます。

:::note
注意 - PostgreSQLでは、`type_name[]`のように作成された配列データは、同じカラム内の異なるテーブル行に異なる次元の多次元配列を含むことがあります。しかしClickHouseでは、同じカラム内のすべてのテーブル行に同じ次元数の多次元配列を持つことのみが許可されています。
:::

複数のレプリカをサポートしており、`|`でリストする必要があります。例：

```sql
CREATE TABLE test_replicas (id UInt32, name String) ENGINE = PostgreSQL(`postgres{2|3|4}:5432`, 'clickhouse', 'test_replicas', 'postgres', 'mysecretpassword');
```

PostgreSQL Dictionary ソース用のレプリカプライオリティがサポートされています。マップ内の数字が大きいほど優先順位は低く、最高優先順位は`0`です。

以下の例では、レプリカ`example01-1`が最高優先順位を持っています：

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

## 使用例 {#usage-example}

### PostgreSQLのテーブル

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

### ClickHouseでのテーブル作成と上記で作成したPostgreSQLテーブルへの接続

この例では、[PostgreSQLテーブルエンジン](/docs/ja/engines/table-engines/integrations/postgresql.md)を使用して、ClickHouseテーブルをPostgreSQLテーブルに接続し、PostgreSQLデータベースに対して`SELECT`と`INSERT`の両方のステートメントを使用します：

``` sql
CREATE TABLE default.postgresql_table
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = PostgreSQL('localhost:5432', 'public', 'test', 'postges_user', 'postgres_password');
```

### SELECTクエリを使用してPostgreSQLテーブルからClickHouseテーブルに初期データを挿入

[postgresqlテーブル関数](/docs/ja/sql-reference/table-functions/postgresql.md)は、PostgreSQLからClickHouseへのデータをコピーし、ClickHouseでクエリを実行または分析を行うことでデータのクエリパフォーマンスを向上させるか、PostgreSQLからClickHouseへのデータ移行に使用されます。ここではPostgreSQLからClickHouseへデータをコピーするので、ClickHouseではMergeTreeテーブルエンジンを使用し、postgresql_copyと呼びます：

``` sql
CREATE TABLE default.postgresql_copy
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = MergeTree
ORDER BY (int_id);
```

``` sql
INSERT INTO default.postgresql_copy
SELECT * FROM postgresql('localhost:5432', 'public', 'test', 'postges_user', 'postgres_password');
```

### PostgreSQLテーブルからClickHouseテーブルへの増分データの挿入

初回の挿入後にPostgreSQLテーブルとClickHouseテーブルの間で継続的な同期を行う場合、ClickHouseでWHERE句を使用して、タイムスタンプまたはユニークなシーケンスIDに基づいてPostgreSQLに追加されたデータのみを挿入できます。

これは以前に追加された最大のIDまたはタイムスタンプを追跡するといった作業を必要とします。例：

``` sql
SELECT max(`int_id`) AS maxIntID FROM default.postgresql_copy;
```

その後、PostgreSQLテーブルからmaxIDより大きい値を挿入します：

``` sql
INSERT INTO default.postgresql_copy
SELECT * FROM postgresql('localhost:5432', 'public', 'test', 'postges_user', 'postgres_password');
WHERE int_id > maxIntID;
```

### ClickHouseテーブルからのデータ選択

``` sql
SELECT * FROM postgresql_copy WHERE str IN ('test');
```

``` text
┌─float_nullable─┬─str──┬─int_id─┐
│           ᴺᵁᴸᴸ │ test │      1 │
└────────────────┴──────┴────────┘
```

### デフォルト以外のスキーマを使用する

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

- [`postgresql`テーブル関数](../../../sql-reference/table-functions/postgresql.md)
- [PostgreSQLをDictionaryソースとして使用する](../../../sql-reference/dictionaries/index.md#dictionary-sources#dicts-external_dicts_dict_sources-postgresql)

## 関連コンテンツ

- ブログ: [ClickHouse と PostgreSQL - データヘブンでの出会い - パート1](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)
- ブログ: [ClickHouse と PostgreSQL - データヘブンでの出会い - パート2](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres-part-2)
