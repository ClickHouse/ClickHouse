---
slug: /ja/engines/table-engines/integrations/materialized-postgresql
sidebar_position: 130
sidebar_label: MaterializedPostgreSQL
---

# [エクスペリメンタル] MaterializedPostgreSQL

PostgreSQL テーブルの初期データダンプを作成し、レプリケーションプロセスを開始します。つまり、リモートの PostgreSQL データベース内の PostgreSQL テーブルで発生した新しい変更を逐次適用するためのバックグラウンドジョブを実行します。

:::note
このテーブルエンジンはエクスペリメンタルです。使用するには、設定ファイルまたは `SET` コマンドを使用して `allow_experimental_materialized_postgresql_table` を 1 に設定します:
```sql
SET allow_experimental_materialized_postgresql_table=1
```
:::

複数のテーブルが必要な場合は、テーブルエンジンではなく[MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md) データベースエンジンを使用し、レプリケートするテーブルを指定する `materialized_postgresql_tables_list` 設定（データベース`スキーマ`も追加可能になります）を使用することを強くお勧めします。これにより、CPU の観点で非常に優れ、リモート PostgreSQL データベース内の接続数とレプリケーションスロットが少なくて済みます。

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_table', 'postgres_user', 'postgres_password')
PRIMARY KEY key;
```

**エンジンパラメータ**

- `host:port` — PostgreSQL サーバーアドレス。
- `database` — リモートデータベース名。
- `table` — リモートテーブル名。
- `user` — PostgreSQL ユーザー。
- `password` — ユーザーパスワード。

## 要件 {#requirements}

1. PostgreSQL の設定ファイルで、[wal_level](https://www.postgresql.org/docs/current/runtime-config-wal.html) 設定は `logical` 、`max_replication_slots` パラメータは少なくとも `2` である必要があります。

2. `MaterializedPostgreSQL` エンジンを持つテーブルには、PostgreSQL テーブルのレプリカアイデンティティインデックス（デフォルトでは主キー、[レプリカアイデンティティインデックスの詳細はこちら](../../../engines/database-engines/materialized-postgresql.md#requirements)）と同じ主キーが必要です。

3. データベース[Atomic](https://en.wikipedia.org/wiki/Atomicity_(database_systems)) のみが許可されています。

4. `MaterializedPostgreSQL` テーブルエンジンは、PostgreSQL バージョンが >= 11 でのみ動作します。これは、[pg_replication_slot_advance](https://pgpedia.info/p/pg_replication_slot_advance.html) PostgreSQL 関数を必要とするためです。

## バーチャルカラム {#virtual-columns}

- `_version` — トランザクションカウンター。タイプ: [UInt64](../../../sql-reference/data-types/int-uint.md)。

- `_sign` — 削除マーク。タイプ: [Int8](../../../sql-reference/data-types/int-uint.md)。可能な値:
    - `1` — 行は削除されていません、
    - `-1` — 行は削除されています。

これらのカラムはテーブル作成時に追加する必要はありません。`SELECT` クエリで常にアクセス可能です。`_version` カラムは `WAL` の `LSN` 位置に等しいので、レプリケーションの最新状態を確認するために使用できます。

``` sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;

SELECT key, value, _version FROM postgresql_db.postgresql_replica;
```

:::note
[**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html) 値のレプリケーションはサポートされていません。データ型のデフォルト値が使用されます。
:::
