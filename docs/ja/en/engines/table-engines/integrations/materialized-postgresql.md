---
description: 'PostgreSQL テーブルの初期データダンプを含む ClickHouse テーブルを作成し、レプリケーションを開始します。'
sidebar_label: 'MaterializedPostgreSQL'
sidebar_position: 130
slug: /engines/table-engines/integrations/materialized-postgresql
title: 'MaterializedPostgreSQL テーブルエンジン'
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="materializedpostgresql-table-engine">
  # MaterializedPostgreSQL テーブルエンジン
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::note
ClickHouse Cloud ユーザーには、PostgreSQL から ClickHouse へのレプリケーションには [ClickPipes](/ja/integrations/clickpipes) の使用を推奨します。これは、PostgreSQL 向けの高性能な CDC (変更データキャプチャ) をネイティブにサポートしています。
:::

PostgreSQL テーブルの初期データダンプを含む ClickHouse テーブルを作成し、レプリケーションを開始します。つまり、リモートの PostgreSQL データベース内にある PostgreSQL テーブルで新たな変更が発生すると、それを反映するバックグラウンドジョブを実行します。

:::note
このテーブルエンジンは Experimental です。使用するには、設定ファイルで `allow_experimental_materialized_postgresql_table` を 1 に設定するか、`SET` コマンドを使用してください。

```sql
SET allow_experimental_materialized_postgresql_table=1
```

:::

複数のテーブルが必要な場合は、テーブルエンジンではなく [MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md) データベースエンジンを使用し、レプリケートするテーブルを指定する `materialized_postgresql_tables_list` 設定を使うことを強く推奨します (今後はデータベース `schema` も追加できるようになる予定です) 。この方法のほうが、CPU 使用量、connection 数、リモート PostgreSQL データベース内のレプリケーションスロット数の面で大幅に有利です。

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_table', 'postgres_user', 'postgres_password')
PRIMARY KEY key;
```

**エンジンパラメータ**

* `host:port` — PostgreSQLサーバーのアドレス。
* `database` — リモートデータベース名。
* `table` — リモートテーブル名。
* `user` — PostgreSQLユーザー。
* `password` — ユーザーのパスワード。

<div id="requirements">
  ## 要件
</div>

1. PostgreSQL の設定ファイルで、[wal&#95;level](https://www.postgresql.org/docs/current/runtime-config-wal.html) の値を `logical` に設定し、`max_replication_slots` パラメータの値を少なくとも `2` にする必要があります。

2. `MaterializedPostgreSQL` エンジンを持つテーブルには、PostgreSQL テーブルの replica identity index (デフォルトでは主キー) と同じ主キーが必要です ([replica identity index の詳細](../../../engines/database-engines/materialized-postgresql.md#requirements)を参照) 。

3. 使用できるのは [Atomic](https://en.wikipedia.org/wiki/Atomicity_\(database_systems\)) データベースのみです。

4. `MaterializedPostgreSQL` テーブルエンジンは、実装上 [pg&#95;replication&#95;slot&#95;advance](https://pgpedia.info/p/pg_replication_slot_advance.html) PostgreSQL 関数を必要とするため、PostgreSQL バージョン 11 以上でのみ動作します。

<div id="virtual-columns">
  ## 仮想カラム
</div>

* `_version` — トランザクションカウンター。型: [UInt64](../../../sql-reference/data-types/int-uint.md)。

* `_sign` — 削除マーク。型: [Int8](../../../sql-reference/data-types/int-uint.md)。設定可能な値:
  * `1` — 行は削除されていません。
  * `-1` — 行は削除されています。

これらのカラムは、テーブルの作成時に追加する必要はありません。これらのカラムには `SELECT` クエリから常にアクセスできます。
`_version` カラムは `WAL` 内の `LSN` の位置に対応するため、レプリケーションがどの程度最新かを確認するために使用できます。

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;

SELECT key, value, _version FROM postgresql_db.postgresql_replica;
```

:::note
[**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html) の値のレプリケーションには対応していません。データ型のデフォルト値が使用されます。
:::