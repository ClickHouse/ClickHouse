---
description: 'CREATE DATABASE に関するドキュメント'
sidebar_label: 'データベース'
sidebar_position: 35
slug: /sql-reference/statements/create/database
title: 'CREATE DATABASE'
doc_type: 'reference'
---

新しいデータベースを作成します。

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)] [SETTINGS ...] [COMMENT 'Comment']
```

<div id="clauses">
  ## 句
</div>

<div id="if-not-exists">
  ### IF NOT EXISTS
</div>

`db_name` データベースがすでに存在する場合、ClickHouse は新しいデータベースを作成せず、次のようになります。

* この句が指定されている場合、例外はスローされません。
* この句が指定されていない場合、例外がスローされます。

<div id="on-cluster">
  ### ON CLUSTER
</div>

ClickHouse は、指定したクラスター内のすべてのサーバー上に `db_name` データベースを作成します。詳しくは、[Distributed DDL](../../../sql-reference/distributed-ddl.md) の記事を参照してください。

<div id="engine">
  ### エンジン
</div>

デフォルトでは、ClickHouse は独自の [Atomic](../../../engines/database-engines/atomic.md) データベースエンジンを使用します。ほかに、[MySQL](../../../engines/database-engines/mysql.md)、[PostgresSQL](../../../engines/database-engines/postgresql.md)、[MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md)、[Replicated](../../../engines/database-engines/replicated.md)、[SQLite](../../../engines/database-engines/sqlite.md) もあります。

<div id="comment">
  ### コメント
</div>

データベースの作成時にコメントを追加できます。

コメントは、すべてのデータベースエンジンでサポートされています。

**構文**

```sql
CREATE DATABASE db_name ENGINE = engine(...) COMMENT 'Comment'
```

**例**

```sql title="Query"
CREATE DATABASE db_comment ENGINE = Memory COMMENT 'The temporary database';
SELECT name, comment FROM system.databases WHERE name = 'db_comment';
```

```text title="Response"
┌─name───────┬─comment────────────────┐
│ db_comment │ The temporary database │
└────────────┴────────────────────────┘
```

<div id="settings">
  ### 設定
</div>

<div id="lazy-load-tables">
  #### lazy_load_tables
</div>

有効にすると、データベースの起動時にテーブルは完全には読み込まれません。代わりに、各テーブルに対して軽量なプロキシが作成され、実際のテーブルエンジンは初回アクセス時に実体化されます。これにより、多数のテーブルを持ち、そのうち実際にクエリされるのが一部に限られるデータベースでは、起動時間とメモリ使用量を削減できます。

```sql
CREATE DATABASE db_name ENGINE = Atomic SETTINGS lazy_load_tables = 1;
```

ディスク上にテーブルのメタデータを保存するデータベースエンジン (例: `Atomic`、`Ordinary`) に適用されます。ビュー、materialized view、辞書、およびテーブル関数を使用するテーブルは、この設定に関係なく常に即座にロードされます。

**使用するタイミング:** この設定は、テーブル数が非常に多く (数百〜数千) 、そのうち実際にクエリされるのが一部だけであるデータベースで有効です。テーブルエンジンオブジェクトの作成、データパーツのスキャン、バックグラウンドスレッドの初期化を最初のアクセス時まで遅らせることで、サーバーの起動時間とメモリ使用量を削減します。

**`system.tables` への影響:**

* テーブルにアクセスする前は、`system.tables` にはそのエンジンが `TableProxy` として表示されます。最初のアクセス後は、実際のエンジン名 (例: `MergeTree`) が表示されます。
* `total_rows` や `total_bytes` などのカラムは、実際のストレージがまだ作成されていないため、未ロードのテーブルでは `NULL` を返します。

**DDL 操作との相互作用:**

* `SELECT`、`INSERT`、`ALTER`、`DROP` は、最初に使用されたときに透過的に実際のテーブルエンジンのロードをトリガーします。
* `RENAME TABLE` はロードをトリガーせずに動作します。
* いったんテーブルがロードされると、サーバープロセスの存続期間中はロードされたままになります。

**制限事項:**

* `system.tables` のメタデータ (例: `total_rows`、`engine`) に依存する監視ツールでは、未ロードのテーブルについて不完全な情報が表示されることがあります。
* 未ロードのテーブルに対する最初のクエリでは、一回限りのロードコスト (保存されている `CREATE TABLE` ステートメントのパースとエンジンの初期化) が発生します。

デフォルト値: `0` (無効) 。