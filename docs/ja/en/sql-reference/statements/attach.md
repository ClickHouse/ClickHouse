---
description: 'ATTACH に関するドキュメント'
sidebar_label: 'ATTACH'
sidebar_position: 40
slug: /sql-reference/statements/attach
title: 'ATTACH ステートメント'
doc_type: 'reference'
---

たとえば database を別の server に移動する際に、table または Dictionary をアタッチします。

**構文**

```sql
ATTACH TABLE|DICTIONARY|DATABASE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster] ...
```

このクエリはディスク上にデータを作成しません。データはすでに適切な場所に存在していることを前提とし、指定されたテーブル、Dictionary、またはデータベースに関する情報をサーバーに追加するだけです。`ATTACH` クエリを実行すると、サーバーはそのテーブル、Dictionary、またはデータベースの存在を認識するようになります。

テーブルが以前にデタッチされており ([DETACH](../../sql-reference/statements/detach.md) クエリ) 、その構造がすでに認識されている場合は、構造を定義せずに簡略記法を使用できます。

<div id="attach-existing-table">
  ## 既存のテーブルをアタッチ
</div>

**構文**

```sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

このクエリは、サーバーの起動時に使用されます。サーバーはテーブルのメタデータを `ATTACH` クエリを記述したファイルとして保存しており、起動時にはそれらをそのまま実行します (ただし、一部のシステムテーブルはサーバー上で明示的に作成されます) 。

テーブルが永続的にデタッチされていた場合、サーバーの起動時に再度アタッチされないため、`ATTACH` クエリを明示的に使用する必要があります。

<div id="create-new-table-and-attach-data">
  ## 新しいテーブルを作成してデータをアタッチする
</div>

<div id="with-specified-path-to-table-data">
  ### テーブルデータへのパスを指定する場合
</div>

このクエリは、指定した構造を持つ新しいテーブルを作成し、`user_files` 内の指定したディレクトリにあるテーブルデータをアタッチします。

**構文**

```sql
ATTACH TABLE name FROM 'path/to/data/' (col1 Type1, ...)
```

**例**

```sql title="Query"
DROP TABLE IF EXISTS test;
INSERT INTO TABLE FUNCTION file('01188_attach/test/data.TSV', 'TSV', 's String, n UInt8') VALUES ('test', 42);
ATTACH TABLE test FROM '01188_attach/test' (s String, n UInt8) ENGINE = File(TSV);
SELECT * FROM test;
```

```sql title="Response"
┌─s────┬──n─┐
│ test │ 42 │
└──────┴────┘
```

<div id="with-specified-table-uuid">
  ### テーブル UUID を指定する場合
</div>

このクエリは、指定した構造を持つ新しいテーブルを作成し、指定した UUID を持つテーブルのデータをアタッチします。
これは [Atomic](../../engines/database-engines/atomic.md) データベースエンジンでサポートされています。

**構文**

```sql
ATTACH TABLE name UUID '<uuid>' (col1 Type1, ...)
```

<div id="attach-mergetree-table-as-replicatedmergetree">
  ## MergeTree テーブルを ReplicatedMergeTree としてアタッチ
</div>

非レプリケートの MergeTree テーブルを ReplicatedMergeTree としてアタッチできます。ReplicatedMergeTree テーブルは、`default_replica_path` と `default_replica_name` 設定の値で作成されます。レプリケートされたテーブルを通常の MergeTree テーブルとしてアタッチすることも可能です。

このクエリでは、ZooKeeper 内のテーブルデータには影響しないことに注意してください。つまり、アタッチ後に `SYSTEM RESTORE REPLICA` を使用して ZooKeeper にメタデータを追加するか、`SYSTEM DROP REPLICA ... FROM ZKPATH ...` を使用してそれを削除する必要があります。

既存の ReplicatedMergeTree テーブルにレプリカを追加しようとしている場合は、変換元の MergeTree テーブル内のローカルデータがすべてデタッチされる点に注意してください。

**構文**

```sql
ATTACH TABLE [db.]name AS [NOT] REPLICATED
```

**テーブルをレプリケートテーブルに変換**

```sql
DETACH TABLE test;
ATTACH TABLE test AS REPLICATED;
SYSTEM RESTORE REPLICA test;
```

**テーブルを非レプリケート化する**

テーブルの ZooKeeper パスとレプリカ名を取得します:

```sql title="Query"
SELECT replica_name, zookeeper_path FROM system.replicas WHERE table='test';
```

```sql title="Response"
┌─replica_name─┬─zookeeper_path─────────────────────────────────────────────┐
│ r1           │ /clickhouse/tables/401e6a1f-9bf2-41a3-a900-abb7e94dff98/s1 │
└──────────────┴────────────────────────────────────────────────────────────┘
```

テーブルを非レプリケートとしてアタッチし、ZooKeeper からレプリカのデータを削除します:

```sql title="Query"
DETACH TABLE test;
ATTACH TABLE test AS NOT REPLICATED;
SYSTEM DROP REPLICA 'r1' FROM ZKPATH '/clickhouse/tables/401e6a1f-9bf2-41a3-a900-abb7e94dff98/s1';
```

<div id="attach-existing-dictionary">
  ## 既存のDictionaryをアタッチ
</div>

以前にデタッチされたDictionaryを再度アタッチします。

**構文**

```sql
ATTACH DICTIONARY [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

<div id="attach-existing-database">
  ## 既存のデータベースをアタッチ
</div>

以前にデタッチされたデータベースをアタッチします。

**構文**

```sql
ATTACH DATABASE [IF NOT EXISTS] name [ENGINE=<database engine>] [ON CLUSTER cluster]
```