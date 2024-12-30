---
slug: /ja/sql-reference/statements/system
sidebar_position: 36
sidebar_label: SYSTEM
---

# SYSTEM ステートメント

## RELOAD EMBEDDED DICTIONARIES

すべての[内部 Dictionary](../../sql-reference/dictionaries/index.md)をリロードします。デフォルトでは、内部 Dictionary は無効になっています。内部 Dictionary の更新結果に関わらず、常に `Ok.` を返します。

## RELOAD DICTIONARIES

以前に成功してロードされたすべての Dictionary をリロードします。デフォルトでは、Dictionary は遅延ロードされます（[dictionaries_lazy_load](../../operations/server-configuration-parameters/settings.md#dictionaries_lazy_load) を参照してください）。したがって、起動時に自動的にロードされるのではなく、dictGet 関数や ENGINE = Dictionary を持つテーブルからの SELECT を通して初めてアクセスされる時に初期化されます。`SYSTEM RELOAD DICTIONARIES` クエリは、そのような (LOADED) Dictionary をリロードします。Dictionary の更新結果に関わらず、常に `Ok.` を返します。

**構文**

```sql
SYSTEM RELOAD DICTIONARIES [ON CLUSTER cluster_name]
```

## RELOAD DICTIONARY

`dictionary_name` を Dictionary の状態 (LOADED / NOT_LOADED / FAILED) に関わらず完全にリロードします。Dictionary の更新結果に関わらず、常に `Ok.` を返します。

``` sql
SYSTEM RELOAD DICTIONARY [ON CLUSTER cluster_name] dictionary_name
```

Dictionary の状態は `system.dictionaries` テーブルをクエリすることで確認できます。

``` sql
SELECT name, status FROM system.dictionaries;
```

## RELOAD MODELS

:::note
このステートメントと `SYSTEM RELOAD MODEL` は、clickhouse-library-bridge から CatBoost モデルをアンロードするだけです。`catboostEvaluate()` 関数は、最初にアクセスされた時点でモデルがロードされていない場合にロードします。
:::

すべての CatBoost モデルをアンロードします。

**構文**

```sql
SYSTEM RELOAD MODELS [ON CLUSTER cluster_name]
```

## RELOAD MODEL

指定された `model_path` の CatBoost モデルをアンロードします。

**構文**

```sql
SYSTEM RELOAD MODEL [ON CLUSTER cluster_name] <model_path>
```

## RELOAD FUNCTIONS

登録されたすべての[実行可能なユーザー定義関数](../functions/index.md#executable-user-defined-functions)またはそれらの1つを設定ファイルからリロードします。

**構文**

```sql
RELOAD FUNCTIONS [ON CLUSTER cluster_name]
RELOAD FUNCTION [ON CLUSTER cluster_name] function_name
```

## RELOAD ASYNCHRONOUS METRICS

すべての[非同期メトリクス](../../operations/system-tables/asynchronous_metrics.md)を再計算します。非同期メトリクスは設定 [asynchronous_metrics_update_period_s](../../operations/server-configuration-parameters/settings.md) に基づいて定期的に更新されるため、このステートメントを使用して手動で更新する必要は通常ありません。

```sql
RELOAD ASYNCHRONOUS METRICS [ON CLUSTER cluster_name]
```

## DROP DNS CACHE

ClickHouse の内部 DNS キャッシュをクリアします。インフラストラクチャを変更する場合（別の ClickHouse サーバーのIPアドレスを変更する場合や Dictionary を使用するサーバーを変更する場合）、このコマンドを使用する必要があります（古い ClickHouse バージョンの場合）。

キャッシュの管理をより便利（自動化）にするために、disable_internal_dns_cache, dns_cache_max_entries, dns_cache_update_periodパラメータを参照してください。

## DROP MARK CACHE

マークキャッシュをクリアします。

## DROP REPLICA

`ReplicatedMergeTree` テーブルのデッド レプリカを次の構文を使用して削除できます。

``` sql
SYSTEM DROP REPLICA 'replica_name' FROM TABLE database.table;
SYSTEM DROP REPLICA 'replica_name' FROM DATABASE database;
SYSTEM DROP REPLICA 'replica_name';
SYSTEM DROP REPLICA 'replica_name' FROM ZKPATH '/path/to/table/in/zk';
```

クエリは `ReplicatedMergeTree` レプリカ パスを ZooKeeper から削除します。レプリカがデッド状態でメタデータが ZooKeeper から `DROP TABLE` によって削除されない場合などに役立ちます。非アクティブ/古いレプリカのみを削除し、ローカルレプリカを削除することはできません。これには `DROP TABLE` を使用してください。`DROP REPLICA` はテーブルを削除せず、ディスクからデータやメタデータを削除しません。

最初のものは、`database.table` テーブルの `'replica_name'` レプリカのメタデータを削除します。 2 番目のものは、データベース内のすべての複製テーブルについて同じ操作を実行します。 3 番目のものは、ローカル サーバー上のすべての複製テーブルについて同じ操作を行います。 4 番目は、テーブルの他のすべてのレプリカが削除されたときにデッドレプリカのメタデータを削除するのに便利です。作成時の `ReplicatedMergeTree` エンジンの最初の引数に指定したのと同じパスである必要があります。

## DROP DATABASE REPLICA

`Replicated` データベースのデッド レプリカを次の構文を使用して削除できます。

``` sql
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM DATABASE database;
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'];
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM ZKPATH '/path/to/table/in/zk';
```

`SYSTEM DROP REPLICA` と似ていますが、`DROP DATABASE` を実行するデータベースが存在しない場合に ZooKeeper から `Replicated` データベースのレプリカ パスを削除します。 `ReplicatedMergeTree` のレプリカは削除されないことに注意してください（そのため `SYSTEM DROP REPLICA` も必要になる場合があります）。 シャードおよびレプリカ名は、データベース作成時に `Replicated` エンジンの引数に指定された名前です。これらの名前は、`system.clusters` の `database_shard_name` および `database_replica_name` カラムから取得することもできます。 `FROM SHARD` 句がない場合、`replica_name` は `shard_name|replica_name` フォーマットの完全なレプリカ名である必要があります。

## DROP UNCOMPRESSED CACHE

非圧縮データキャッシュをクリアします。非圧縮データキャッシュは、クエリ/ユーザー/プロファイルレベルの設定 [use_uncompressed_cache](../../operations/settings/settings.md#setting-use_uncompressed_cache) で有効化/無効化されます。サーバーレベルの設定 [uncompressed_cache_size](../../operations/server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) を使用してそのサイズを構成できます。

## DROP COMPILED EXPRESSION CACHE

コンパイルされた式キャッシュをクリアします。コンパイルされた式キャッシュは、クエリ/ユーザー/プロファイルレベルの設定 [compile_expressions](../../operations/settings/settings.md#compile-expressions) で有効化/無効化されます。

## DROP QUERY CACHE

```sql
SYSTEM DROP QUERY CACHE;
SYSTEM DROP QUERY CACHE TAG '<tag>'
````

[クエリキャッシュ](../../operations/query-cache.md)をクリアします。タグが指定された場合、指定されたタグを持つクエリキャッシュエントリのみが削除されます。

## DROP FORMAT SCHEMA CACHE {#system-drop-schema-format}

[format_schema_path](../../operations/server-configuration-parameters/settings.md#format_schema_path) からロードされたスキーマのキャッシュをクリアします。

サポートされているフォーマット:

- Protobuf

```sql
SYSTEM DROP FORMAT SCHEMA CACHE [FOR Protobuf]
```

## FLUSH LOGS

バッファされたログメッセージをシステムテーブル（例: system.query_log）にフラッシュします。ほとんどのシステムテーブルはデフォルトでフラッシュ間隔が7.5秒に設定されているため、デバッグに役立ちます。
メッセージキューが空でもシステムテーブルを作成します。

```sql
SYSTEM FLUSH LOGS [ON CLUSTER cluster_name]
```

## RELOAD CONFIG

ClickHouseの設定をリロードします。設定がZooKeeperに保存されている場合に使用されます。`SYSTEM RELOAD CONFIG` はZooKeeperに保存された`USER` 設定をリロードせず、`users.xml` に保存された`USER` 設定のみをリロードします。すべての`USER` 設定をリロードするには `SYSTEM RELOAD USERS` を使用します。

```sql
SYSTEM RELOAD CONFIG [ON CLUSTER cluster_name]
```

## RELOAD USERS

すべてのアクセスストレージをリロードします。含まれるのは: users.xml、ローカルディスクアクセスストレージ、ZooKeeperでレプリケートされたアクセスストレージです。

```sql
SYSTEM RELOAD USERS [ON CLUSTER cluster_name]
```

## SHUTDOWN

ClickHouseを正常にシャットダウンします（`service clickhouse-server stop` / `kill {$pid_clickhouse-server}` のように）。

## KILL

ClickHouseプロセスを強制終了します（`kill -9 {$ pid_clickhouse-server}` のように）。

## 分散テーブルの管理

ClickHouse は[分散](../../engines/table-engines/special/distributed.md) テーブルを管理できます。ユーザーがこれらのテーブルにデータを挿入すると、ClickHouseは最初にデータをクラスタノードに送信するためのキューを作成し、その後データを非同期で送信します。[STOP DISTRIBUTED SENDS](#stop-distributed-sends), [FLUSH DISTRIBUTED](#flush-distributed), [START DISTRIBUTED SENDS](#start-distributed-sends) クエリを使用してキューの処理を管理できます。また、[distributed_foreground_insert](../../operations/settings/settings.md#distributed_foreground_insert) 設定を使用して分散データを同期的に挿入することもできます。

### STOP DISTRIBUTED SENDS

分散テーブルにデータを挿入する際のバックグラウンドデータ分散を無効化します。

``` sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

### FLUSH DISTRIBUTED

ClickHouse がクラスタノードにデータを同期的に送信することを強制します。ノードが使用できない場合、ClickHouse は例外をスローし、クエリの実行を停止します。すべてのノードが再度オンラインになった時に、クエリが成功するまで再試行できます。

`SETTINGS` 句を通じていくつかの設定を上書きすることもでき、`max_concurrent_queries_for_all_users` や `max_memory_usage` などの一時的な制限を回避するのに便利です。

``` sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name> [ON CLUSTER cluster_name] [SETTINGS ...]
```

:::note
保留中の各ブロックは、初期の INSERT クエリからの設定を使用してディスクに保存されます。そのため、設定を上書きしたい場合があります。
:::

### START DISTRIBUTED SENDS

分散テーブルにデータを挿入する際のバックグラウンドデータ分散を有効にします。

``` sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

### STOP LISTEN

指定したポートで指定したプロトコルを使用してサーバーへの既存の接続を優雅に終了し、ソケットを閉じます。

ただし、対応するプロトコルの設定が clickhouse-server の設定で指定されていない場合、このコマンドは効果がありません。

```sql
SYSTEM STOP LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

- `CUSTOM 'protocol'` 修飾子が指定されている場合、サーバー構成のプロトコルセクションで指定されたカスタムプロトコルが停止されます。
- `QUERIES ALL [EXCEPT .. [,..]]` 修飾子が指定されている場合、`EXCEPT` 句で指定されたものを除き、すべてのプロトコルが停止されます。
- `QUERIES DEFAULT [EXCEPT .. [,..]]` 修飾子が指定されている場合、`EXCEPT` 句で指定されたものを除き、すべてのデフォルトプロトコルが停止されます。
- `QUERIES CUSTOM [EXCEPT .. [,..]]` 修飾子が指定されている場合、`EXCEPT` 句で指定されたものを除き、すべてのカスタムプロトコルが停止されます。

### START LISTEN

指定されたプロトコルで新しい接続を確立できるようにします。

ただし、SYSTEM STOP LISTEN コマンドを使用して、指定されたポートとプロトコルでサーバーを停止していない場合、このコマンドは効果を発揮しません。

```sql
SYSTEM START LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

## MergeTree テーブルの管理

ClickHouse は [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) テーブルのバックグラウンドプロセスを管理できます。

### STOP MERGES

MergeTree ファミリ内のテーブルに対するバックグラウンド マージを停止する機能を提供します。

``` sql
SYSTEM STOP MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

:::note
テーブルを `DETACH / ATTACH` すると、以前にすべての MergeTree テーブルに対してマージが停止されていた場合でも、テーブルに対してバックグラウンド マージが開始されます。
:::

### START MERGES

MergeTree ファミリ内のテーブルに対するバックグラウンド マージを開始する機能を提供します。

``` sql
SYSTEM START MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

### STOP TTL MERGES

MergeTree ファミリのテーブルに対して [有効期限 (TTL) 式](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) に従って古いデータをバックグラウンドで削除することを停止する機能を提供します。 テーブルが存在しない場合でも `Ok.` を返しますが、データベースが存在しない場合はエラーを返します。

``` sql
SYSTEM STOP TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

### START TTL MERGES

MergeTree ファミリのテーブルに対して [有効期限 (TTL) 式](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) に従って古いデータをバックグラウンドで削除を開始する機能を提供します。 テーブルが存在しない場合でも `Ok.` を返します。データベースが存在しない場合はエラーを返します。

``` sql
SYSTEM START TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

### STOP MOVES

MergeTree ファミリのテーブルに対して [TO VOLUME または TO DISK 句を伴う TTL テーブル式](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) に従ってデータをバックグラウンドで移動することを停止する機能を提供します。 テーブルが存在しない場合でも `Ok.` を返します。データベースが存在しない場合はエラーを返します。

``` sql
SYSTEM STOP MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

### START MOVES

MergeTree ファミリのテーブルに対して [TO VOLUME または TO DISK 句を伴う TTL テーブル式](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) に従ってデータをバックグラウンドで移動を開始する機能を提供します。 テーブルが存在しない場合でも `Ok.` を返します。データベースが存在しない場合はエラーを返します。

``` sql
SYSTEM START MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

### SYSTEM UNFREEZE {#query_language-system-unfreeze}

指定された名前のフリーズされたバックアップをすべてのディスクからクリアします。[ALTER TABLE table_name UNFREEZE WITH NAME](alter/partition.md#alter_unfreeze-partition) で個々のパーツのアンフリーズについて詳しく学べます。

``` sql
SYSTEM UNFREEZE WITH NAME <backup_name>
```

### WAIT LOADING PARTS

テーブルのすべての非同期でロードされているデータパーツ（古いデータパーツ）がロードされるまで待機します。

``` sql
SYSTEM WAIT LOADING PARTS [ON CLUSTER cluster_name] [db.]merge_tree_family_table_name
```

## ReplicatedMergeTree テーブルの管理

ClickHouse は [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md#table_engines-replication) テーブルの背景レプリケーション関連プロセスを管理できます。

### STOP FETCHES

`ReplicatedMergeTree` ファミリ内のテーブルに対する挿入されたパーツのバックグラウンド フェッチを停止できる機能を提供します。 テーブルエンジンにかかわらず、またテーブルやデータベースが存在しない場合でも `Ok.` を常に返します。

``` sql
SYSTEM STOP FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### START FETCHES

`ReplicatedMergeTree` ファミリ内のテーブルに対する挿入されたパーツのバックグラウンド フェッチを開始できる機能を提供します。 テーブルエンジンにかかわらず、またテーブルやデータベースが存在しない場合でも `Ok.` を常に返します。

``` sql
SYSTEM START FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### STOP REPLICATED SENDS

`ReplicatedMergeTree` ファミリ内のテーブルに対する新しく挿入されたパーツを他のレプリカにバックグラウンドで送信することを停止できる機能を提供します。

``` sql
SYSTEM STOP REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### START REPLICATED SENDS

`ReplicatedMergeTree` ファミリ内のテーブルに対する新しく挿入されたパーツを他のレプリカにバックグラウンドで送信を開始する機能を提供します。

``` sql
SYSTEM START REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### STOP REPLICATION QUEUES

`ReplicatedMergeTree` ファミリのテーブルに対して ZooKeeper に格納されているレプリケーション キューからのバックグラウンド フェッチ タスク（マージ、フェッチ、ミューテーション、ON CLUSTER 句を含む DDL ステートメント）を停止する機能を提供します。

``` sql
SYSTEM STOP REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### START REPLICATION QUEUES

`ReplicatedMergeTree` ファミリのテーブルに対して ZooKeeper に格納されているレプリケーション キューからのバックグラウンド スタスク（マージ、フェッチ、ミューテーション、ON CLUSTER 句を含む DDL ステートメント）を開始する機能を提供します。

``` sql
SYSTEM START REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### STOP PULLING REPLICATION LOG

`ReplicatedMergeTree` テーブルのレプリケーション キューに新しいエントリを追加することから、新しいエントリのロードを停止します。

``` sql
SYSTEM STOP PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### START PULLING REPLICATION LOG

`SYSTEM STOP PULLING REPLICATION LOG` をキャンセルします。

``` sql
SYSTEM START PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### SYNC REPLICA

`ReplicatedMergeTree` テーブルがクラスター内の他のレプリカと同期するまで待機しますが、`receive_timeout` 秒を超えてはなりません。

``` sql
SYSTEM SYNC REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name [STRICT | LIGHTWEIGHT [FROM 'srcReplica1'[, 'srcReplica2'[, ...]]] | PULL]
```

このステートメントを実行すると、`[db.]replicated_merge_tree_family_table_name` は ZooKeeper の一般的なレプリケートログからエントリを取得してその独自のレプリケーション キューに追加し、クエリは取得されたコマンドがすべて処理されるまで待機します。サポートされている修飾子は次のとおりです:

- `STRICT` 修飾子が指定された場合、クエリはレプリケーション キューが空になるまで待機します。`STRICT` バージョンは、レプリケーション キューに新しいエントリが常に表示される場合、成功しない可能性があります。
- `LIGHTWEIGHT` 修飾子が指定された場合、クエリは `GET_PART`, `ATTACH_PART`, `DROP_RANGE`, `REPLACE_RANGE` および `DROP_PART` エントリの処理のみを待ちます。また、LIGHTWEIGHT 修飾子はオプションの FROM 'srcReplicas' 句をサポートしています。'srcReplicas'はカンマで区切られたソース レプリカ名のリストです。この拡張により、指定されたソース レプリカからのレプリケーション タスクのみに焦点を当てることで、より対象を絞った同期を実現できます。
- `PULL` 修飾子が指定された場合、クエリは ZooKeeper から新しいレプリケーション キュー エントリを取得しますが、何も処理待ちしません。

### SYNC DATABASE REPLICA

指定された[レプリケートデータベース](https://clickhouse.com/docs/ja/engines/database-engines/replicated) がそのデータベースの DDL キューからすべてのスキーマ変更を適用するまで待機します。

**構文**
```sql
SYSTEM SYNC DATABASE REPLICA replicated_database_name;
```

### RESTART REPLICA

`ReplicatedMergeTree` テーブルの ZooKeeper セッションの状態を再初期化し、Zookeeper をソースとした現在の状態を比較し、必要に応じて Zookeeper キューにタスクを追加する機能を提供します。
レプリケーション キューの ZooKeeper データに基づく初期化は、`ATTACH TABLE` ステートメントの場合と同様に行われます。短時間、テーブルがすべての操作に対して利用できなくなります。

``` sql
SYSTEM RESTART REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

### RESTORE REPLICA

データが [潜在的に] 存在するが Zookeeper メタデータが失われている場合に、レプリカを復元します。

読み取り専用 `ReplicatedMergeTree` テーブルでのみ機能します。

クエリは次の後に実行できます：

- ZooKeeper ルート `/` の損失。
- レプリカのパス `/replicas` の損失。
- 個々のレプリカのパス `/replicas/replica_name/` の損失。

レプリカはローカルで見つかったパーツをアタッチし、それらの情報を Zookeeper に送信します。
メタデータ損失前にレプリカに存在したパーツは、古くない限り（したがって、レプリカの復元はすべてのデータをネットワーク経由で再ダウンロードすることを意味しない）、他のレプリカから再フェッチされません。

:::note
すべての状態のパーツは `detached/` フォルダに移動されます。データ損失前にアクティブだったパーツ（コミットされた）はアタッチされます。
:::

**構文**

```sql
SYSTEM RESTORE REPLICA [db.]replicated_merge_tree_family_table_name [ON CLUSTER cluster_name]
```

代替構文：

```sql
SYSTEM RESTORE REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

**例**

複数のサーバーでテーブルを作成する。ZooKeeper 内のレプリカのメタデータが失われた後、メタデータがないため、テーブルは読み取り専用としてアタッチされます。最後のクエリはすべてのレプリカで実行する必要があります。

```sql
CREATE TABLE test(n UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/', '{replica}')
ORDER BY n PARTITION BY n % 10;

INSERT INTO test SELECT * FROM numbers(1000);

-- zookeeper_delete_path("/clickhouse/tables/test", recursive=True) <- root loss.

SYSTEM RESTART REPLICA test;
SYSTEM RESTORE REPLICA test;
```

別の方法：

```sql
SYSTEM RESTORE REPLICA test ON CLUSTER cluster;
```

### RESTART REPLICAS

すべての `ReplicatedMergeTree` テーブルの ZooKeeper セッションの状態を再初期化し、Zookeeper をソースとした現在の状態と比較して必要に応じて Zookeeper キューにタスクを追加する機能を提供します。

### DROP FILESYSTEM CACHE

ファイルシステム キャッシュを削除することが可能です。

```sql
SYSTEM DROP FILESYSTEM CACHE [ON CLUSTER cluster_name]
```

### SYNC FILE CACHE

:::note
それは非常に重いものであり、誤用の可能性があります。
:::

sync システムコールを実行します。

```sql
SYSTEM SYNC FILE CACHE [ON CLUSTER cluster_name]
```

### UNLOAD PRIMARY KEY

指定されたテーブルまたはすべてのテーブルの主キーをアンロードします。

```sql
SYSTEM UNLOAD PRIMARY KEY [db.]name
```

```sql
SYSTEM UNLOAD PRIMARY KEY
```

## リフレッシュ可能なマテリアライズドビューの管理 {#refreshable-materialized-views}

[リフレッシュ可能なマテリアライズドビュー](../../sql-reference/statements/create/view.md#refreshable-materialized-view) によって実行されるバックグラウンドタスクを制御するコマンド。

それらを使用する際は [`system.view_refreshes`](../../operations/system-tables/view_refreshes.md) を監視してください。

### REFRESH VIEW

指定されたビューのスケジュール外の即時更新をトリガーします。

```sql
SYSTEM REFRESH VIEW [db.]name
```

### REFRESH VIEW

現在実行中の更新が完了するのを待ちます。更新が失敗した場合は、例外をスローします。更新が実行中でない場合は直ちに完了し、前回の更新が失敗した場合は例外をスローします。

### STOP VIEW, STOP VIEWS

指定されたビューまたはすべてのリフレッシュ可能なビューの定期的な更新を無効にします。更新が進行中の場合は、それもキャンセルします。

```sql
SYSTEM STOP VIEW [db.]name
```
```sql
SYSTEM STOP VIEWS
```

### START VIEW, START VIEWS

指定されたビューまたはすべてのリフレッシュ可能なビューの定期的な更新を有効にします。即時更新はトリガーされません。

```sql
SYSTEM START VIEW [db.]name
```
```sql
SYSTEM START VIEWS
```

### CANCEL VIEW

指定されたビューの更新が進行中の場合、それを中断してキャンセルします。それ以外の場合は何もしません。

```sql
SYSTEM CANCEL VIEW [db.]name
```

### SYSTEM WAIT VIEW

実行中の更新が完了するのを待ちます。更新が実行中でない場合は直ちに戻ります。最新の更新試行が失敗した場合は、エラーを報告します。

新しいリフレッシュ可能なマテリアライズドビュー（EMPTYキーワードなし）を作成した直後に使用して、初回の更新が完了するのを待つことができます。

```sql
SYSTEM WAIT VIEW [db.]name
```

