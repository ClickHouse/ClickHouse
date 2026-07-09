---
description: 'SYSTEM文のドキュメント'
sidebar_label: 'SYSTEM'
sidebar_position: 36
slug: /sql-reference/statements/system
title: 'SYSTEM文'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="system-statements">
  # SYSTEM文
</div>

<div id="reload-embedded-dictionaries">
  ## SYSTEM RELOAD EMBEDDED DICTIONARIES
</div>

すべての[内部辞書](./create/dictionary/overview.md)を再読み込みします。
デフォルトでは、内部辞書は無効になっています。
内部辞書の更新結果にかかわらず、常に `Ok.` を返します。

<div id="reload-dictionaries">
  ## SYSTEM RELOAD DICTIONARIES
</div>

`SYSTEM RELOAD DICTIONARIES` クエリは、`LOADED` ステータスの Dictionary ([`system.dictionaries`](/ja/operations/system-tables/dictionaries) の `status` カラムを参照) 、つまり過去に正常に読み込まれた Dictionary を再読み込みします。
デフォルトでは、Dictionary は遅延読み込みされます ([dictionaries&#95;lazy&#95;load](../../operations/server-configuration-parameters/settings.md#dictionaries_lazy_load) を参照) 。そのため、起動時に自動で読み込まれるのではなく、[`dictGet`](/ja/sql-reference/functions/ext-dict-functions#dictGet) 関数を使用したとき、または `ENGINE = Dictionary` を持つテーブルに対して `SELECT` を実行したときに、初回アクセス時に初期化されます。

**構文**

```sql
SYSTEM RELOAD DICTIONARIES [ON CLUSTER cluster_name]
```

<div id="reload-dictionary">
  ## SYSTEM RELOAD DICTIONARY
</div>

Dictionary `dictionary_name` を、Dictionary の状態 (LOADED / NOT&#95;LOADED / FAILED) にかかわらず完全に再読み込みします。
Dictionary の更新結果に関係なく、常に `Ok.` を返します。

```sql
SYSTEM RELOAD DICTIONARY [ON CLUSTER cluster_name] dictionary_name
```

Dictionaryの状態は、`system.dictionaries` テーブルに対してクエリを実行することで確認できます。

```sql
SELECT name, status FROM system.dictionaries;
```

<div id="reload-models">
  ## SYSTEM RELOAD MODELS
</div>

:::note
このステートメントと `SYSTEM RELOAD MODEL` は、clickhouse-library-bridge から CatBoost モデルをアンロードするだけです。関数 `catboostEvaluate()`
は、まだモデルが読み込まれていない場合、最初にアクセスされたときにそのモデルを読み込みます。
:::

すべての CatBoost モデルをアンロードします。

**構文**

```sql
SYSTEM RELOAD MODELS [ON CLUSTER cluster_name]
```

<div id="reload-model">
  ## SYSTEM RELOAD MODEL
</div>

`model_path` の CatBoost モデルをアンロードします。

**構文**

```sql
SYSTEM RELOAD MODEL [ON CLUSTER cluster_name] <model_path>
```

<div id="reload-functions">
  ## SYSTEM RELOAD FUNCTIONS
</div>

登録されているすべての[実行可能ユーザー定義関数](/ja/sql-reference/functions/udf#executable-user-defined-functions)、またはそのうちの1つを設定ファイルから再読み込みします。

**構文**

```sql
SYSTEM RELOAD FUNCTIONS [ON CLUSTER cluster_name]
SYSTEM RELOAD FUNCTION [ON CLUSTER cluster_name] function_name
```

<div id="reload-asynchronous-metrics">
  ## SYSTEM RELOAD ASYNCHRONOUS METRICS
</div>

すべての[非同期メトリクス](../../operations/system-tables/asynchronous_metrics.md)を再計算します。非同期メトリクスは設定[asynchronous&#95;metrics&#95;update&#95;period&#95;s](../../operations/server-configuration-parameters/settings.md)に基づいて定期的に更新されるため、通常、このステートメントで手動更新する必要はありません。

```sql
SYSTEM RELOAD ASYNCHRONOUS METRICS [ON CLUSTER cluster_name]
```

<div id="drop-dns-cache">
  ## SYSTEM CLEAR|DROP DNS CACHE
</div>

ClickHouse の内部DNSキャッシュをクリアします。インフラストラクチャを変更する際 (別の ClickHouse server の IP アドレスや、Dictionaries で使用される server を変更する場合) には、古い ClickHouse バージョンではこのコマンドが必要になることがあります。

より便利な (自動) cache 管理については、`disable_internal_dns_cache`、`dns_cache_max_entries`、`dns_cache_update_period` パラメータを参照してください。

<div id="drop-mark-cache">
  ## SYSTEM CLEAR|DROP MARK CACHE
</div>

mark cacheをクリアします。

<div id="drop-primary-index-cache">
  ## SYSTEM CLEAR|DROP PRIMARY INDEX CACHE
</div>

プライマリインデックスキャッシュをクリアします。これは [`MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md) テーブルの主キーをメモリ内に保持するキャッシュです。
そのサイズは、サーバーレベルの設定 [`primary_index_cache_size`](../../operations/server-configuration-parameters/settings.md#primary_index_cache_size) で設定します。

<div id="drop-iceberg-metadata-cache">
  ## SYSTEM CLEAR|DROP ICEBERG METADATA CACHE
</div>

Iceberg のメタデータキャッシュをクリアします。

<div id="drop-avro-schema-cache">
  ## SYSTEM CLEAR|DROP AVRO SCHEMA CACHE
</div>

`AvroConfluent` フォーマットで使用される、URL ごとの Confluent スキーマレジストリの cache をクリアします。これにより、スキーマ取得 cache (id → スキーマ) とスキーマ登録 cache (subject + スキーマ → id) の両方が削除されるため、その後の読み取りと書き込みはレジストリサーバーにフォールバックします。これは、レジストリ側でスキーマが削除または書き換えられた場合や、テストでレジストリのべき等性を検証する場合に役立ちます。

<div id="drop-parquet-metadata-cache">
  ## SYSTEM DROP PARQUET METADATA CACHE
</div>

Parquet メタデータキャッシュを削除します。

<div id="drop-point-in-polygon-cache">
  ## SYSTEM CLEAR|DROP POINT IN POLYGON CACHE
</div>

[`pointInPolygon`](../functions/geo/coordinates.md#pointinpolygon) 関数で使用される、前処理済みの定数 Polygon の cache をクリアします。設定されているサイズ上限 (サーバー設定 `point_in_polygon_cache_size`) は変更されないため、その後も cache は引き続きエントリを受け付けます。代わりに cache を無効化するには、`point_in_polygon_cache_size` を `0` に設定してください。

<div id="drop-text-index-caches">
  ## SYSTEM CLEAR|DROP TEXT INDEX CACHES
</div>

テキスト索引のトークン、ヘッダー、およびポスティングリストのcacheをクリアします。

これらのcacheのいずれかを個別にクリアする場合は、次を実行できます。

* `SYSTEM CLEAR TEXT INDEX TOKENS CACHE`,
* `SYSTEM CLEAR TEXT INDEX HEADER CACHE`, または
* `SYSTEM CLEAR TEXT INDEX POSTINGS CACHE`

<div id="drop-index-mark-cache">
  ## SYSTEM CLEAR|DROP INDEX MARK CACHE
</div>

セカンダリ (データスキッピング) 索引の索引マークキャッシュをクリアします。

<div id="drop-index-uncompressed-cache">
  ## SYSTEM CLEAR|DROP INDEX UNCOMPRESSED CACHE
</div>

セカンダリ (データスキッピング) 索引の非圧縮ブロックのキャッシュをクリアします。

<div id="drop-mmap-cache">
  ## SYSTEM CLEAR|DROP MMAP CACHE
</div>

メモリマップドファイルのキャッシュをクリアします。

<div id="drop-page-cache">
  ## SYSTEM CLEAR|DROP PAGE CACHE
</div>

基盤ストレージから読み込んだデータを保持する、ClickHouse 独自のインメモリキャッシュであるユーザー空間のページキャッシュをクリアします。

<div id="drop-vector-similarity-index-cache">
  ## SYSTEM CLEAR|DROP VECTOR SIMILARITY INDEX CACHE
</div>

ベクトル類似度索引キャッシュをクリアします。

<div id="drop-connections-cache">
  ## SYSTEM CLEAR|DROP CONNECTIONS CACHE
</div>

外向き接続に使用されるHTTP接続プールのcacheをクリアします。

<div id="drop-s3-client-cache">
  ## SYSTEM CLEAR|DROP S3 CLIENT CACHE
</div>

S3 Clientのcacheをクリアします。

<div id="prewarm-mark-cache">
  ## SYSTEM PREWARM MARK CACHE
</div>

テーブルのマークを [mark cache](#drop-mark-cache) に読み込みます。セカンダリ索引のマークも [マークキャッシュ](#drop-index-mark-cache) に読み込まれます。

```sql
SYSTEM PREWARM MARK CACHE [ON CLUSTER cluster_name] [db.]table
```

<div id="prewarm-primary-index-cache">
  ## SYSTEM PREWARM PRIMARY INDEX CACHE
</div>

`MergeTree` テーブルのプライマリインデックスを[プライマリインデックスキャッシュ](#drop-primary-index-cache)へ読み込みます。

```sql
SYSTEM PREWARM PRIMARY INDEX CACHE [ON CLUSTER cluster_name] [db.]table
```

<div id="drop-disk-metadata-cache">
  ## SYSTEM CLEAR|DROP DISK メタデータキャッシュ
</div>

指定したディスクのメタデータキャッシュを消去します。

```sql
SYSTEM DROP DISK METADATA CACHE <disk_name>
```

<div id="sync-filesystem-cache">
  ## SYSTEM SYNC FILESYSTEM CACHE
</div>

ClickHouse のファイルシステムキャッシュのインメモリ状態を、実際にディスク上に存在するキャッシュファイルとリコンサイルし、キャッシュされた各ファイルセグメントの `cache_name`、`path`、およびダウンロード済みの `size` を返します。オプションで cache 名を指定すると、この操作を単一の cache に限定できます。

```sql
SYSTEM SYNC FILESYSTEM CACHE ['<cache_name>']
```

<div id="drop-distributed-cache">
  ## SYSTEM CLEAR|DROP DISTRIBUTED CACHE
</div>

:::note
`SYSTEM CLEAR|DROP DISTRIBUTED CACHE` は ClickHouse Cloud でのみ使用できます。
:::

分散キャッシュを削除します。分散キャッシュサーバーへのキャッシュ済み接続のみを削除するには `CONNECTIONS` を使用するか、特定の 1 台のサーバーを対象にするにはサーバー識別子を指定します。

```sql
SYSTEM DROP DISTRIBUTED CACHE [CONNECTIONS | 'server_id']
```

<div id="drop-replica">
  ## SYSTEM DROP REPLICA
</div>

`ReplicatedMergeTree` テーブルの利用不能になったレプリカは、次の構文で削除できます:

```sql
SYSTEM DROP REPLICA 'replica_name' FROM TABLE database.table;
SYSTEM DROP REPLICA 'replica_name' FROM DATABASE database;
SYSTEM DROP REPLICA 'replica_name';
SYSTEM DROP REPLICA 'replica_name' FROM ZKPATH '/path/to/table/in/zk';
```

これらのクエリは、ZooKeeper 内の `ReplicatedMergeTree` のレプリカパスを削除します。これは、レプリカが停止していて、すでに該当するテーブルが存在しないために `DROP TABLE` では ZooKeeper からそのメタデータを削除できない場合に有用です。削除できるのは非アクティブな古いレプリカのみで、ローカルレプリカは削除できません。その場合は `DROP TABLE` を使用してください。`DROP REPLICA` はテーブル自体を削除せず、ディスク上のデータやメタデータも削除しません。

1 つ目は、`database.table` テーブルの `'replica_name'` レプリカのメタデータを削除します。
2 つ目は、データベース内のすべてのレプリケートテーブルに対して同じ処理を行います。
3 つ目は、ローカルサーバー上のすべてのレプリケートテーブルに対して同じ処理を行います。
4 つ目は、テーブルの他のすべてのレプリカが削除されている場合に、停止したレプリカのメタデータを削除するのに役立ちます。この場合、テーブルのパスを明示的に指定する必要があります。このパスは、テーブル作成時に `ReplicatedMergeTree` engine の第 1 引数として渡したものと同じでなければなりません。

<div id="drop-database-replica">
  ## SYSTEM DROP DATABASE REPLICA
</div>

`Replicated` データベースの停止したレプリカは、次の構文で削除できます。

```sql
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM DATABASE database;
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'];
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM ZKPATH '/path/to/table/in/zk';
```

`SYSTEM DROP REPLICA` と似ていますが、`DROP DATABASE` を実行する対象のデータベースが存在しない場合に、ZooKeeper から `Replicated` データベースのレプリカパスを削除します。なお、`ReplicatedMergeTree` のレプリカは削除されないため、`SYSTEM DROP REPLICA` も必要になる場合があります。分片名とレプリカ名は、データベース作成時に `Replicated` エンジンの引数で指定した名前です。また、これらの名前は `system.clusters` の `database_shard_name` カラムおよび `database_replica_name` カラムから取得することもできます。`FROM SHARD` 句がない場合、`replica_name` は `shard_name|replica_name` フォーマットの完全なレプリカ名でなければなりません。

<div id="drop-uncompressed-cache">
  ## SYSTEM CLEAR|DROP UNCOMPRESSED CACHE
</div>

非圧縮データキャッシュをクリアします。
非圧縮データキャッシュは、クエリ/USER/profile レベルの設定 [`use_uncompressed_cache`](../../operations/settings/settings.md#use_uncompressed_cache) で有効/無効を切り替えられます。
そのサイズは、server レベルの設定 [`uncompressed_cache_size`](../../operations/server-configuration-parameters/settings.md#uncompressed_cache_size) で設定できます。

<div id="drop-compiled-expression-cache">
  ## SYSTEM CLEAR|DROP COMPILED EXPRESSION CACHE
</div>

コンパイル済み式の cache をクリアします。
コンパイル済み式の cache は、クエリ/ユーザー/プロファイル レベルの設定 [`compile_expressions`](../../operations/settings/settings.md#compile_expressions) で有効/無効を切り替えられます。

<div id="drop-query-condition-cache">
  ## SYSTEM CLEAR|DROP QUERY CONDITION CACHE
</div>

query condition cache をクリアします。

<div id="drop-query-cache">
  ## SYSTEM CLEAR|DROP QUERY CACHE
</div>

```sql
SYSTEM CLEAR QUERY CACHE;
SYSTEM CLEAR QUERY CACHE TAG '<tag>'
```

[クエリキャッシュ](../../operations/query-cache.md)をクリアします。
タグを指定した場合は、そのタグが付いたクエリキャッシュのエントリのみが削除されます。

<div id="system-drop-schema-format">
  ## SYSTEM CLEAR|DROP FORMAT SCHEMA CACHE
</div>

[`format_schema_path`](../../operations/server-configuration-parameters/settings.md#format_schema_path) から読み込まれたスキーマのキャッシュをクリアします。

サポートされる対象:

* Protobuf: インポートされた Protobuf メッセージ定義をメモリから削除します。
* Files: [`format_schema_path`](../../operations/server-configuration-parameters/settings.md#format_schema_path) にローカルに保存された、キャッシュ済みのスキーマファイルを削除します。これらは `format_schema_source` が `query` に設定されている場合に生成されます。
  注: 対象が指定されていない場合は、両方のキャッシュがクリアされます。

```sql
SYSTEM CLEAR|DROP FORMAT SCHEMA CACHE [FOR Protobuf/Files]
```

<div id="flush-logs">
  ## SYSTEM FLUSH LOGS
</div>

バッファされたログメッセージをシステムテーブル (例: system.query&#95;log) にフラッシュします。ほとんどのシステムテーブルのデフォルトのフラッシュ間隔は 7.5 秒であるため、主にデバッグ時に有用です。
メッセージキューが空でも、これによりシステムテーブルが作成されます。

```sql
SYSTEM FLUSH LOGS [ON CLUSTER cluster_name] [log_name|[database.table]] [, ...]
```

すべてをフラッシュしたくない場合は、名前またはターゲットテーブルを指定して、個別のログを1つ以上フラッシュできます:

```sql
SYSTEM FLUSH LOGS query_log, system.query_views_log;
```

<div id="reload-config">
  ## SYSTEM RELOAD CONFIG
</div>

ClickHouse の設定を再読み込みします。設定が ZooKeeper に保存されている場合に使用します。`SYSTEM RELOAD CONFIG` では、ZooKeeper に保存された `USER` 設定は再読み込みされず、`users.xml` に保存されている `USER` 設定のみが再読み込みされる点に注意してください。すべての `USER` 設定を再読み込みするには、`SYSTEM RELOAD USERS` を使用します。

```sql
SYSTEM RELOAD CONFIG [ON CLUSTER cluster_name]
```

<div id="reload-users">
  ## SYSTEM RELOAD USERS
</div>

users.xml、ローカルディスクのアクセスストレージ、レプリケートされた (ZooKeeper 内の) アクセスストレージなど、すべてのアクセスストレージを再読み込みします。

```sql
SYSTEM RELOAD USERS [ON CLUSTER cluster_name]
```

<div id="shutdown">
  ## SYSTEM SHUTDOWN
</div>

<CloudNotSupportedBadge />

通常は ClickHouse を停止します (`service clickhouse-server stop` / `kill {$pid_clickhouse-server}` と同様)

<div id="kill">
  ## SYSTEM KILL
</div>

ClickHouse プロセスを強制終了します (`kill -9 {$ pid_clickhouse-server}` など) 。

<div id="instrument">
  ## SYSTEM INSTRUMENT
</div>

`ENABLE_XRAY=1` を指定して ClickHouse をビルドした場合に利用できる、LLVM の XRay 機能を使用してインストルメンテーションポイントを管理します。
これにより、ソースコードを変更することなく、最小限のオーバーヘッドで本番環境でのデバッグやプロファイリングが可能になります。
インストルメンテーションポイントが追加されていない場合、パフォーマンスへの影響はごくわずかです。これは、200 命令を超える関数のプロローグとエピローグに、近くの
アドレスへの追加のジャンプが 1 つ加わるだけだからです。

<div id="instrument-add">
  ### SYSTEM INSTRUMENT ADD
</div>

新しいインストルメンテーションポイントを追加します。インストルメントされた関数は、[`system.instrumentation`](../../operations/system-tables/instrumentation.md) システムテーブルで確認できます。同じ関数に複数のハンドラーを追加でき、それらはインストルメンテーションが追加された順序で実行されます。
インストルメントする関数は、[`system.symbols`](../../operations/system-tables/symbols.md) システムテーブルから取得できます。

関数に追加できるハンドラーには、3 つの種類があります。

**構文**

```sql
SYSTEM INSTRUMENT ADD FUNCTION HANDLER [ARGUMENTS]
```

ここで、`FUNCTION` は `QueryMetricLog::startQuery` のような任意の関数、または関数名の一部を指し、ハンドラーは次のいずれかです

<div id="instrument-add-log">
  #### LOG
</div>

引数として指定したテキストとスタックトレースを、関数の `ENTRY` または `EXIT` 時に出力します。

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG ENTRY 'this is a log printed at entry'
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG EXIT 'this is a log printed at exit'
```

<div id="instrument-add-sleep">
  #### SLEEP
</div>

`ENTRY` または `EXIT` のいずれかで、一定の秒数だけスリープします:

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0.5
```

または、空白で区切った最小値と最大値を指定して、一様分布に従うランダムな秒数を指定します:

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0 1
```

<div id="instrument-add-profile">
  #### PROFILE
</div>

関数の `ENTRY` から `EXIT` までの間に費やされた時間を測定します。
プロファイリングの結果は [`system.trace_log`](../../operations/system-tables/trace_log.md) に保存され、
[Chrome Event Trace Format](../../operations/system-tables/trace_log.md#chrome-event-trace-format) に変換できます。

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' PROFILE
```

<div id="instrument-remove">
  ### SYSTEM INSTRUMENT REMOVE
</div>

次のように、単一のインストルメンテーションポイントを削除します:

```sql
SYSTEM INSTRUMENT REMOVE ID
```

`ALL` キーワードを使用して、すべてを指定できます:

```sql
SYSTEM INSTRUMENT REMOVE ALL
```

サブクエリから取得したIDの集合:

```sql
SYSTEM INSTRUMENT REMOVE (SELECT id FROM system.instrumentation WHERE handler = 'log')
```

または、指定したfunction&#95;nameに一致するすべてのインストルメンテーションポイント:

```sql
SYSTEM INSTRUMENT REMOVE 'QueryMetricLog::startQuery'
```

インストルメンテーションポイントの情報は、[`system.instrumentation`](../../operations/system-tables/instrumentation.md) システムテーブルから取得できます。

<div id="managing-distributed-tables">
  ## 分散テーブルの管理
</div>

ClickHouse では、[distributed](../../engines/table-engines/special/distributed.md) テーブルを管理できます。ユーザーがこれらのテーブルにデータを挿入すると、ClickHouse はまずクラスターのノードに送信するデータのキューを作成し、その後、それを非同期に送信します。[`STOP DISTRIBUTED SENDS`](#stop-distributed-sends)、[FLUSH DISTRIBUTED](#flush-distributed)、および [`START DISTRIBUTED SENDS`](#start-distributed-sends) クエリを使用すると、このキューの処理を管理できます。また、[`distributed_foreground_insert`](../../operations/settings/settings.md#distributed_foreground_insert) 設定を使用して、分散テーブルにデータを同期的に挿入することもできます。

<div id="stop-distributed-sends">
  ### SYSTEM STOP DISTRIBUTED SENDS
</div>

分散テーブルにデータを挿入する際に、バックグラウンドで行われるデータ分散を無効にします。

```sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

:::note
[`prefer_localhost_replica`](../../operations/settings/settings.md#prefer_localhost_replica) が有効 (デフォルト) の場合でも、データはローカル分片にそのまま挿入されます。
:::

<div id="flush-distributed">
  ### SYSTEM FLUSH DISTRIBUTED
</div>

ClickHouse がデータをクラスターのノードに同期的に送信するよう強制します。いずれかのノードが利用できない場合、ClickHouse は例外を発生させてクエリ実行を停止します。クエリは成功するまで再試行できます。すべてのノードが再びオンラインになれば成功します。

`SETTINGS` 句を使って一部の設定をオーバーライドすることもできます。これは、`max_concurrent_queries_for_all_users` や `max_memory_usage` のような一時的な制限を回避するのに役立つ場合があります。

```sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name> [ON CLUSTER cluster_name] [SETTINGS ...]
```

:::note
保留中の各ブロックは、最初の INSERT クエリの設定とともにディスクに保存されるため、場合によっては設定を上書きしたいことがあります。
:::

<div id="start-distributed-sends">
  ### SYSTEM START DISTRIBUTED SENDS
</div>

分散テーブルへのデータ挿入時に行われる、バックグラウンドでのデータ分散を有効にします。

```sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

<div id="stop-listen">
  ### SYSTEM STOP LISTEN
</div>

ソケットを閉じ、指定したプロトコルの指定したポートに対する既存のサーバー接続を正常に終了します。

ただし、対応するプロトコル設定が clickhouse-server の設定で指定されていない場合、このコマンドは効果を発揮しません。

```sql
SYSTEM STOP LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

* `CUSTOM 'protocol'` 修飾子が指定されている場合、サーバー設定の protocols セクションで定義されている、指定した名前のカスタムプロトコルが停止されます。
* `QUERIES ALL [EXCEPT .. [,..]]` 修飾子が指定されている場合、`EXCEPT` 句で指定されたものを除き、すべてのプロトコルが停止されます。
* `QUERIES DEFAULT [EXCEPT .. [,..]]` 修飾子が指定されている場合、`EXCEPT` 句で指定されたものを除き、すべてのデフォルトプロトコルが停止されます。
* `QUERIES CUSTOM [EXCEPT .. [,..]]` 修飾子が指定されている場合、`EXCEPT` 句で指定されたものを除き、すべてのカスタムプロトコルが停止されます。

<div id="start-listen">
  ### SYSTEM START LISTEN
</div>

指定したプロトコルで新しい接続を確立できるようにします。

ただし、指定したポートとプロトコルのサーバーが SYSTEM STOP LISTEN コマンドで停止されていない場合、このコマンドは何の効果もありません。

```sql
SYSTEM START LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

<div id="managing-mergetree-tables">
  ## MergeTree テーブルの管理
</div>

ClickHouse では、[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) テーブルのバックグラウンド処理を管理できます。

<div id="stop-merges">
  ### SYSTEM STOP MERGES
</div>

<CloudNotSupportedBadge />

MergeTree family のテーブルで、バックグラウンドマージを停止できます。

```sql
SYSTEM STOP MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

:::note
テーブルに `DETACH / ATTACH` を実行すると、たとえそれ以前にすべての MergeTree テーブルでマージが停止されていても、そのテーブルのバックグラウンドマージが開始されます。
:::

<div id="start-merges">
  ### SYSTEM START MERGES
</div>

<CloudNotSupportedBadge />

MergeTree family のテーブルでバックグラウンドマージを開始できます。

```sql
SYSTEM START MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

<div id="stop-ttl-merges">
  ### SYSTEM STOP TTL MERGES
</div>

<CloudNotSupportedBadge />

MergeTree family のテーブルに対して、[有効期限 (TTL) 式](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl)に基づく古いデータのバックグラウンド削除を停止できます。
テーブルが存在しない場合や、テーブルで MergeTree エンジンが使用されていない場合でも `Ok.` を返します。データベースが存在しない場合はエラーを返します。

```sql
SYSTEM STOP TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="start-ttl-merges">
  ### SYSTEM START TTL MERGES
</div>

<CloudNotSupportedBadge />

MergeTree family のテーブルに対して、[有効期限 (TTL) 式](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) に従って古いデータをバックグラウンドで削除する処理を開始できます。
テーブルが存在しない場合でも `Ok.` を返します。データベースが存在しない場合はエラーを返します:

```sql
SYSTEM START TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="stop-moves">
  ### SYSTEM STOP MOVES
</div>

MergeTree family のテーブルに対して、[TO VOLUME または TO DISK 句を含む TTL テーブル式](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl)に基づくバックグラウンドでのデータ移動を停止できます。
テーブルが存在しない場合でも `Ok.` を返します。データベースが存在しない場合はエラーを返します。

```sql
SYSTEM STOP MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="start-moves">
  ### SYSTEM START MOVES
</div>

[TO VOLUME 句および TO DISK 句を含む TTL テーブル式](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl)に従って、MergeTree family のテーブルに対するバックグラウンドでのデータ移動を開始できます。
テーブルが存在しない場合でも `Ok.` を返します。データベースが存在しない場合はエラーを返します。

```sql
SYSTEM START MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="query_language-system-unfreeze">
  ### SYSTEM UNFREEZE
</div>

指定した名前の凍結バックアップを、すべてのディスクから削除します。個別のパーツの凍結解除について詳しくは、[ALTER TABLE table&#95;name UNFREEZE WITH NAME ](/ja/sql-reference/statements/alter/partition#unfreeze-partition)を参照してください。

```sql
SYSTEM UNFREEZE WITH NAME <backup_name>
```

<div id="wait-loading-parts">
  ### SYSTEM WAIT LOADING PARTS
</div>

テーブル内の非同期で読み込まれるすべてのデータパーツ (古いデータパーツ) が読み込まれるまで待機します。

```sql
SYSTEM WAIT LOADING PARTS [ON CLUSTER cluster_name] [db.]merge_tree_family_table_name
```

<div id="managing-replicatedmergetree-tables">
  ## ReplicatedMergeTree テーブルの管理
</div>

ClickHouse では、[ReplicatedMergeTree](/ja/engines/table-engines/mergetree-family/replication) テーブルに関連するバックグラウンドのレプリケーション処理を管理できます。

<div id="stop-fetches">
  ### SYSTEM STOP FETCHES
</div>

<CloudNotSupportedBadge />

`ReplicatedMergeTree` ファミリーのテーブルで、挿入されたパーツのバックグラウンドフェッチを停止できます。
テーブルエンジンに関係なく、またテーブルやデータベースが存在しない場合でも、常に `Ok.` を返します。

```sql
SYSTEM STOP FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-fetches">
  ### SYSTEM START FETCHES
</div>

<CloudNotSupportedBadge />

`ReplicatedMergeTree` ファミリーのテーブルに対して、挿入されたパーツのバックグラウンドフェッチを開始できます。
テーブルエンジンに関係なく、またテーブルやデータベースが存在しない場合でも、常に `Ok.` を返します。

```sql
SYSTEM START FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-replicated-sends">
  ### SYSTEM STOP REPLICATED SENDS
</div>

`ReplicatedMergeTree` ファミリーのテーブルでは、新たに挿入されたパーツをクラスター内の他のレプリカに送信するバックグラウンド処理を停止できます。

```sql
SYSTEM STOP REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-replicated-sends">
  ### SYSTEM START REPLICATED SENDS
</div>

`ReplicatedMergeTree` ファミリーのテーブルで、新たに挿入されたパーツをクラスター内の他のレプリカへ送信するバックグラウンド処理を開始できます。

```sql
SYSTEM START REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-replication-queues">
  ### SYSTEM STOP REPLICATION QUEUES
</div>

`ReplicatedMergeTree` ファミリーのテーブルに対して、Zookeeper に保存されているレプリケーションキュー内のバックグラウンド fetch タスクを停止できます。対象となるバックグラウンドタスクの種類は、マージ、fetch、ミューテーション、ON CLUSTER 句を含む DDL ステートメントです。

```sql
SYSTEM STOP REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-replication-queues">
  ### SYSTEM START REPLICATION QUEUES
</div>

`ReplicatedMergeTree` ファミリーのテーブルについて、ZooKeeper に保存されているレプリケーションキューから、バックグラウンドのフェッチタスクを開始できます。実行可能なバックグラウンドタスクの種類は、マージ、フェッチ、ミューテーション、ON CLUSTER 句を含む DDL ステートメントです。

```sql
SYSTEM START REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-pulling-replication-log">
  ### SYSTEM STOP PULLING REPLICATION LOG
</div>

`ReplicatedMergeTree` テーブルで、新しいエントリをレプリケーションログからレプリケーションキューに読み込む処理を停止します。

```sql
SYSTEM STOP PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-pulling-replication-log">
  ### SYSTEM START PULLING REPLICATION LOG
</div>

`SYSTEM STOP PULLING REPLICATION LOG` を解除します。

```sql
SYSTEM START PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="sync-replica">
  ### SYSTEM SYNC REPLICA
</div>

`ReplicatedMergeTree` テーブルがクラスター内の他のレプリカと同期されるまで待機します。ただし、`receive_timeout` Seconds を超えて待機することはありません。

```sql
SYSTEM SYNC REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name [IF EXISTS] [STRICT | LIGHTWEIGHT [FROM 'srcReplica1'[, 'srcReplica2'[, ...]]] | PULL]
```

このステートメントを実行すると、`[db.]replicated_merge_tree_family_table_name` は共通のレプリケートログからコマンドを自身のレプリケーションキューに取り込み、その後クエリはレプリカが取り込まれたすべてのコマンドを処理し終えるまで待機します。以下の修飾子がサポートされています。

* `IF EXISTS` を指定すると (25.6 以降で利用可能) 、テーブルが存在しない場合でもクエリはエラーを発生させません。これは、新しいレプリカをクラスターに追加する際に便利です。すでにクラスター構成には含まれているものの、テーブルの作成と同期がまだ進行中である場合に役立ちます。
* `STRICT` 修飾子を指定した場合、クエリはレプリケーションキューが空になるまで待機します。レプリケーションキューに新しいエントリが絶えず追加される場合、`STRICT` 版はいつまでも成功しない可能性があります。
* `LIGHTWEIGHT` 修飾子を指定した場合、クエリは `GET_PART`、`ATTACH_PART`、`DROP_RANGE`、`REPLACE_RANGE`、`DROP_PART` の各エントリが処理されるまでのみ待機します。
  さらに、LIGHTWEIGHT 修飾子はオプションの FROM &#39;srcReplicas&#39; 句をサポートしており、ここで &#39;srcReplicas&#39; はソースレプリカ名のカンマ区切りリストです。この拡張機能により、指定したソースレプリカに由来するレプリケーションタスクだけに対象を絞って、より限定的な同期を行えます。
* `PULL` 修飾子を指定した場合、クエリは ZooKeeper から新しいレプリケーションキューのエントリを取得しますが、何かが処理されるのは待機しません。

<div id="sync-database-replica">
  ### SYNC DATABASE REPLICA
</div>

指定した[Replicated データベース](/ja/engines/database-engines/replicated)が、そのデータベースのDDLキューにあるすべてのスキーマ変更を適用し終えるまで待機します。

**構文**

```sql
SYSTEM SYNC DATABASE REPLICA replicated_database_name;
```

<div id="restart-replica">
  ### SYSTEM RESTART REPLICA
</div>

`ReplicatedMergeTree` テーブルの ZooKeeper セッションの状態を再初期化します。現在の状態を正とする ZooKeeper の状態と比較し、必要に応じて ZooKeeper のキューにタスクを追加します。
ZooKeeper のデータに基づくレプリケーションキューの初期化は、`ATTACH TABLE` ステートメントの場合と同じ方法で行われます。短時間のあいだ、このテーブルは一切の操作を実行できなくなります。

```sql
SYSTEM RESTART REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

<div id="restore-replica">
  ### SYSTEM RESTORE REPLICA
</div>

データは[存在する可能性がある]ものの、ZooKeeper のメタデータが失われた場合にレプリカを復元します。

読み取り専用の `ReplicatedMergeTree` テーブルでのみ動作します。

次の状況の後にクエリを実行できます。

* ZooKeeper ルート `/` の消失。
* レプリカパス `/replicas` の消失。
* 個別のレプリカパス `/replicas/replica_name/` の消失。

レプリカはローカルで見つかったパーツをアタッチし、それらに関する情報を ZooKeeper に送信します。
メタデータ消失前にレプリカ上に存在していたパーツは、outdated でない限り他のレプリカから再取得されません (つまり、レプリカの復元はネットワーク経由ですべてのデータを再ダウンロードすることを意味しません) 。

:::note
すべての状態のパーツは `detached/` フォルダに移動されます。データ消失前にアクティブだったパーツ (committed) はアタッチされます。
:::

<div id="restore-database-replica">
  ### SYSTEM RESTORE DATABASE REPLICA
</div>

データは[存在している可能性がある]ものの、Zookeeperのメタデータが失われた場合にレプリカを復元します。

**構文**

```sql
SYSTEM RESTORE DATABASE REPLICA repl_db [ON CLUSTER cluster]
```

**例**

```sql
CREATE DATABASE repl_db
ENGINE=Replicated("/clickhouse/repl_db", shard1, replica1);

CREATE TABLE repl_db.test_table (n UInt32)
ENGINE = ReplicatedMergeTree
ORDER BY n PARTITION BY n % 10;

-- zookeeper_delete_path("/clickhouse/repl_db", recursive=True) <- root loss.

SYSTEM RESTORE DATABASE REPLICA repl_db;
```

**構文**

```sql
SYSTEM RESTORE REPLICA [db.]replicated_merge_tree_family_table_name [ON CLUSTER cluster_name]
```

代替構文:

```sql
SYSTEM RESTORE REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

**例**

複数のサーバー上にテーブルを作成します。ZooKeeper 内のレプリカのメタデータが失われると、メタデータが欠落しているため、そのテーブルは読み取り専用でアタッチされます。最後のクエリは、すべてのレプリカで実行する必要があります。

```sql
CREATE TABLE test(n UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/', '{replica}')
ORDER BY n PARTITION BY n % 10;

INSERT INTO test SELECT * FROM numbers(1000);

-- zookeeper_delete_path("/clickhouse/tables/test", recursive=True) <- root loss.

SYSTEM RESTART REPLICA test;
SYSTEM RESTORE REPLICA test;
```

もう1つの方法:

```sql
SYSTEM RESTORE REPLICA test ON CLUSTER cluster;
```

<div id="restart-replicas">
  ### SYSTEM RESTART REPLICAS
</div>

すべての `ReplicatedMergeTree` テーブルについて、ZooKeeper のセッション状態を再初期化できます。現在の状態を正とする情報源である ZooKeeper と比較し、必要に応じて ZooKeeper のキューにタスクを追加します

<div id="drop-filesystem-cache">
  ### SYSTEM CLEAR|DROP FILESYSTEM CACHE
</div>

ファイルシステムキャッシュをクリアできます。

```sql
SYSTEM CLEAR FILESYSTEM CACHE [ON CLUSTER cluster_name]
```

<div id="sync-file-cache">
  ### SYSTEM SYNC FILE CACHE
</div>

:::note
負荷が高く、誤用されるおそれがあります。
:::

`sync` システムコールを実行します。

```sql
SYSTEM SYNC FILE CACHE [ON CLUSTER cluster_name]
```

<div id="load-primary-key">
  ### SYSTEM LOAD PRIMARY KEY
</div>

指定したテーブル、またはすべてのテーブルの主キーをロードします。

```sql
SYSTEM LOAD PRIMARY KEY [db.]name
```

```sql
SYSTEM LOAD PRIMARY KEY
```

<div id="unload-primary-key">
  ### SYSTEM UNLOAD PRIMARY KEY
</div>

指定したテーブルまたはすべてのテーブルの主キーをアンロードします。

```sql
SYSTEM UNLOAD PRIMARY KEY [db.]name
```

```sql
SYSTEM UNLOAD PRIMARY KEY
```

<div id="managing-refreshable-materialized-views">
  ## リフレッシュ可能なマテリアライズドビューの管理
</div>

[リフレッシュ可能なマテリアライズドビュー](../../sql-reference/statements/create/view.md#refreshable-materialized-view)が実行するバックグラウンドタスクを制御するコマンド

これらのコマンドの使用中は、[`system.view_refreshes`](../../operations/system-tables/view_refreshes.md)を確認してください。

<div id="stop-view-stop-views">
  ### SYSTEM STOP [REPLICATED] VIEW, STOP VIEWS
</div>

指定したビュー、またはすべてのリフレッシュ可能なビューの定期的なリフレッシュを無効にします。リフレッシュが進行中の場合は、それもキャンセルします。

ビューが Replicated または Shared データベースにある場合、`STOP VIEW` は現在のレプリカにのみ影響し、`STOP REPLICATED VIEW` はすべてのレプリカに影響します。

:::note
停止状態はサーバーの再起動後も保持されません。再起動すると、ビューは設定されたリフレッシュスケジュールに従って再開されます。
Replicated または Shared データベースでは、`SYSTEM STOP VIEW` は現在のレプリカにのみ影響します。すべてのレプリカでリフレッシュを停止するには、`SYSTEM STOP REPLICATED VIEW` を使用してください。
:::

```sql
SYSTEM STOP VIEW [db.]name
```

```sql
SYSTEM STOP VIEWS
```

<div id="start-view-start-views">
  ### SYSTEM START [REPLICATED] VIEW, START VIEWS
</div>

指定したビュー、またはすべてのリフレッシュ可能なビューの定期的なリフレッシュを有効にします。即時にリフレッシュがトリガーされることはありません。

ビューが Replicated または Shared データベース内にある場合、`START VIEW` は `STOP VIEW` の効果を解除し、`START REPLICATED VIEW` は `STOP REPLICATED VIEW` の効果を解除します。`START VIEW` は `PAUSE VIEW` の効果も解除します。

```sql
SYSTEM START VIEW [db.]name
```

```sql
SYSTEM START VIEWS
```

<div id="pause-view-pause-views">
  ### SYSTEM PAUSE VIEW, PAUSE VIEWS
</div>

指定したビュー、またはすべてのリフレッシュ可能なビューの定期リフレッシュを無効にします。
`SYSTEM STOP VIEW` とは異なり、`SYSTEM PAUSE VIEW` はすでに進行中のリフレッシュを中断しません。実行中のリフレッシュは完了まで継続され、以降のリフレッシュのみが停止されます。

`SYSTEM START VIEW` または `SYSTEM START VIEWS` で元に戻せます。

:::note
一時停止状態はサーバーの再起動後も保持されません。再起動すると、ビューは設定されたリフレッシュスケジュールを再開します。
Replicated または Shared データベースでは、`SYSTEM PAUSE VIEW` は現在のレプリカにのみ影響します。
:::

```sql
SYSTEM PAUSE VIEW [db.]name
```

```sql
SYSTEM PAUSE VIEWS
```

<div id="refresh-view">
  ### SYSTEM REFRESH VIEW
</div>

指定したビューのスケジュール外リフレッシュを即時にトリガーします。

```sql
SYSTEM REFRESH VIEW [db.]name
```

<div id="wait-view">
  ### SYSTEM WAIT VIEW
</div>

実行中のリフレッシュが完了するまで待機します。リフレッシュが実行されていない場合は、ただちに戻ります。直近のリフレッシュの試行が失敗した場合は、エラーを返します。

新しいリフレッシャブルmaterialized view を作成した直後に (EMPTY キーワードなしで) 使用すると、初回のリフレッシュが完了するまで待機できます。

ビューが Replicated または Shared データベース内にあり、別のレプリカでリフレッシュが実行されている場合は、そのリフレッシュが完了するまで待機します。

```sql
SYSTEM WAIT VIEW [db.]name
```

<div id="cancel-view">
  ### SYSTEM CANCEL VIEW
</div>

現在のレプリカで指定されたビューのリフレッシュが進行中であれば、それを中断してキャンセルします。進行中でない場合は何もしません。

```sql
SYSTEM CANCEL VIEW [db.]name
```

<div id="flush-object-storage-queue">
  ## SYSTEM FLUSH OBJECT STORAGE QUEUE
</div>

指定した [S3Queue](../../engines/table-engines/integrations/s3queue.md) または [AzureQueue](../../engines/table-engines/integrations/azure-queue.md) テーブルによって、指定したファイルが処理されるか、永久的に Failed になるまで待機します。ファイルがすでに処理済みの場合は、直ちに返ります。ファイルが永久的に Failed している場合 (すべての再試行を使い果たした場合) は、エラーを発生させます。

```sql
SYSTEM FLUSH OBJECT STORAGE QUEUE [db.]table_name PATH 'path'
```