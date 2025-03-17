---
slug: /ja/operations/system-tables/metrics
---
# metrics

即時に計算できる、または現在の値を持つメトリクスを含んでいます。例えば、同時に処理されているクエリの数や現在のレプリカ遅延があります。このテーブルは常に最新です。

カラム:

- `metric` ([String](../../sql-reference/data-types/string.md)) — メトリクス名。
- `value` ([Int64](../../sql-reference/data-types/int-uint.md)) — メトリクスの値。
- `description` ([String](../../sql-reference/data-types/string.md)) — メトリクスの説明。
- `name` ([String](../../sql-reference/data-types/string.md)) — `metric`の別名。

サポートされているすべてのメトリクスは、ソースファイル [src/Common/CurrentMetrics.cpp](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/CurrentMetrics.cpp) で確認できます。

**例**

``` sql
SELECT * FROM system.metrics LIMIT 10
```

``` text
┌─metric───────────────────────────────┬─value─┬─description────────────────────────────────────────────────────────────┐
│ Query                                │     1 │ Number of executing queries                                            │
│ Merge                                │     0 │ Number of executing background merges                                  │
│ PartMutation                         │     0 │ Number of mutations (ALTER DELETE/UPDATE)                              │
│ ReplicatedFetch                      │     0 │ Number of data parts being fetched from replicas                       │
│ ReplicatedSend                       │     0 │ Number of data parts being sent to replicas                            │
│ ReplicatedChecks                     │     0 │ Number of data parts checking for consistency                          │
│ BackgroundMergesAndMutationsPoolTask │     0 │ Number of active merges and mutations in an associated background pool │
│ BackgroundFetchesPoolTask            │     0 │ Number of active fetches in an associated background pool              │
│ BackgroundCommonPoolTask             │     0 │ Number of active tasks in an associated background pool                │
│ BackgroundMovePoolTask               │     0 │ Number of active tasks in BackgroundProcessingPool for moves           │
└──────────────────────────────────────┴───────┴────────────────────────────────────────────────────────────────────────┘
```

## メトリクスの説明

### AggregatorThreads

Aggregatorスレッドプール内のスレッドの数。

### AggregatorThreadsActive

Aggregatorスレッドプールでタスクを実行しているスレッドの数。

### TablesLoaderForegroundThreads

非同期ローダー前景スレッドプール内のスレッドの数。

### TablesLoaderForegroundThreadsActive

非同期ローダー前景スレッドプールでタスクを実行しているスレッドの数。

### TablesLoaderBackgroundThreads

非同期ローダー背景スレッドプール内のスレッドの数。

### TablesLoaderBackgroundThreadsActive

非同期ローダー背景スレッドプールでタスクを実行しているスレッドの数。

### AsyncInsertCacheSize

キャッシュ内の非同期挿入ハッシュIDの数。

### AsynchronousInsertThreads

非同期挿入スレッドプール内のスレッドの数。

### AsynchronousInsertThreadsActive

非同期挿入スレッドプールでタスクを実行しているスレッドの数。

### AsynchronousReadWait

非同期読み込みを待機しているスレッドの数。

### BackgroundBufferFlushSchedulePoolSize

BackgroundBufferFlushSchedulePoolでのタスク数の制限。

### BackgroundBufferFlushSchedulePoolTask

BackgroundBufferFlushSchedulePoolでのアクティブなタスクの数。このプールは一定周期でのBufferフラッシュに使用されます。

### BackgroundCommonPoolSize

関連する背景プールでのタスク数の制限。

### BackgroundCommonPoolTask

関連する背景プールでのアクティブなタスクの数。

### BackgroundDistributedSchedulePoolSize

BackgroundDistributedSchedulePoolでのタスク数の制限。

### BackgroundDistributedSchedulePoolTask

BackgroundDistributedSchedulePoolでのアクティブなタスクの数。このプールは分散送信を背景で実行するために使用されます。

### BackgroundFetchesPoolSize

関連する背景プールでの同時フェッチ数の制限。

### BackgroundFetchesPoolTask

関連する背景プールでのアクティブなフェッチ数。

### BackgroundMergesAndMutationsPoolSize

関連する背景プールでのアクティブなマージとミューテーションの制限。

### BackgroundMergesAndMutationsPoolTask

関連する背景プールでのアクティブなマージとミューテーションの数。

### BackgroundMessageBrokerSchedulePoolSize

メッセージストリーミングのためのBackgroundProcessingPoolでのタスク数の制限。

### BackgroundMessageBrokerSchedulePoolTask

メッセージストリーミングのためのBackgroundProcessingPoolでのアクティブなタスクの数。

### BackgroundMovePoolSize

移動のためのBackgroundProcessingPoolでのタスク数の制限。

### BackgroundMovePoolTask

移動のためのBackgroundProcessingPoolでのアクティブなタスクの数。

### BackgroundSchedulePoolSize

BackgroundSchedulePoolでのタスク数の制限。このプールは古いデータパーツのクリーニング、データパーツの変更、レプリカの再初期化などの定期的なReplicatedMergeTreeタスクに使用されます。

### BackgroundSchedulePoolTask

BackgroundSchedulePoolでのアクティブなタスクの数。このプールは古いデータパーツのクリーニング、データパーツの変更、レプリカの再初期化などの定期的なReplicatedMergeTreeタスクに使用されます。

### BackupsIOThreads

BackupsIOスレッドプール内のスレッドの数。

### BackupsIOThreadsActive

BackupsIOスレッドプールでタスクを実行しているスレッドの数。

### BackupsThreads

BACKUP用のスレッドプール内のスレッドの数。

### BackupsThreadsActive

BACKUP用のスレッドプールでタスクを実行しているスレッドの数。

### BrokenDistributedFilesToInsert

破損としてマークされた分散テーブルへの非同期挿入ファイルの数。このメトリクスは開始時0から始まります。各シャードのファイル数を合算します。

### CacheDetachedFileSegments

既存の切り離されたキャッシュファイルセグメントの数。

### CacheDictionaryThreads

CacheDictionaryスレッドプール内のスレッドの数。

### CacheDictionaryThreadsActive

CacheDictionaryスレッドプールでタスクを実行しているスレッドの数。

### CacheDictionaryUpdateQueueBatches

CacheDictionaries内の更新キューの「バッチ」（キーのセット）数。

### CacheDictionaryUpdateQueueKeys

CacheDictionaries内の更新キューにある正確なキーの数。

### CacheFileSegments

既存のキャッシュファイルセグメントの数。

### ContextLockWait

Context内のロックを待っているスレッドの数。これはグローバルロックです。

### DDLWorkerThreads

ON CLUSTERクエリ用のDDLWorkerスレッドプール内のスレッドの数。

### DDLWorkerThreadsActive

ON CLUSTERクエリ用のDDLWorkerスレッドプールでタスクを実行しているスレッドの数。

### DatabaseCatalogThreads

DatabaseCatalogスレッドプール内のスレッドの数。

### DatabaseCatalogThreadsActive

DatabaseCatalogスレッドプールでタスクを実行しているスレッドの数。

### DatabaseOnDiskThreads

DatabaseOnDiskスレッドプール内のスレッドの数。

### DatabaseOnDiskThreadsActive

DatabaseOnDiskスレッドプールでタスクを実行しているスレッドの数。

### DelayedInserts

MergeTreeテーブルのパーティションに対してアクティブなデータパーツが多いため、調整されているINSERTクエリの数。

### DestroyAggregatesThreads

アグリゲート状態の破棄用スレッドプール内のスレッドの数。

### DestroyAggregatesThreadsActive

アグリゲート状態の破棄用スレッドプールでタスクを実行しているスレッドの数。

### DictCacheRequests

キャッシュタイプのディクショナリのデータソースへのフライリクエスト数。

### DiskObjectStorageAsyncThreads

DiskObjectStorage用の非同期スレッドプール内のスレッドの数。

### DiskObjectStorageAsyncThreadsActive

DiskObjectStorage用非同期スレッドプールでタスクを実行しているスレッドの数。

### DiskSpaceReservedForMerge

現在行っている背景マージ用に予約されたディスクスペース。これは現在マージしているパーツの合計サイズより若干大きくなります。

### DistributedFilesToInsert

分散テーブルへの非同期挿入に処理される保留中のファイルの数。各シャードのファイル数を合算します。

### DistributedSend

分散テーブルにINSERTされたデータを遠隔サーバへ送信する接続数。同期および非同期モードの両方を含みます。

### EphemeralNode

ZooKeeper内で保持するエフェメラルノードの数。

### FilesystemCacheElements

ファイルシステムキャッシュ要素（ファイルセグメント）。

### FilesystemCacheReadBuffers

アクティブなキャッシュバッファの数。

### FilesystemCacheSize

ファイルシステムキャッシュのサイズ（バイト単位）。

### GlobalThread

グローバルスレッドプール内のスレッドの数。

### GlobalThreadActive

グローバルスレッドプールでタスクを実行しているスレッドの数。

### HTTPConnection

HTTPサーバへの接続数。

### HashedDictionaryThreads

HashedDictionaryスレッドプール内のスレッドの数。

### HashedDictionaryThreadsActive

HashedDictionaryスレッドプールでタスクを実行しているスレッドの数。

### IOPrefetchThreads

IOプリフェッチスレッドプール内のスレッドの数。

### IOPrefetchThreadsActive

IOプリフェッチスレッドプールでタスクを実行しているスレッドの数。

### IOThreads

IOスレッドプール内のスレッドの数。

### IOThreadsActive

IOスレッドプールでタスクを実行しているスレッドの数。

### IOUringInFlightEvents

フライト中のio_uring SQEの数。

### IOUringPendingEvents

送信待ちのio_uring SQEの数。

### IOWriterThreads

IO書き込みスレッドプール内のスレッドの数。

### IOWriterThreadsActive

IO書き込みスレッドプールでタスクを実行しているスレッドの数。

### InterserverConnection

部品をフェッチするための他のレプリカからの接続数。

### KafkaAssignedPartitions

Kafkaテーブルに現在割り当てられているパーティションの数。

### KafkaBackgroundReads

バックグラウンドリード（Kafkaからのマテリアライズドビューの補充）に現在作業中の数。

### KafkaConsumers

アクティブなKafkaコンシューマの数。

### KafkaConsumersInUse

直接またはバックグラウンドリードにより現在利用されているコンシューマの数。

### KafkaConsumersWithAssignment

割り当てられたパーティションを持つアクティブなKafkaコンシューマの数。

### KafkaLibrdkafkaThreads

アクティブなlibrdkafkaスレッドの数。

### KafkaProducers

作成されたアクティブなKafkaプロデューサの数。

### KafkaWrites

Kafkaに挿入を実行中の数。

### KeeperAliveConnections

アライブ接続の数。

### KeeperOutstandingRequests

未処理のリクエストの数。

### LocalThread

ローカルスレッドプール内のスレッドの数。ローカルスレッドプール内のスレッドはグローバルスレッドプールから取得されます。

### LocalThreadActive

ローカルスレッドプールでタスクを実行しているスレッドの数。

### MMappedAllocBytes

mmappedアロケーションの合計バイト数。

### MMappedAllocs

mmappedアロケーションの総数。

### MMappedFileBytes

mmappedファイル領域の合計サイズ。

### MMappedFiles

mmappedファイルの総数。

### MarksLoaderThreads

マークのロード用スレッドプール内のスレッドの数。

### MarksLoaderThreadsActive

マークのロード用スレッドプールでタスクを実行しているスレッドの数。

### MaxDDLEntryID

DDLWorkerが処理した最大のDDLエントリ。

### MaxPushedDDLEntryID

ZooKeeperにプッシュされたDDLWorkerの最大DDLエントリ。

### MemoryTracking

サーバによって割り当てられたメモリ（バイト単位）の総量。

### Merge

実行中の背景マージの数。

### MergeTreeAllRangesAnnouncementsSent

リモートサーバから起動サーバへデータパーツのセットについて送信中のアナウンスメントの現在数（MergeTreeテーブル用）。リモートサーバ側で測定されます。

### MergeTreeBackgroundExecutorThreads

MergeTreeBackgroundExecutorスレッドプール内のスレッドの数。

### MergeTreeBackgroundExecutorThreadsActive

MergeTreeBackgroundExecutorスレッドプールでタスクを実行しているスレッドの数。

### MergeTreeDataSelectExecutorThreads

MergeTreeDataSelectExecutorスレッドプール内のスレッドの数。

### MergeTreeDataSelectExecutorThreadsActive

MergeTreeDataSelectExecutorスレッドプールでタスクを実行しているスレッドの数。

### MergeTreePartsCleanerThreads

MergeTreeパーツクリーナースレッドプール内のスレッドの数。

### MergeTreePartsCleanerThreadsActive

MergeTreeパーツクリーナースレッドプールでタスクを実行しているスレッドの数。

### MergeTreePartsLoaderThreads

MergeTreeパーツローダースレッドプール内のスレッドの数。

### MergeTreePartsLoaderThreadsActive

MergeTreeパーツローダースレッドプールでタスクを実行しているスレッドの数。

### MergeTreeReadTaskRequestsSent

リモートサーバ側で読み取りタスクを選択するため、リモートサーバから起動サーバ側へフライト中のコールバックリクエストの数（MergeTreeテーブル用）。リモートサーバ側で測定されます。

### Move

現在実行中の移動の数。

### MySQLConnection

MySQLプロトコルを使用しているクライアント接続の数。

### NetworkReceive

ネットワークからデータを受信するスレッドの数。ClickHouse関連のネットワーク相互作用のみが含まれ、サードパーティライブラリは含まれません。

### NetworkSend

ネットワークにデータを送信するスレッドの数。ClickHouse関連のネットワーク相互作用のみが含まれ、サードパーティライブラリは含まれません。

### OpenFileForRead

読み取り用に開かれたファイルの数。

### OpenFileForWrite

書き込み用に開かれたファイルの数。

### ParallelFormattingOutputFormatThreads

ParallelFormattingOutputFormatThreadsスレッドプール内のスレッドの数。

### ParallelFormattingOutputFormatThreadsActive

ParallelFormattingOutputFormatThreadsスレッドプールでタスクを実行しているスレッドの数。

### ParallelParsingInputFormatThreads

ParallelParsingInputFormatスレッドプール内のスレッドの数。

### ParallelParsingInputFormatThreadsActive

ParallelParsingInputFormatスレッドプールでタスクを実行しているスレッドの数。

### PartMutation

ミューテーション（ALTER DELETE/UPDATE）の数。

### PartsActive

現在および今後のSELECTによって使用されるアクティブデータパート。

### PartsCommitted

廃止されました。PartsActiveを参照してください。

### PartsCompact

コンパクトなパーツ。

### PartsDeleteOnDestroy

別のディスクに移動され、独自のデストラクタで削除されるパート。

### PartsDeleting

アイデンティティ参照カウンタを持つ非アクティブデータパートで、現在クリーナーによって削除中。

### PartsOutdated

非アクティブデータパートですが、現在のSELECTによってのみ使用可能で、SELECTが終了したら削除可能。

### PartsPreActive

データパーツに存在しますが、SELECTでは使用されていない。

### PartsPreCommitted

廃止されました。PartsPreActiveを参照してください。

### PartsTemporary

現在生成中で、data_partsリストには存在しません。

### PartsWide

ワイドパーツ。

### PendingAsyncInsert

フラッシュ待機中の非同期挿入の数。

### PostgreSQLConnection

PostgreSQLプロトコルを使用しているクライアント接続の数。

### Query

実行中のクエリの数。

### QueryPreempted

「priority」設定のために停止し、待機中のクエリの数。

### QueryThread

クエリ処理スレッドの数。

### RWLockActiveReaders

テーブルのRWLockで読み取りロックを保持しているスレッドの数。

### RWLockActiveWriters

テーブルのRWLockで書き込みロックを保持しているスレッドの数。

### RWLockWaitingReaders

テーブルのRWLockで読み取り待機中のスレッドの数。

### RWLockWaitingWriters

テーブルのRWLockで書き込み待機中のスレッドの数。

### Read

リード（read、pread、io_geteventsなど）システムコールのフライト中の数。

### ReadTaskRequestsSent

リモートサーバから起動サーバへフライト中のコールバックリクエストの数（s3Clusterテーブル関数など用）。リモートサーバ側で測定されます。

### ReadonlyReplica

ZooKeeperセッションの喪失後の再初期化、またはZooKeeper未設定での起動により、現在readonly状態のレプリケートテーブルの数。

### RemoteRead

リモートリーダーでのフライト中のリードの数。

### ReplicatedChecks

一貫性をチェックするためのデータパーツの数。

### ReplicatedFetch

レプリカからフェッチ中のデータパーツの数。

### ReplicatedSend

レプリカへ送信中のデータパーツの数。

### RestartReplicaThreads

RESTART REPLICAスレッドプール内のスレッドの数。

### RestartReplicaThreadsActive

RESTART REPLICAスレッドプールでタスクを実行しているスレッドの数。

### RestoreThreads

RESTORE用スレッドプール内のスレッドの数。

### RestoreThreadsActive

RESTORE用スレッドプールでタスクを実行しているスレッドの数。

### Revision

サーバのリビジョン。パッチリリースを除く、すべてのリリースまたはリリース候補でインクリメントされる数。

### S3Requests

S3リクエスト。

### SendExternalTables

リモートサーバへ外部テーブルのデータを送信中の接続数。外部テーブルは、分散サブクエリを使用したGLOBAL INおよびGLOBAL JOIN演算子を実装するために使用されます。

### SendScalars

リモートサーバへスカラーのデータを送信中の接続数。

### StorageBufferBytes

Bufferテーブルのバッファ内のバイト数。

### StorageBufferRows

Bufferテーブルのバッファ内の行数。

### StorageDistributedThreads

StorageDistributedスレッドプール内のスレッドの数。

### StorageDistributedThreadsActive

StorageDistributedスレッドプールでタスクを実行しているスレッドの数。

### StorageHiveThreads

StorageHiveスレッドプール内のスレッドの数。

### StorageHiveThreadsActive

StorageHiveスレッドプールでタスクを実行しているスレッドの数。

### StorageS3Threads

StorageS3スレッドプール内のスレッドの数。

### StorageS3ThreadsActive

StorageS3スレッドプールでタスクを実行しているスレッドの数。

### SystemReplicasThreads

system.replicasスレッドプール内のスレッドの数。

### SystemReplicasThreadsActive

system.replicasスレッドプールでタスクを実行しているスレッドの数。

### TCPConnection

TCPサーバへの接続数（ネイティブインターフェースを持つクライアント）、サーバ間の分散クエリ接続も含まれる。

### TablesToDropQueueSize

背景でのデータ削除を待機している削除テーブルの数。

### TemporaryFilesForAggregation

外部集計のために作成された一時ファイルの数。

### TemporaryFilesForJoin

JOINのために作成された一時ファイルの数。

### TemporaryFilesForSort

外部ソートのために作成された一時ファイルの数。

### TemporaryFilesUnknown

目的不明で作成された一時ファイルの数。

### ThreadPoolFSReaderThreads

local_filesystem_read_method=threadpool用スレッドプール内のスレッドの数。

### ThreadPoolFSReaderThreadsActive

local_filesystem_read_method=threadpool用スレッドプールでタスクを実行しているスレッドの数。

### ThreadPoolRemoteFSReaderThreads

remote_filesystem_read_method=threadpool用スレッドプール内のスレッドの数。

### ThreadPoolRemoteFSReaderThreadsActive

remote_filesystem_read_method=threadpool用スレッドプールでタスクを実行しているスレッドの数。

### ThreadsInOvercommitTracker

OvercommitTracker内で待機中のスレッド数。

### TotalTemporaryFiles

作成された一時ファイルの数。

### VersionInteger

バージョンを基数1000の単一整数で表したサーバのバージョン。例えば、バージョン11.22.33は11022033に変換されます。

### Write

書き込み（write、pwrite、io_geteventsなど）システムコールのフライト中の数。

### ZooKeeperRequest

フライト中のZooKeeperへのリクエスト数。

### ZooKeeperSession

ZooKeeperへのセッション（接続）数。バグの原因となる可能性があるため、ZooKeeperへの複数接続を使用しない限り、1を超えないようにしてください。これはZooKeeperの一貫性モデルが許す直線化性の欠如（古い読み取り）によるものです。

### ZooKeeperWatch

ZooKeeperでのウォッチ（イベント購読）の数。

### ConcurrencyControlAcquired

取得されたCPUスロットの総数。

### ConcurrencyControlSoftLimit

CPUスロット数のソフトリミット値。

**参照**

- [system.asynchronous_metrics](../../operations/system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics) — 定期的に計算されるメトリクスを含みます。
- [system.events](../../operations/system-tables/events.md#system_tables-events) — 発生したイベントの数を含みます。
- [system.metric_log](../../operations/system-tables/metric_log.md#system_tables-metric_log) — テーブル `system.metrics` と `system.events` からのメトリクス値の履歴を含みます。
- [モニタリング](../../operations/monitoring.md) — ClickHouseモニタリングの基本概念。
