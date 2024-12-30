---
slug: /ja/operations/system-tables/asynchronous_metrics
---
# asynchronous_metrics

バックグラウンドで定期的に計算されるメトリクスを含みます。例えば、使用中のRAMの量などです。

カラム:

- `metric` ([String](../../sql-reference/data-types/string.md)) — メトリクス名。
- `value` ([Float64](../../sql-reference/data-types/float.md)) — メトリクスの値。
- `description` ([String](../../sql-reference/data-types/string.md) - メトリクスの説明）

**例**

``` sql
SELECT * FROM system.asynchronous_metrics LIMIT 10
```

``` text
┌─metric──────────────────────────────────┬──────value─┬─description────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ AsynchronousMetricsCalculationTimeSpent │ 0.00179053 │ 時間（秒単位）非同期メトリクスの計算に費やされた時間（これは非同期メトリクスのオーバーヘッドです）。                                                                                                                                              │
│ NumberOfDetachedByUserParts             │          0 │ `ALTER TABLE DETACH`クエリを使用してユーザーによってMergeTreeテーブルから分離されたパーツの総数（予期しない、壊れた、または無視されたパーツとは対照的に）。サーバーは分離されたパーツを気にしません、それらは削除される可能性があります。                          │
│ NumberOfDetachedParts                   │          0 │ MergeTreeテーブルから分離されたパーツの総数。パーツは、ユーザーが`ALTER TABLE DETACH`クエリを使用するか、サーバー自身によって壊れた、予期しない、または不要な場合に分離される可能性があります。サーバーは分離されたパーツを気にしません、それらは削除される可能性があります。 │
│ TotalRowsOfMergeTreeTables              │    2781309 │ MergeTreeファミリーのすべてのテーブルに格納されている行（レコード）の総数。                                                                                                                                                                                   │
│ TotalBytesOfMergeTreeTables             │    7741926 │ MergeTreeファミリーのすべてのテーブルに格納されているバイト数（データとインデックスを含む圧縮）。                                                                                                                                                   │
│ NumberOfTables                          │         93 │ サーバー上のデータベース全体のテーブル総数、MergeTreeテーブルを含むことができないデータベースを除外。`Lazy`、`MySQL`、`PostgreSQL`、`SQlite`のように動的にテーブルセットを生成するデータベースエンジンは除外されます。 │
│ NumberOfDatabases                       │          6 │ サーバー上のデータベースの総数。                                                                                                                                                                                                                   │
│ MaxPartCountForPartition                │          6 │ MergeTreeファミリーのすべてのテーブルの各パーティションにおけるパーツの最大数。値が300を超えると、設定ミス、過負荷、大規模なデータロードを示します。                                                                       │
│ ReplicasSumMergesInQueue                │          0 │ レプリケートされたテーブル全体でキュー（まだ適用されていない）にあるマージ操作の合計。                                                                                                                                                                       │
│ ReplicasSumInsertsInQueue               │          0 │ レプリケートされたテーブル全体でキュー（まだレプリケートされていない）にあるINSERT操作の合計。                                                                                                                                                                   │
└─────────────────────────────────────────┴────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

<!--- Unlike with system.events and system.metrics, the asynchronous metrics are not gathered in a simple list in a source code file - they are mixed with logic in src/Interpreters/ServerAsynchronousMetrics.cpp. Listing them here explicitly for reader convenience. --->

## メトリクスの説明

### AsynchronousHeavyMetricsCalculationTimeSpent

非同期の重い（テーブル関連の）メトリクス計算に費やされた時間（秒単位）（これは非同期メトリクスのオーバーヘッドです）。

### AsynchronousHeavyMetricsUpdateInterval

重い（テーブル関連の）メトリクス更新間隔

### AsynchronousMetricsCalculationTimeSpent

非同期メトリクス計算に費やされた時間（秒単位）（これは非同期メトリクスのオーバーヘッドです）。

### AsynchronousMetricsUpdateInterval

メトリクス更新間隔

### BlockActiveTime_*name*

ブロックデバイス上でIOリクエストがキューされていた時間（秒単位）。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなくホストマシン上のすべてのプロセスが含まれます。出典：`/sys/block`。詳細はhttps://www.kernel.org/doc/Documentation/block/stat.txtを参照。

### BlockDiscardBytes_*name*

ブロックデバイス上で破棄されたバイト数。これらの操作はSSDに関連しています。破棄操作はClickHouseで使用されませんが、システム内の他のプロセスで使用される可能性があります。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなくホストマシン上のすべてのプロセスが含まれます。出典：`/sys/block`。詳細はhttps://www.kernel.org/doc/Documentation/block/stat.txtを参照。

### BlockDiscardMerges_*name*

ブロックデバイスから要求され、OS IOスケジューラによってマージされた破棄操作の数。これらの操作はSSDに関連しています。破棄操作はClickHouseで使用されませんが、システム内の他のプロセスで使用される可能性があります。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなくホストマシン上のすべてのプロセスが含まれます。出典：`/sys/block`。詳細はhttps://www.kernel.org/doc/Documentation/block/stat.txtを参照。

### BlockDiscardOps_*name*

ブロックデバイスから要求された破棄操作の数。これらの操作はSSDに関連しています。破棄操作はClickHouseで使用されませんが、システム内の他のプロセスで使用される可能性があります。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなくホストマシン上のすべてのプロセスが含まれます。出典：`/sys/block`。詳細はhttps://www.kernel.org/doc/Documentation/block/stat.txtを参照。

### BlockDiscardTime_*name*

ブロックデバイスから要求された破棄操作に費やされた時間（秒単位）、すべての操作に渡って合計されたもの。これらの操作はSSDに関連しています。破棄操作はClickHouseで使用されませんが、システム内の他のプロセスで使用される可能性があります。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなくホストマシン上のすべてのプロセスが含まれます。出典：`/sys/block`。詳細はhttps://www.kernel.org/doc/Documentation/block/stat.txtを参照。

### BlockInFlightOps_*name*

デバイスドライバに発行されたがまだ完了していないI/Oリクエストの数をカウントします。これは、キューにありまだデバイスドライバに発行されていないI/Oリクエストは含まれません。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなくホストマシン上のすべてのプロセスが含まれます。出典：`/sys/block`。詳細はhttps://www.kernel.org/doc/Documentation/block/stat.txtを参照。

### BlockQueueTime_*name*

このブロックデバイスでIOリクエストが待機していたミリ秒数をカウントします。複数のIOリクエストが待機している場合、この値はミリ秒数と待機しているリクエスト数の積として増加します。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなくホストマシン上のすべてのプロセスが含まれます。出典：`/sys/block`。詳細はhttps://www.kernel.org/doc/Documentation/block/stat.txtを参照。

### BlockReadBytes_*name*

ブロックデバイスから読み取られたバイト数。OSページキャッシュの使用により、ファイルシステムから読み取られるバイト数よりも少なくなる可能性があります。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなくホストマシン上のすべてのプロセスが含まれます。出典：`/sys/block`。詳細はhttps://www.kernel.org/doc/Documentation/block/stat.txtを参照。

### BlockReadMerges_*name*

ブロックデバイスから要求され、OS IOスケジューラによってマージされた読み取り操作の数。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなくホストマシン上のすべてのプロセスが含まれます。出典：`/sys/block`。詳細はhttps://www.kernel.org/doc/Documentation/block/stat.txtを参照。

### BlockReadOps_*name*

ブロックデバイスから要求された読み取り操作の数。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなくホストマシン上のすべてのプロセスが含まれます。出典：`/sys/block`。詳細はhttps://www.kernel.org/doc/Documentation/block/stat.txtを参照。

### BlockReadTime_*name*

ブロックデバイスからの読み取り操作に費やされた時間（秒単位）、すべての操作に渡って合計されたもの。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなくホストマシン上のすべてのプロセスが含まれます。出典：`/sys/block`。詳細はhttps://www.kernel.org/doc/Documentation/block/stat.txtを参照。

### BlockWriteBytes_*name*

ブロックデバイスに書き込まれたバイト数。OSページキャッシュの使用により、ファイルシステムへの書き込みと比べて少ない場合があります。また、書き込みスルーキャッシングのため、対応するファイルシステムへの書き込みよりも後にブロックデバイスへの書き込みが発生する可能性があります。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなくホストマシン全体のプロセスが含まれます。出典：`/sys/block`。詳細はhttps://www.kernel.org/doc/Documentation/block/stat.txtを参照。

### BlockWriteMerges_*name*

ブロックデバイスから要求され、OS IOスケジューラによってマージされた書き込み操作の数。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなくホストマシン全体のプロセスが含まれます。出典：`/sys/block`。詳細はhttps://www.kernel.org/doc/Documentation/block/stat.txtを参照。

### BlockWriteOps_*name*

ブロックデバイスから要求された書き込み操作の数。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなくホストマシン全体のプロセスが含まれます。出典：`/sys/block`。詳細はhttps://www.kernel.org/doc/Documentation/block/stat.txtを参照。

### BlockWriteTime_*name*

ブロックデバイスからの書き込み操作に費やされた時間（秒単位）、すべての操作に渡って合計されたもの。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなくホストマシン全体のプロセスが含まれます。出典：`/sys/block`。詳細はhttps://www.kernel.org/doc/Documentation/block/stat.txtを参照。

### CPUFrequencyMHz_*name*

CPUの現在の周波数（MHz）。ほとんどの最新のCPUは、電力節約とターボブーストのために周波数を動的に調整します。

### CompiledExpressionCacheBytes

JITコンパイルコードキャッシュに使用されている総バイト数。

### CompiledExpressionCacheCount

JITコンパイルコードキャッシュの総エントリー数。

### DiskAvailable_*name*

ディスク（仮想ファイルシステム）で使用可能なバイト数。リモートファイルシステムは、16 EiBのような大きな値を示すことがあります。

### DiskTotal_*name*

ディスク（仮想ファイルシステム）の総サイズ（バイト単位）。リモートファイルシステムは、16 EiBのような大きな値を示すことがあります。

### DiskUnreserved_*name*

マージ、フェッチ、移動のための予約なしでディスク（仮想ファイルシステム）で使用可能なバイト数。リモートファイルシステムは、16 EiBのような大きな値を示すことがあります。

### DiskUsed_*name*

ディスク（仮想ファイルシステム）で使用されているバイト数。リモートファイルシステムは、常にこの情報を提供するとは限りません。

### FilesystemCacheBytes

`cache`仮想ファイルシステム内の総バイト数。このキャッシュはディスク上に保持されます。

### FilesystemCacheFiles

`cache`仮想ファイルシステム内のキャッシュされたファイルセグメントの総数。このキャッシュはディスク上に保持されます。

### FilesystemLogsPathAvailableBytes

ClickHouseログパスがマウントされているボリュームで使用可能なバイト数。この値がゼロに近づくと、設定ファイルでログのローテーションを調整する必要があります。

### FilesystemLogsPathAvailableINodes

ClickHouseログパスがマウントされているボリュームで利用可能なinodeの数。

### FilesystemLogsPathTotalBytes

ClickHouseログパスがマウントされているボリュームのサイズ（バイト単位）。ログのために少なくとも10 GBを確保することが推奨されます。

### FilesystemLogsPathTotalINodes

ClickHouseログパスがマウントされているボリューム上のinodeの総数。

### FilesystemLogsPathUsedBytes

ClickHouseログパスがマウントされているボリュームで使用されているバイト数。

### FilesystemLogsPathUsedINodes

ClickHouseログパスがマウントされているボリュームで使用されているinodeの数。

### FilesystemMainPathAvailableBytes

メインClickHouseパスがマウントされているボリュームで使用可能なバイト数。

### FilesystemMainPathAvailableINodes

メインClickHouseパスがマウントされているボリュームで利用可能なinodeの数。それがゼロに近づいている場合、設定ミスを示しており、ディスクがいっぱいでなくても'no space left on device'エラーを引き起こします。

### FilesystemMainPathTotalBytes

メインClickHouseパスがマウントされているボリュームのサイズ（バイト単位）。

### FilesystemMainPathTotalINodes

メインClickHouseパスがマウントされているボリューム上のinodeの総数。それが2500万未満である場合、設定ミスを示しています。

### FilesystemMainPathUsedBytes

メインClickHouseパスがマウントされているボリュームで使用されているバイト数。

### FilesystemMainPathUsedINodes

メインClickHouseパスがマウントされているボリュームで使用されているinodeの数。この値は主にファイル数に対応しています。

### HTTPThreads

HTTPインターフェースのサーバー上のスレッド数（TLSなし）。

### InterserverThreads

レプリカ通信プロトコルのサーバー上のスレッド数（TLSなし）。

### Jitter

非同期メトリクスの計算用スレッドが起床するようにスケジュールされた時刻と実際に起床した時刻の時間差。全体的なシステムのレイテンシーと応答性の代理指標。

### LoadAverage_*N*

指数平滑化で1分間平滑化された全システムの負荷。この負荷は、現在CPUが実行中であるか、IOを待っているか、またはこの時点でスケジュールされていないが実行可能なOSカーネルのスレッド数を表しています。この数値はClickhouse-serverだけでなく全てのプロセスを含んでいます。システムが過負荷状態にある場合、特に数十個のプロセスが実行可能であるがCPUまたはIOを待っている場合、数値はCPUコア数を超えることがあります。

### MMapCacheCells

`mmap`（メモリマップ）で開かれたファイルの数。この設定は、`local_filesystem_read_method`を`mmap`に設定したクエリに使用されます。`ｍmap`で開かれたファイルは、費用がかかるTLBフラッシュを避けるためにキャッシュに保持されます。

### MarkCacheBytes

マークキャッシュ内の総サイズ（バイト単位）

### MarkCacheFiles

マークキャッシュ内にキャッシュされたマークファイルの総数

### MaxPartCountForPartition

MergeTreeファミリーのすべてのテーブルの各パーティションにおけるパーツの最大数。値が300を超えると、設定ミス、過負荷、大規模データロードを示します。

### MemoryCode

サーバープロセスの機械コードページのためにマップされた仮想メモリの量（バイト単位）。

### MemoryDataAndStack

スタックおよび割り当てられたメモリの使用のためにマップされた仮想メモリの量（バイト単位）。これは、スレッドごとのスタックと'mmap'システムコールで割り当てられたメモリのほとんどが含まれているかどうかは不特定です。このメトリクスは完全性のために存在します。モニタリングには`MemoryResident`メトリクスを使用することをお勧めします。

### MemoryResidentMax

サーバープロセスによって使用される物理メモリの最大量（バイト単位）。

### MemoryResident

サーバープロセスによって使用される物理メモリの量（バイト単位）。

### MemoryShared

サーバープロセスによって使用されるメモリの量であり、他のプロセスと共有されています（バイト単位）。ClickHouseは共有メモリを使用しませんが、一部のメモリはOSによって独自の理由で共有としてラベル付けされることがあります。このメトリクスは、監視する意味はあまりなく、完全性のために存在します。

### MemoryVirtual

サーバープロセスによって割り当てられた仮想アドレススペースのサイズ（バイト単位）。仮想アドレススペースのサイズは通常、物理メモリ消費よりもはるかに大きく、メモリ消費の見積もりとして使用されるべきではありません。このメトリクスの大きな値は完全に正常であり、技術的な意味しか持たない。

### MySQLThreads

MySQL互換性プロトコルのサーバー内のスレッド数。

### NetworkReceiveBytes_*name*

ネットワークインターフェースを介して受信されたバイト数。これはシステム全体のメトリクスであり、ホストマシン上のすべてのプロセスを含んでいます、Clickhouse-serverだけではありません。

### NetworkReceiveDrop_*name*

ネットワークインターフェースを介して受信中にパケットがドロップされたバイト数。これはシステム全体のメトリクスであり、ホストマシン上のすべてのプロセスを含んでいます、Clickhouse-serverだけではありません。

### NetworkReceiveErrors_*name*

ネットワークインターフェースを介して受信中に発生したエラーの回数。これはシステム全体のメトリクスであり、ホストマシン上のすべてのプロセスを含んでいます、Clickhouse-serverだけではありません。

### NetworkReceivePackets_*name*

ネットワークインターフェースを介して受信されたネットワークパケットの数。これはシステム全体のメトリクスであり、ホストマシン上のすべてのプロセスを含んでいます、Clickhouse-serverだけではありません。

### NetworkSendBytes_*name*

ネットワークインターフェースを介して送信されたバイト数。これはシステム全体のメトリクスであり、ホストマシン上のすべてのプロセスを含んでいます、Clickhouse-serverだけではありません。

### NetworkSendDrop_*name*

ネットワークインターフェースを介して送信中にパケットがドロップされた回数。これはシステム全体のメトリクスであり、ホストマシン上のすべてのプロセスを含んでいます、Clickhouse-serverだけではありません。

### NetworkSendErrors_*name*

ネットワークインターフェースを介して送信中にエラー（例：TCP再送信）が発生した回数。これはシステム全体のメトリクスであり、ホストマシン上のすべてのプロセスを含んでいます、Clickhouse-serverだけではありません。

### NetworkSendPackets_*name*

ネットワークインターフェースを介して送信されたネットワークパケットの数。これはシステム全体のメトリクスであり、ホストマシン上のすべてのプロセスを含んでいます、Clickhouse-serverだけではありません。

### NumberOfDatabases

サーバー上のデータベースの総数。

### NumberOfDetachedByUserParts

ユーザーによって`ALTER TABLE DETACH`クエリを使用してMergeTreeテーブルから分離されたパーツの合計（予期しない、壊れた、または無視されたパーツとは対照的）。サーバーは分離されたパーツを気にせず、それらは削除可能です。

### NumberOfDetachedParts

MergeTreeテーブルから分離されたパーツの合計。ユーザーが`ALTER TABLE DETACH`クエリを使ってパーツを分離するか、サーバー自身が壊れた、予期しない、または不要な場合に分離されます。サーバーは分離されたパーツを気にせず、それらは削除可能です。

### NumberOfTables

サーバー上のデータベース全体のテーブル総数、MergeTreeテーブルを含むことができないデータベースを除外。`Lazy`、`MySQL`、`PostgreSQL`、`SQlite`のように動的にテーブルセットを生成するデータベースエンジンは除外されます。

### OSContextSwitches

ホストマシンが経験したコンテキストスイッチの数。これはシステム全体のメトリクスであり、ホストマシン上のすべてのプロセスを含んでいます、Clickhouse-serverだけではありません。

### OSGuestNiceTime

Linuxカーネルの制御下で、ゲストが優先度を高く設定されたとき（`man procfs`参照）、仮想CPUを実行するために費やされた時間の割合です。これはシステム全体のメトリクスであり、ホストマシン上のすべてのプロセスを含んでいます、Clickhouse-serverだけではありません。このメトリクスはClickHouseには無関係ですが、完全性のために存在します。単一のCPUコアの値は[0..1]の範囲内にあります。全CPUコアの値はそれらの合計として計算され、[0..数コア]の範囲になります。

### OSGuestNiceTimeCPU_*N*

Linuxカーネルの制御下で、ゲストが優先度を高く設定されたとき（`man procfs`参照）、仮想CPUを実行するために費やされた時間の割合です。これはシステム全体のメトリクスであり、ホストマシン上のすべてのプロセスを含んでいます、Clickhouse-serverだけではありません。このメトリクスはClickHouseには無関係ですが、完全性のために存在します。単一のCPUコアの値は[0..1]の範囲内にあります。全CPUコアの値はそれらの合計として計算され、[0..数コア]の範囲になります。

### OSGuestNiceTimeNormalized

この値は`OSGuestNiceTime`と類似していますが、コア数で割られており、コア数に関係なく[0..1]の範囲で測定されます。これにより、コア数が非一様であってもクラスタ内の複数のサーバー間でこのメトリクスの平均値を算出し、平均リソース使用率メトリクスを得ることができます。

### OSGuestTime

Linuxカーネルの制御下で、仮想CPUを実行するために費やされた時間の割合です（`man procfs`参照）。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなくホストマシン上のすべてのプロセスが含まれます。このメトリクスはClickHouseには無関係ですが、完全性のために存在します。単一のCPUコアの値は[0..1]の範囲内にあり、全CPUコアの値はそれらの合計として計算され、[0..コアの数]の範囲になります。

### OSGuestTimeCPU_*N*

Linuxカーネルの制御下で、仮想CPUを実行するために費やされた時間の割合です（`man procfs`参照）。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなくホストマシン上のすべてのプロセスが含まれます。このメトリクスはClickHouseには無関係ですが、完全性のために存在します。単一のCPUコアの値は[0..1]の範囲内にあり、全CPUコアの値はそれらの合計として計算され、[0..コアの数]の範囲になります。

### OSGuestTimeNormalized

この値は`OSGuestTime`と類似していますが、コア数で割られており、コア数に関係なく[0..1]の範囲で測定されます。これにより、コア数が非一様であってもクラスタ内の複数のサーバー間でこのメトリクスの平均値を算出し、平均リソース使用率メトリクスを得ることができます。

### OSIOWaitTime

プロセスがI/Oを待っている際に、OSカーネルがこのCPUで他のプロセスを実行しなかった時間の割合です。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなくホストマシン上のすべてのプロセスが含まれます。単一のCPUコアの値は[0..1]の範囲内にあり、全CPUコアの値はそれらの合計として計算され、[0..コアの数]の範囲になります。

### OSIOWaitTimeCPU_*N*

プロセスがI/Oを待っている際に、OSカーネルがこのCPUで他のプロセスを実行しなかった時間の割合です。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなくホストマシン上のすべてのプロセスが含まれます。単一のCPUコアの値は[0..1]の範囲内にあり、全CPUコアの値はそれらの合計として計算され、[0..コアの数]の範囲になります。

### OSIOWaitTimeNormalized

この値は`OSIOWaitTime`と類似していますが、コア数で割られており、コア数に関係なく[0..1]の範囲で測定されます。これにより、コア数が非一様であってもクラスタ内の複数のサーバー間でこのメトリクスの平均値を算出し、平均リソース使用率メトリクスを得ることができます。

### OSIdleTime

CPUコアがアイドル状態（I/Oを待っているプロセスを実行する準備ができていない状態）の時間の割合をOSカーネルの観点から示します。これはシステム全体のメトリクスであり、ホストマシン上のすべてのプロセスが含まれます、Clickhouse-serverだけではありません。これには、CPUが内部で要因（メモリロード、パイプラインの停止、分岐予測の失敗、他のSMTコアの実行など）によって十分に利用されていなかった時間は含まれません。単一のCPUコアの値は[0..1]の範囲内にあります。すべてのCPUコアの値は、これらすべてのコアの合計として計算されるため、[0..コア数]の範囲内になります。

### OSIdleTimeCPU_*N*

CPUコアがアイドル状態（I/Oを待っているプロセスを実行する準備ができていない状態）の時間の割合をOSカーネルの観点から示します。これはシステム全体のメトリクスであり、ホストマシン上のすべてのプロセスが含まれます、Clickhouse-serverだけではありません。これには、CPUが内部で要因（メモリロード、パイプラインの停止、分岐予測の失敗、他のSMTコアの実行など）によって十分に利用されていなかった時間は含まれません。単一のCPUコアの値は[0..1]の範囲内にあります。すべてのCPUコアの値は、これらすべてのコアの合計として計算されるため、[0..コア数]の範囲内になります。

### OSIdleTimeNormalized

この値は`OSIdleTime`と類似しており、コア数で割られるため、コア数に関係なく[0..1]の範囲で測定されます。これにより、コア数が非一様であっても、クラスタ内の複数のサーバー間でのこのメトリクスの平均化が可能になり、平均リソース利用率メトリクスを得ることができます。

### OSInterrupts

ホストマシン上で発生した割り込みの総数です。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなく、ホストマシン上のすべてのプロセスが含まれます。

### OSIrqTime

CPUでハードウェア割込み要求の実行に費やされた時間の割合を示します。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなく、ホストマシン上のすべてのプロセスが含まれます。このメトリクスの高い値は、ハードウェアの設定ミスや非常に高いネットワーク負荷を示している可能性があります。単一のCPUコアの値は[0..1]の範囲にあります。全CPUコアの値はそれらの合計で計算され、[0..num cores]の範囲になります。

### OSIrqTimeCPU_*N*

CPUでハードウェア割込み要求の実行に費やされた時間の割合を示します。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなく、ホストマシン上のすべてのプロセスが含まれます。このメトリクスの高い値は、ハードウェアの設定ミスや非常に高いネットワーク負荷を示している可能性があります。単一のCPUコアの値は[0..1]の範囲にあります。全CPUコアの値はそれらの合計で計算され、[0..num cores]の範囲になります。

### OSIrqTimeNormalized

この値は`OSIrqTime`と類似していますが、コア数で割られており、コア数に関係なく[0..1]の範囲で測定されます。これにより、コア数が非一様であってもクラスタ内の複数のサーバー間でこのメトリクスの平均化が可能になり、平均リソース使用率メトリクスを得ることができます。

### OSMemoryAvailable

プログラムが使用できるメモリの量（バイト単位）。これは`OSMemoryFreePlusCached`メトリクスと非常に似ています。これはシステム全体のメトリクスであり、ホストマシン上のすべてのプロセスが含まれています。

### OSMemoryBuffers

OSカーネルバッファに使用されているメモリの量（バイト単位）。これは通常、小さいはずであり、大きな値はOSの設定ミスを示している可能性があります。これはシステム全体のメトリクスであり、ホストマシン上のすべてのプロセスが含まれています。

### OSMemoryCached

OSページキャッシュによって使用されているメモリの量（バイト単位）。一般に、ほぼすべての利用可能なメモリがOSページキャッシュによって使用され、高い値は正常かつ予期されるものです。これはシステム全体のメトリクスであり、ホストマシン上のすべてのプロセスが含まれています。

### OSMemoryFreePlusCached

ホストシステムの自由メモリとOSページキャッシュメモリの合計量（バイト単位）。このメモリはプログラムで使用可能です。この値は`OSMemoryAvailable`と非常に似ているはずです。これはシステム全体のメトリクスであり、ホストマシン上のすべてのプロセスが含まれています。

### OSMemoryFreeWithoutCached

ホストシステムの自由メモリの量（バイト単位）。OSページキャッシュメモリは含まれていません。ページキャッシュメモリはプログラムで使用可能であるため、このメトリクスの値は混乱を招く可能性があります。代わりに`OSMemoryAvailable`メトリクスを参照してください。便宜上、`OSMemoryFreePlusCached`メトリクスも提供しており、OSMemoryAvailableとほぼ似ているはずです。詳細はhttps://www.linuxatemyram.com/を参照してください。これはシステム全体のメトリクスであり、ホストマシン上のすべてのプロセスが含まれています。

### OSMemoryTotal

ホストシステムの総メモリ量（バイト単位）。

### OSNiceTime

CPUコアが高い優先度でユーザースペースコードを実行していた時間の割合。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなく、ホストマシン上のすべてのプロセスが含まれます。単一のCPUコアの値は[0..1]の範囲にあります。全CPUコアの値はそれらの合計で計算され、[0..num cores]の範囲になります。

### OSNiceTimeCPU_*N*

CPUコアが高い優先度でユーザースペースコードを実行していた時間の割合。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなく、ホストマシン上のすべてのプロセスが含まれます。単一のCPUコアの値は[0..1]の範囲にあります。全CPUコアの値はそれらの合計で計算され、[0..num cores]の範囲になります。

### OSNiceTimeNormalized

この値は`OSNiceTime`と類似していますが、コア数で割られており、コア数に関係なく[0..1]の範囲で測定されます。これにより、コア数が非一様であっても、クラスタ内の複数のサーバー間でこのメトリクスの平均値を算出し、平均リソース使用率メトリクスを得ることができます。

### OSOpenFiles

ホストマシン上で開かれているファイルの総数。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなく、ホストマシン上のすべてのプロセスが含まれます。

### OSProcessesBlocked

I/O完了待ちでブロックされているスレッドの数（`man procfs`参照）。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなく、ホストマシン上のすべてのプロセスが含まれます。

### OSProcessesCreated

作成されたプロセスの数。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなく、ホストマシン上のすべてのプロセスが含まれます。

### OSProcessesRunning

オペレーティングシステムによって実行可能と見なされるスレッドの数（実行中か、実行準備中か）。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなく、ホストマシン上のすべてのプロセスが含まれます。

### OSSoftIrqTime

CPUでソフトウェア割込み要求の実行に費やされた時間の割合。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなく、ホストマシン上のすべてのプロセスが含まれます。このメトリクスの高い値は、システム上で非効率的なソフトウェアが実行されている可能性を示しています。単一のCPUコアの値は[0..1]の範囲にあります。全CPUコアの値はそれらの合計で計算され、[0..num cores]の範囲になります。

### OSSoftIrqTimeCPU_*N*

CPUでソフトウェア割込み要求の実行に費やされた時間の割合。これはシステム全体のメトリクスであり、Clickhouse-serverだけではなく、ホストマシン上のすべてのプロセスが含まれます。このメトリクスの高い値は、システム上で非効率的なソフトウェアが実行されている可能性を示しています。単一のCPUコアの値は[0..1]の範囲にあります。全CPUコアの値はそれらの合計で計算され、[0..num cores]の範囲になります。

### OSSoftIrqTimeNormalized

この値は`OSSoftIrqTime`と類似していますが、コア数で割られており、コア数に関係なく[0..1]の範囲で測定されます。これにより、コア数が非一様であってもクラスタ内の複数のサーバー間でこのメトリクスの平均化が可能になり、平均リソース使用率メトリクスを得ることができます。

### OSStealTime

仮想化環境で実行中のCPUが他のオペレーティングシステムに費やされた時間の割合。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなく、ホストマシン上のすべてのプロセスが含まれます。このメトリクスを提示する仮想化環境はすべてではなく、ほとんどの環境にはこのメトリクスがありません。単一のCPUコアの値は[0..1]の範囲にあります。すべてのCPUコアの値は、それらすべての合計として計算されます、[0..num cores]の範囲にあります。

### OSStealTimeCPU_*N*

仮想化環境で実行中のCPUが他のオペレーティングシステムに費やされた時間の割合。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなく、ホストマシン上のすべてのプロセスが含まれます。このメトリクスを提示する仮想化環境はすべてではなく、ほとんどの環境にはこのメトリクスがありません。単一のCPUコアの値は[0..1]の範囲にあります。すべてのCPUコアの値は、それらすべての合計として計算されます、[0..num cores]の範囲にあります。

### OSStealTimeNormalized

この値は`OSStealTime`と類似していますが、コア数で割られており、コア数に関係なく[0..1]の範囲で測定されます。これにより、コア数が非一様であっても、クラスタ内の複数のサーバー間でこのメトリクスの平均値を算出し、平均リソース使用率メトリクスを得ることができます。

### OSSystemTime

CPUコアがOSカーネル（システム）コードを実行していた時間の割合。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなく、ホストマシン上のすべてのプロセスが含まれています。単一のCPUコアの値は[0..1]の範囲内にあります。すべてのCPUコアの値は、これらすべてのコアの合計として計算されるため、[0..num cores]の範囲内になります。

### OSSystemTimeCPU_*N*

CPUコアがOSカーネル（システム）コードを実行していた時間の割合。これはシステム全体のメトリクスであり、Clickhouse-serverだけでなく、ホストマシン上のすべてのプロセスが含まれています。単一のCPUコアの値は[0..1]の範囲内にあります。すべてのCPUコアの値は、これらすべてのコアの合計として計算されるため、[0..num cores]の範囲内になります。

### OSSystemTimeNormalized

この値は`OSSystemTime`と類似しており、コア数で割られるため、コア数に関係なく[0..1]の範囲で測定されます。これにより、コア数が非一様であっても、クラスタ内の複数のサーバー間でのこのメトリクスの平均化が可能になり、平均リソース使用率メトリクスを得ることができます。

### OSThreadsRunnable

OSカーネルスケジューラが認識している'runnable'スレッドの総数。

### OSThreadsTotal

OSカーネルスケジューラが認識しているスレッドの総数。

### OSUptime

ホストサーバー（ClickHouseが実行されているマシン）の稼働時間（秒単位）。

### OSUserTime

CPUコアがユーザースペースコードを実行していた時間の割合。これはシステム全体のメトリクスであり、ホストマシン上のすべてのプロセスが含まれ、Clickhouse-serverだけではありません。これは、CPUが内部で要因（メモリロード、パイプラインの停止、分岐予測の失敗など）によって十分に利用されていない時間も含まれます。単一のCPUコアの値は[0..1]の範囲内にあります。すべてのCPUコアの値は、これらすべてのコアの合計として計算されるため、[0..num cores]の範囲内になります。

### OSUserTimeCPU_*N*

CPUコアがユーザースペースコードを実行していた時間の割合。これはシステム全体のメトリクスであり、ホストマシン上のすべてのプロセスが含まれ、Clickhouse-serverだけではありません。これは、CPUが内部で要因（メモリロード、パイプラインの停止、分岐予測の失敗など）によって十分に利用されていない時間も含まれます。単一のCPUコアの値は[0..1]の範囲内にあります。すべてのCPUコアの値は、これらすべてのコアの合計として計算されるため、[0..num cores]の範囲内になります。

### OSUserTimeNormalized

この値は`OSUserTime`と類似しており、コア数で割られており、コア数に関係なく[0..1]の範囲で測定されます。これにより、コア数が非一様であっても、クラスタ内の複数のサーバー間でこのメトリクスの平均値を測定し、平均リソース使用率メトリクスを得ることができます。

### PostgreSQLThreads

PostgreSQL互換プロトコルのサーバー内のスレッド数。

### QueryCacheBytes

クエリキャッシュの総サイズ（バイト単位）。

### QueryCacheEntries

クエリキャッシュ内の総エントリー数。

### ReplicasMaxAbsoluteDelay

レプリケートされたテーブル全体での最新のレプリケートされたパーツとまだレプリケートされていない最も新しいデータパーツの間の秒単位の最大差。非常に高い値はデータのないレプリカを示します。

### ReplicasMaxInsertsInQueue

レプリケートされたテーブル全体でのキュー（まだレプリケートされていない）にあるINSERT操作の最大数。

### ReplicasMaxMergesInQueue

レプリケートされたテーブル全体でのキュー（まだ適用されていない）にあるマージ操作の最大数。

### ReplicasMaxQueueSize

レプリケートされたテーブル全体でのキューサイズ（取得、マージなどの操作数）。

### ReplicasMaxRelativeDelay

レプリケートされたテーブル全体でのレプリカ遅延と同じテーブルの最新のレプリカの遅延との相対差の最大値。

### ReplicasSumInsertsInQueue

レプリケートされたテーブル全体でのキュー（まだレプリケートされていない）にあるINSERT操作の合計数。

### ReplicasSumMergesInQueue

レプリケートされたテーブル全体でのキュー（まだ適用されていない）にあるマージ操作の合計数。

### ReplicasSumQueueSize

レプリケートされたテーブル全体でのキューサイズ（取得、マージなどの操作数）。

### TCPThreads

TCPプロトコル（TLSなし）のサーバー上のスレッド数。

### Temperature_*N*

対応するデバイスの温度（℃）。センサーは非現実的な値を返すことがあります。ソース：`/sys/class/thermal`

### Temperature_*name*

対応するハードウェアモニタおよび対応するセンサーによって報告された温度（℃）。センサーは非現実的な値を返すことがあります。ソース：`/sys/class/hwmon`

### TotalBytesOfMergeTreeTables

MergeTreeファミリーのすべてのテーブルに保存されているバイト数（圧縮データとインデックスを含む）の合計。

### TotalPartsOfMergeTreeTables

MergeTreeファミリーのすべてのテーブル内のデータパーツの合計数。10,000を超える数値は、サーバーの起動時間に悪影響を及ぼし、パーティションキーの選択が不合理であることを示している可能性があります。

### TotalPrimaryKeyBytesInMemory

主キー値が使用するメモリの総量（バイト単位）（アクティブパーツのみが対象）。

### TotalPrimaryKeyBytesInMemoryAllocated

主キー値のために予約されたメモリの総量（バイト単位）（アクティブパーツのみが対象）。

### TotalRowsOfMergeTreeTables

MergeTreeファミリーのすべてのテーブルに保存されている行（レコード）の合計数。

### UncompressedCacheBytes

非圧縮キャッシュの総サイズ（バイト単位）。非圧縮キャッシュは通常、性能を向上させることはなく、ほとんどの場合避けるべきです。

### UncompressedCacheCells

非圧縮キャッシュ内のエントリーの総数。各エントリーはデータの非圧縮ブロックを表します。非圧縮キャッシュは通常、性能改善に役立たず、ほとんどの場合避けるべきです。

### Uptime

サーバーの稼働時間（秒単位）。これは、接続を受け入れる前のサーバー初期化に費やされた時間も含まれます。

### jemalloc.active

ローレベルメモリアロケータ（jemalloc）の内部メトリクス。詳細はhttps://jemalloc.net/jemalloc.3.htmlを参照。

### jemalloc.allocated

ローレベルメモリアロケータ（jemalloc）の内部メトリクス。詳細はhttps://jemalloc.net/jemalloc.3.htmlを参照。

### jemalloc.arenas.all.dirty_purged

ローレベルメモリアロケータ（jemalloc）の内部メトリクス。詳細はhttps://jemalloc.net/jemalloc.3.htmlを参照。

### jemalloc.arenas.all.muzzy_purged

ローレベルメモリアロケータ（jemalloc）の内部メトリクス。詳細はhttps://jemalloc.net/jemalloc.3.htmlを参照。

### jemalloc.arenas.all.pactive

ローレベルメモリアロケータ（jemalloc）の内部メトリクス。詳細はhttps://jemalloc.net/jemalloc.3.htmlを参照。

### jemalloc.arenas.all.pdirty

ローレベルメモリアロケータ（jemalloc）の内部メトリクス。詳細はhttps://jemalloc.net/jemalloc.3.htmlを参照。

### jemalloc.arenas.all.pmuzzy

ローレベルメモリアロケータ（jemalloc）の内部メトリクス。詳細はhttps://jemalloc.net/jemalloc.3.htmlを参照。

### jemalloc.background_thread.num_runs

ローレベルメモリアロケータ（jemalloc）の内部メトリクス。詳細はhttps://jemalloc.net/jemalloc.3.htmlを参照。

### jemalloc.background_thread.num_threads

ローレベルメモリアロケータ（jemalloc）の内部メトリクス。詳細はhttps://jemalloc.net/jemalloc.3.htmlを参照。

### jemalloc.background_thread.run_intervals

ローレベルメモリアロケータ（jemalloc）の内部メトリクス。詳細はhttps://jemalloc.net/jemalloc.3.htmlを参照。

### jemalloc.epoch

jemalloc（Jason Evansのメモリアロケータ）の統計の内部増分更新番号であり、他のすべての`jemalloc`メトリクスに使われます。

### jemalloc.mapped

ローレベルメモリアロケータ（jemalloc）の内部メトリクス。詳細はhttps://jemalloc.net/jemalloc.3.htmlを参照。

### jemalloc.metadata

ローレベルメモリアロケータ（jemalloc）の内部メトリクス。詳細はhttps://jemalloc.net/jemalloc.3.htmlを参照。

### jemalloc.metadata_thp

ローレベルメモリアロケータ（jemalloc）の内部メトリクス。詳細はhttps://jemalloc.net/jemalloc.3.htmlを参照。

### jemalloc.resident

ローレベルメモリアロケータ（jemalloc）の内部メトリクス。詳細はhttps://jemalloc.net/jemalloc.3.htmlを参照。

### jemalloc.retained

ローレベルメモリアロケータ（jemalloc）の内部メトリクス。詳細はhttps://jemalloc.net/jemalloc.3.htmlを参照。

### jemalloc.prof.active

ローレベルメモリアロケータ（jemalloc）の内部メトリクス。詳細はhttps://jemalloc.net/jemalloc.3.htmlを参照。

**関連項目**

- [モニタリング](../../operations/monitoring.md) — ClickHouseモニタリングの基本概念。
- [system.metrics](../../operations/system-tables/metrics.md#system_tables-metrics) — 即時計算されるメトリクスを含む。
- [system.events](../../operations/system-tables/events.md#system_tables-events) — 発生したイベント数を含む。
- [system.metric_log](../../operations/system-tables/metric_log.md#system_tables-metric_log) — テーブル`system.metrics`および`system.events`のメトリック値の履歴を含む。
