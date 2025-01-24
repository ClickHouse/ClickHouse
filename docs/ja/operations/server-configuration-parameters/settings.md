---
slug: /ja/operations/server-configuration-parameters/settings
sidebar_position: 57
sidebar_label: グローバルサーバー設定
description: このセクションには、セッションまたはクエリレベルで変更できないサーバー設定の説明が含まれています。
---

# グローバルサーバー設定

このセクションには、セッションまたはクエリレベルで変更できないサーバー設定の説明が含まれています。

これらの設定はClickHouseサーバーの`config.xml`ファイルに保存されています。

その他の設定は「[設定](../../operations/settings/index.md#session-settings-intro)」セクションに説明されています。

設定を学ぶ前に、[設定ファイル](../../operations/configuration-files.md#configuration_files)セクションを読み、置換の使用（`incl`および`optional`属性）に注意してください。

## allow_use_jemalloc_memory

jemallocメモリを使用することを許可します。

タイプ: Bool

デフォルト: 1

## asynchronous_heavy_metrics_update_period_s

非同期メトリックを更新する期間（秒単位）。

タイプ: UInt32

デフォルト: 120

## asynchronous_metrics_update_period_s

非同期メトリックを更新する期間（秒単位）。

タイプ: UInt32

デフォルト: 1

## auth_use_forwarded_address

プロキシを介して接続されたクライアントの認証に元のアドレスを使用します。

:::note
この設定は、転送されたアドレスが容易に偽装される可能性があるため、特に注意して使用する必要があります。したがって、そのような認証を受け入れるサーバーは、信頼できるプロキシ経由でのみアクセスされるべきです。
:::

タイプ: Bool

デフォルト: 0

## background_buffer_flush_schedule_pool_size

バックグラウンドでBufferエンジンテーブルのフラッシュ操作を実行するために使用される最大スレッド数。

タイプ: UInt64

デフォルト: 16

## background_common_pool_size

バックグラウンドで\*MergeTreeエンジンテーブルのさまざまな操作（主にガベージコレクション）を実行するために使用される最大スレッド数。

タイプ: UInt64

デフォルト: 8

## background_distributed_schedule_pool_size

分散送信を実行するために使用される最大スレッド数。

タイプ: UInt64

デフォルト: 16

## background_fetches_pool_size

バックグラウンドで\*MergeTreeエンジンテーブルの他のレプリカからデータパーツをフェッチするために使用される最大スレッド数。

タイプ: UInt64

デフォルト: 16

## background_merges_mutations_concurrency_ratio

スレッド数とバックグラウンドのマージおよびミューテーションを並行して実行することができる数の比率を設定します。例えば、この比率が2に等しく、`background_pool_size`が16に設定されている場合、ClickHouseはバックグラウンドで32個のマージを同時に実行できます。これは、バックグラウンド操作が一時停止して延期される可能性があるため可能です。小さなマージにより高い実行優先順位を与えるために必要です。この比率を増やすことは実行時にのみ可能です。これを下げるには、サーバーを再起動する必要があります。`background_pool_size`設定と同様に、後方互換性のために`default`プロファイルから`background_merges_mutations_concurrency_ratio`を適用することができます。

タイプ: Float

デフォルト: 2

## background_merges_mutations_scheduling_policy

バックグラウンドマージとミューテーションのスケジューリングを行う方針。可能な値は`round_robin`と`shortest_task_first`です。

バックグラウンドスレッドプールによって実行される次のマージまたはミューテーションを選択するために使用されるアルゴリズム。ポリシーは、サーバーを再起動せずに実行時に変更可能です。後方互換性のために`default`プロファイルから適用できます。

可能な値:

- 「round_robin」— すべての同時マージとミューテーションは、枯渇のない操作を保証するためにラウンドロビン順に実行されます。小さなマージは、単にマージするブロックが少ないため、より大きなものよりも早く完了します。
- 「shortest_task_first」— 常に小さなマージまたはミューテーションを実行します。マージとミューテーションは、結果のサイズに基づいて優先順位が割り当てられます。小さいサイズのマージは、大きなものに対して厳密に優先されます。このポリシーは、小さなパーツを可能な限り早くマージすることを保証しますが、INSERTsで過度に負荷がかかると、大きなマージが無期限に遅れる可能性があります。

タイプ: String

デフォルト: round_robin

## background_message_broker_schedule_pool_size

メッセージストリーミングのバックグラウンド操作を実行するために使用される最大スレッド数。

タイプ: UInt64

デフォルト: 16

## background_move_pool_size

バックグラウンドで\*MergeTreeエンジンテーブルのデータパーツを他のディスクまたはボリュームに移動するために使用される最大スレッド数。

タイプ: UInt64

デフォルト: 8

## background_pool_size

MergeTreeエンジンを持つテーブルのバックグラウンドマージとミューテーションを実行するスレッド数を設定します。このスレッド数を実行時に増やすことだけが可能です。スレッド数を減らすには、サーバーを再起動する必要があります。この設定を調整することで、CPUとディスクの負荷を管理します。プールサイズが小さいほどCPUとディスクリソースの使用が少なくなりますが、バックグラウンドプロセスの進行が遅くなり、最終的にはクエリのパフォーマンスに影響を与える可能性があります。

変更する前に、関連するMergeTreeの設定、たとえば`number_of_free_entries_in_pool_to_lower_max_size_of_merge`や`number_of_free_entries_in_pool_to_execute_mutation`なども参照してください。

タイプ: UInt64

デフォルト: 16

## background_schedule_pool_size

レプリカーテーブル、Kafkaストリーミング、DNSキャッシュの更新のための軽量な周期的操作を常時実行するために使用される最大スレッド数。

タイプ: UInt64

デフォルト: 512

## backup_threads

BACKUPリクエストを実行するための最大スレッド数。

タイプ: UInt64

デフォルト: 16

## backups_io_thread_pool_queue_size

Backups IO スレッドプールでスケジュールできるジョブの最大数。現在のS3バックアップロジックのため、キューを無制限（0）に保つことが推奨されます。

タイプ: UInt64

デフォルト: 0

## cache_size_to_ram_max_ratio

RAM最大比に対するキャッシュサイズを設定します。この設定は低メモリシステムでキャッシュサイズを減少させることができます。

タイプ: Double

デフォルト: 0.5

## concurrent_threads_soft_limit_num

リモートサーバーからデータを取得するスレッドを除く、すべてのクエリを実行するために許可される最大クエリ処理スレッド数。これは厳しい制限ではありません。制限に達した場合でも、クエリは少なくとも1つのスレッドを取得して実行され続けます。実行中にさらに多くのスレッドが使用可能になると、クエリは希望するスレッド数にスケールアップすることができます。

ゼロは無制限を意味します。

タイプ: UInt64

デフォルト: 0

## concurrent_threads_soft_limit_ratio_to_cores

concurrent_threads_soft_limit_numと同じですが、コアと比率で指定します。

タイプ: UInt64

デフォルト: 0

## default_database

デフォルトのデータベース名。

タイプ: String

デフォルト: default

## disable_internal_dns_cache

内部DNSキャッシュを無効にします。ClickHouseを頻繁に変更されるインフラストラクチャ（例: Kubernetes）で運用する際に推奨されます。

タイプ: Bool

デフォルト: 0

## dns_cache_max_entries

内部DNSキャッシュの最大エントリ数。

タイプ: UInt64

デフォルト: 10000

## dns_cache_update_period

内部DNSキャッシュの更新期間（秒単位）。

タイプ: Int32

デフォルト: 15

## dns_max_consecutive_failures

ClickHouseのDNSキャッシュからホストを削除する前の最大連続解決失敗数

タイプ: UInt32

デフォルト: 10

## index_mark_cache_policy

インデックスマークキャッシュポリシー名。

タイプ: String

デフォルト: SLRU

## index_mark_cache_size

インデックスマークのキャッシュサイズ。ゼロは無効を意味します。

:::note
この設定は実行時に変更可能で、すぐに適用されます。
:::

タイプ: UInt64

デフォルト: 0

## index_mark_cache_size_ratio

キャッシュの合計サイズに対するインデックスマークキャッシュ内の保護されたキューのサイズ。

タイプ: Double

デフォルト: 0.5

## index_uncompressed_cache_policy

インデックス非圧縮キャッシュポリシー名。

タイプ: String

デフォルト: SLRU

## index_uncompressed_cache_size

MergeTreeインデックスの非圧縮ブロックのキャッシュサイズ。ゼロは無効を意味します。

:::note
この設定は実行時に変更可能で、すぐに適用されます。
:::

タイプ: UInt64

デフォルト: 0

## index_uncompressed_cache_size_ratio

キャッシュの合計サイズに対するインデックス非圧縮キャッシュ内の保護されたキューのサイズ。

タイプ: Double

デフォルト: 0.5

## io_thread_pool_queue_size

IOスレッドプールのキューサイズ。ゼロは無制限を意味します。

タイプ: UInt64

デフォルト: 10000

## mark_cache_policy

マークキャッシュポリシー名。

タイプ: String

デフォルト: SLRU

## mark_cache_size

マーク（MergeTreeファミリーのテーブルのインデックス）のキャッシュサイズ。

:::note
この設定は実行時に変更可能で、すぐに適用されます。
:::

タイプ: UInt64

デフォルト: 5368709120

## mark_cache_size_ratio

キャッシュの合計サイズに対するマークキャッシュ内の保護されたキューのサイズ。

タイプ: Double

デフォルト: 0.5

## max_backup_bandwidth_for_server

サーバー上のすべてのバックアップに対する最大読み取り速度（バイト毎秒）。ゼロは無制限を意味します。

タイプ: UInt64

デフォルト: 0

## max_backups_io_thread_pool_free_size

バックアップIOスレッドプール内の**アイドル**スレッド数が`max_backup_io_thread_pool_free_size`を超える場合、ClickHouseはアイドル状態のスレッドが占めているリソースを解放し、プールサイズを減少させます。必要に応じてスレッドを再作成できます。

タイプ: UInt64

デフォルト: 0

## max_backups_io_thread_pool_size

BACKUPクエリのためのIO操作に使用される最大スレッド数

タイプ: UInt64

デフォルト: 1000

## max_concurrent_queries

同時に実行されるクエリの総数の制限。ゼロは無制限を意味します。挿入および選択クエリの制限、ユーザーの最大クエリ数の制限も考慮する必要があります。max_concurrent_insert_queries, max_concurrent_select_queries, max_concurrent_queries_for_all_usersも参照してください。ゼロは無制限を意味します。

:::note
この設定は実行時に変更可能で、すぐに適用されます。既に実行中のクエリは変更されません。
:::

タイプ: UInt64

デフォルト: 0

## max_concurrent_insert_queries

同時に実行される挿入クエリの総数の制限。ゼロは無制限を意味します。

:::note
この設定は実行時に変更可能で、すぐに適用されます。既に実行中のクエリは変更されません。
:::

タイプ: UInt64

デフォルト: 0

## max_concurrent_select_queries

同時に実行される選択クエリの総数の制限。ゼロは無制限を意味します。

:::note
この設定は実行時に変更可能で、すぐに適用されます。既に実行中のクエリは変更されません。
:::

タイプ: UInt64

デフォルト: 0

## max_waiting_queries

同時待機クエリの総数の制限。待機中のクエリの実行は、必要なテーブルが非同期に読み込まれる間（`async_load_databases`を参照）ブロックされます。待機中のクエリは、`max_concurrent_queries`、`max_concurrent_insert_queries`、`max_concurrent_select_queries`、`max_concurrent_queries_for_user`、`max_concurrent_queries_for_all_users`の制限をチェックする際にカウントされません。この修正は、サーバーの起動直後にこれらの制限に達することを避けるために行われます。ゼロは無制限を意味します。

:::note
この設定は実行時に変更可能で、すぐに適用されます。既に実行中のクエリは変更されません。
:::

タイプ: UInt64

デフォルト: 0

## max_connections

サーバーの最大接続数。

タイプ: Int32

デフォルト: 1024

## max_io_thread_pool_free_size

IOスレッドプールの最大フリーサイズ。

タイプ: UInt64

デフォルト: 0

## max_io_thread_pool_size

IO操作に使用される最大スレッド数

タイプ: UInt64

デフォルト: 100

## max_local_read_bandwidth_for_server

ローカル読み取りの最大速度（1秒あたりのバイト数）。ゼロは無制限を意味します。

タイプ: UInt64

デフォルト: 0

## max_local_write_bandwidth_for_server

ローカル書き込みの最大速度（1秒あたりのバイト数）。ゼロは無制限を意味します。

タイプ: UInt64

デフォルト: 0

## max_partition_size_to_drop

パーティション削除の制限。

[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)テーブルのサイズが`max_partition_size_to_drop`（バイト単位）を超える場合、[DROP PARTITION](../../sql-reference/statements/alter/partition.md#drop-partitionpart)クエリを使用してパーティションを削除することはできません。この設定は適用するためにClickHouseサーバーの再起動を必要としません。もう一つの制限を解除する方法は、`<clickhouse-path>/flags/force_drop_table`ファイルを作成することです。デフォルト値: 50 GB。値が0の場合、制限なしでパーティションを削除できます。

:::note
この制限は、テーブルの削除やテーブルの切り捨てを制限しません。詳細は[max_table_size_to_drop](#max-table-size-to-drop)を参照してください。
:::

## max_remote_read_network_bandwidth_for_server

ネットワーク経由でのデータ交換の最大速度（バイト毎秒）での読み取り。ゼロは無制限を意味します。

タイプ: UInt64

デフォルト: 0

## max_remote_write_network_bandwidth_for_server

ネットワーク経由でのデータ交換の最大速度（バイト毎秒）での書き込み。ゼロは無制限を意味します。

タイプ: UInt64

デフォルト: 0

## max_server_memory_usage

サーバー全体のメモリ使用量の制限。ゼロは無制限を意味します。

デフォルトの`max_server_memory_usage`値は`memory_amount * max_server_memory_usage_to_ram_ratio`として計算されます。

タイプ: UInt64

デフォルト: 0

## max_server_memory_usage_to_ram_ratio

max_server_memory_usageと同じですが、物理RAMに対する比率で設定します。低メモリシステムでのメモリ使用率を下げることができます。ゼロは無制限を意味します。

RAMとスワップが低いホストでは、`max_server_memory_usage_to_ram_ratio`を1より大きく設定する必要があります。

タイプ: Double

デフォルト: 0.9

## max_build_vector_similarity_index_thread_pool_size {#server_configuration_parameters_max_build_vector_similarity_index_thread_pool_size}

ベクトルインデックスの構築に使用する最大スレッド数。0はすべてのコアを意味します。

タイプ: UInt64

デフォルト: 16

## cgroups_memory_usage_observer_wait_time

cgroupsのしきい値でサーバーの最大許容メモリ消費を調整する間隔（秒単位）。 (`cgroup_memory_watcher_hard_limit_ratio`と`cgroup_memory_watcher_soft_limit_ratio` の設定を参照）。

cgroupオブザーバーを無効にするには、この値を`0`に設定します。

タイプ: UInt64

デフォルト: 15

## cgroup_memory_watcher_hard_limit_ratio

cgroupsに従ったサーバープロセスのメモリ消費に関する「ハード」しきい値を指定します。このしきい値を超えると、サーバーの最大メモリ消費がしきい値の値に調整されます。

`cgroups_memory_usage_observer_wait_time`および`cgroup_memory_watcher_soft_limit_ratio`の設定を参照してください。

タイプ: Double

デフォルト: 0.95

## cgroup_memory_watcher_soft_limit_ratio

cgroupsに従ったサーバープロセスのメモリ消費に関する「ソフト」しきい値を指定し、jemallocのアリーナをパージします。

`cgroups_memory_usage_observer_wait_time`と`cgroup_memory_watcher_hard_limit_ratio`の設定を参照してください。

タイプ: Double

デフォルト: 0.9

## max_table_size_to_drop

テーブル削除の制限。

[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)テーブルのサイズが`max_table_size_to_drop`（バイト単位）を超える場合、[DROP](../../sql-reference/statements/drop.md)クエリや[TRUNCATE](../../sql-reference/statements/truncate.md)クエリを使用して削除できません。

この設定は適用するためにClickHouseサーバーの再起動を必要としません。もう一つの制限を解除する方法は、`<clickhouse-path>/flags/force_drop_table`ファイルを作成することです。

デフォルト値: 50 GB。値が0の場合、すべてのテーブルを制限なしで削除できます。
**例**
``` xml
<max_table_size_to_drop>0</max_table_size_to_drop>
```

## max\_database\_num\_to\_warn {#max-database-num-to-warn}
アタッチされたデータベースの数が指定された値を超えた場合、clickhouseサーバーは`system.warnings`テーブルに警告メッセージを追加します。
デフォルト値: 1000

**例**

``` xml
<max_database_num_to_warn>50</max_database_num_to_warn>
```

## max\_table\_num\_to\_warn {#max-table-num-to-warn}
アタッチされたテーブルの数が指定された値を超えた場合、clickhouseサーバーは`system.warnings`テーブルに警告メッセージを追加します。
デフォルト値: 5000

**例**

``` xml
<max_table_num_to_warn>400</max_table_num_to_warn>
```

## max\_view\_num\_to\_warn {#max-view-num-to-warn}
アタッチされたビューの数が指定された値を超えた場合、clickhouseサーバーは`system.warnings`テーブルに警告メッセージを追加します。
デフォルト値: 10000

**例**

``` xml
<max_view_num_to_warn>400</max_view_num_to_warn>
```

## max\_dictionary\_num\_to\_warn {#max-dictionary-num-to-warn}
アタッチされたディクショナリの数が指定された値を超えた場合、clickhouseサーバーは`system.warnings`テーブルに警告メッセージを追加します。
デフォルト値: 1000

**例**

``` xml
<max_dictionary_num_to_warn>400</max_dictionary_num_to_warn>
```

## max\_part\_num\_to\_warn {#max-part-num-to-warn}
アクティブパーツの数が指定された値を超えた場合、clickhouseサーバーは`system.warnings`テーブルに警告メッセージを追加します。
デフォルト値: 100000

**例**

``` xml
<max_part_num_to_warn>400</max_part_num_to_warn>
```

## max\_table\_num\_to\_throw {#max-table-num-to-throw}
テーブルの数がこの値を超える場合、サーバーは例外をスローします。0は制限がないことを意味します。ビュー、リモートテーブル、ディクショナリ、システムテーブルはカウントされません。Atomic/Ordinary/Replicated/Lazyデータベースエンジンのテーブルだけがカウントされます。デフォルト値: 0

**例**
```xml
<max_table_num_to_throw>400</max_table_num_to_throw>
```

## max\_database\_num\_to\_throw {#max-table-num-to-throw}
データベースの数がこの値を超える場合、サーバーは例外をスローします。0は制限がないことを意味します。
デフォルト値: 0

**例**
```xml
<max_database_num_to_throw>400</max_database_num_to_throw>
```

## max_temporary_data_on_disk_size

外部集計、結合、またはソートに使用できる最大ストレージ量。
この制限を超えたクエリは例外で失敗します。ゼロは無制限を意味します。

`max_temporary_data_on_disk_size_for_user`や`max_temporary_data_on_disk_size_for_query`も参照。

タイプ: UInt64

デフォルト: 0

## max_thread_pool_free_size

グローバルスレッドプールの**アイドル**スレッド数が`max_thread_pool_free_size`を超える場合、ClickHouseは一部のスレッドが占めているリソースを解放し、プールサイズを減少させます。必要に応じてスレッドを再作成できます。

タイプ: UInt64

デフォルト: 1000

## max_thread_pool_size

OSから割り当てられ、クエリ実行やバックグラウンド操作に使用できる最大スレッド数。

タイプ: UInt64

デフォルト: 10000

## mmap_cache_size

マップされたファイルのキャッシュサイズ（バイト単位）を設定します。この設定は頻繁な開閉呼び出し（それに伴うページフォールトのため非常に高価）を回避し、複数のスレッドやクエリからのマッピングを再利用できます。設定値はマップされた領域の数です（通常はマップされたファイルの数に等しい）。マップされたファイル内のデータの量は、システムテーブル`system.metrics`と`system.metric_log`で`MMappedFiles`と`MMappedFileBytes`メトリクスでモニタリングできます。また、`system.asynchronous_metrics`と`system.asynchronous_metrics_log`で`MMapCacheCells`メトリクス、`system.events`、`system.processes`、`system.query_log`、`system.query_thread_log`、`system.query_views_log`で`CreatedReadBufferMMap`、`CreatedReadBufferMMapFailed`、`MMappedFileCacheHits`、`MMappedFileCacheMisses`イベントでもモニタリングできます。

注意: マップされたファイル内のデータ量はメモリを直接消費せず、クエリやサーバーのメモリ使用量にカウントされません。これはOSページキャッシュと同様に破棄可能なためです。キャッシュは、MergeTreeファミリーのテーブル内の古いパーツが削除されると自動的に削除され、`SYSTEM DROP MMAP CACHE`クエリで手動で削除することもできます。

:::note
この設定は実行時に変更可能で、すぐに適用されます。
:::

タイプ: UInt64

デフォルト: 1000

## restore_threads

RESTOREリクエストを実行するための最大スレッド数。

タイプ: UInt64

デフォルト: 16

## show_addresses_in_stack_traces

真に設定されている場合、スタックトレースでアドレスを表示します

タイプ: Bool

デフォルト: 1

## shutdown_wait_unfinished_queries

真に設定されている場合、ClickHouseはシャットダウン前に実行中のクエリが終了するのを待ちます。

タイプ: Bool

デフォルト: 0

## temporary_data_in_cache

このオプションにより、一時データは特定のディスクのキャッシュ内に保存されます。
このセクションでは、タイプが`cache`のディスク名を指定する必要があります。
この場合、キャッシュと一時データは同じスペースを共有し、一時データを作成するためにディスクキャッシュを退避させることができます。

:::note
一時データストレージを構成するために使用できるオプションは、`tmp_path` 、`tmp_policy` 、`temporary_data_in_cache` のいずれかの一つのみです。
:::

**例**

`local_disk`用のキャッシュと一時データは、ファイルシステム上の`/tiny_local_cache`に保存され、`tiny_local_cache`で管理されます。

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <local_disk>
                <type>local</type>
                <path>/local_disk/</path>
            </local_disk>

            <!-- highlight-start -->
            <tiny_local_cache>
                <type>cache</type>
                <disk>local_disk</disk>
                <path>/tiny_local_cache/</path>
                <max_size_rows>10M</max_size_rows>
                <max_file_segment_size>1M</max_file_segment_size>
                <cache_on_write_operations>1</cache_on_write_operations>
            </tiny_local_cache>
            <!-- highlight-end -->
        </disks>
    </storage_configuration>

    <!-- highlight-start -->
    <temporary_data_in_cache>tiny_local_cache</temporary_data_in_cache>
    <!-- highlight-end -->
</clickhouse>
```

タイプ: String

デフォルト:

## thread_pool_queue_size

グローバルスレッドプールでスケジュールできるジョブの最大数。キューサイズを増やすと、より多くのメモリが使用されます。この値を`max_thread_pool_size`と等しく保つことが推奨されます。ゼロは無制限を意味します。

タイプ: UInt64

デフォルト: 10000

## tmp_policy

一時データを含むストレージのポリシー。MergeTreeテーブルエンジンのドキュメントも参照してください。

:::note
- 一時データストレージを構成するために使用できるオプションは、`tmp_path` 、`tmp_policy` 、`temporary_data_in_cache` のいずれかの一つのみです。
- `move_factor`、`keep_free_space_bytes`、`max_data_part_size_bytes`などは無視されます。
- ポリシーには正確に*1つのボリューム*と*ローカル*ディスクを含むべきです。
:::

**例**

`/disk1`が満杯になると、一時データは`/disk2`に保存されます。

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <disk1>
                <path>/disk1/</path>
            </disk1>
            <disk2>
                <path>/disk2/</path>
            </disk2>
        </disks>

        <policies>
            <!-- highlight-start -->
            <tmp_two_disks>
                <volumes>
                    <main>
                        <disk>disk1</disk>
                        <disk>disk2</disk>
                    </main>
                </volumes>
            </tmp_two_disks>
            <!-- highlight-end -->
        </policies>
    </storage_configuration>

    <!-- highlight-start -->
    <tmp_policy>tmp_two_disks</tmp_policy>
    <!-- highlight-end -->
</clickhouse>
```
タイプ: String

デフォルト:

## uncompressed_cache_policy

非圧縮キャッシュポリシー名。

タイプ: String

デフォルト: SLRU

## uncompressed_cache_size

MergeTreeファミリーのテーブルエンジンによって使用される非圧縮データのキャッシュサイズ（バイト単位）。ゼロは無効を意味します。

サーバーには1つの共有キャッシュがあります。メモリはオンデマンドで割り当てられます。キャッシュは、オプションuse_uncompressed_cacheが有効になっている場合に使用されます。

非常に短いクエリに対して個別のケースで非圧縮キャッシュが有利です。

:::note
この設定は実行時に変更可能で、すぐに適用されます。
:::

タイプ: UInt64

デフォルト: 0

## uncompressed_cache_size_ratio

キャッシュの合計サイズに対する非圧縮キャッシュ内の保護されたキューのサイズ。

タイプ: Double

デフォルト: 0.5

## builtin_dictionaries_reload_interval {#builtin-dictionaries-reload-interval}

組み込みディクショナリを再読み込みする前の間隔（秒単位）。

ClickHouseは指定された秒数ごとに組み込みディクショナリを再読み込みします。これにより、サーバーを再起動することなくディクショナリを「オンザフライ」で編集することができます。

デフォルト値: 3600。

**例**

```xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

## compression {#server-settings-compression}

[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-エンジンテーブルのデータ圧縮設定。

:::note
ClickHouseを使い始めたばかりの場合は使用しないでください。
:::

設定テンプレート:

```xml
<compression>
    <case>
      <min_part_size>...</min_part_size>
      <min_part_size_ratio>...</min_part_size_ratio>
      <method>...</method>
      <level>...</level>
    </case>
    ...
</compression>
```

`<case>`フィールド:

- `min_part_size` – データパートの最小サイズ。
- `min_part_size_ratio` – データパートのサイズとテーブルサイズの比率。
- `method` – 圧縮方法。許可される値は: `lz4`, `lz4hc`, `zstd`,`deflate_qpl`.
- `level` – 圧縮レベル。[Codecs](../../sql-reference/statements/create/table.md#create-query-general-purpose-codecs)を参照。

複数の`<case>`セクションを設定できます。

条件が満たされた場合の動作:

- データパートが条件セットと一致する場合、ClickHouseは指定された圧縮方法を使用します。
- データパートが複数の条件セットと一致する場合、ClickHouseは最初に一致した条件セットを使用します。

データパートに対する条件が満たされない場合、ClickHouseは`lz4`圧縮を使用します。

**例**

```xml
<compression incl="clickhouse_compression">
    <case>
        <min_part_size>10000000000</min_part_size>
        <min_part_size_ratio>0.01</min_part_size_ratio>
        <method>zstd</method>
        <level>1</level>
    </case>
</compression>
```

## encryption {#server-settings-encryption}

[暗号化コーデック](../../sql-reference/statements/create/table.md#create-query-encryption-codecs)で使用するキーを取得するコマンドを構成します。キー（またはキー）は、環境変数に書き込むか、設定ファイルに設定する必要があります。

キーは16バイトの長さの16進数または文字列にできます。

**例**

設定から読み込み:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key>1234567812345678</key>
    </aes_128_gcm_siv>
</encryption_codecs>
```

:::note
設定ファイルにキーを格納することは推奨されません。これは安全ではありません。鍵を安全なディスク上の別の設定ファイルに移動し、それに対するシンボリックリンクを`config.d/`フォルダーに置くことを検討してください。
:::

16進数のキーを設定から読み込む場合:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex>00112233445566778899aabbccddeeff</key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

環境変数からキーを読み込む場合:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex from_env="ENVVAR"></key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

ここで`current_key_id`は暗号化のための現在のキーを設定し、指定されたすべてのキーが復号化に使用されることができます。

これらの方法のそれぞれは、複数のキーにも適用可能です:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
        <key_hex id="1" from_env="ENVVAR"></key_hex>
        <current_key_id>1</current_key_id>
    </aes_128_gcm_siv>
</encryption_codecs>
```

ここで`current_key_id`は暗号化のための現在のキーを示しています。

またユーザーは12バイト長のnonceを追加することができます（デフォルトの暗号化および復号化プロセスではゼロバイトで構成されるnonceが使用されます）:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce>012345678910</nonce>
    </aes_128_gcm_siv>
</encryption_codecs>
```

または16進数で設定できます:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce_hex>abcdefabcdef</nonce_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

上記のすべてのことは、`aes_256_gcm_siv`にも適用されます（ただしキーは32バイト長でなければなりません）。

## error_log {#error_log}

デフォルトでは無効です。

**有効化**

エラーログの履歴収集[`system.error_log`](../../operations/system-tables/error_log.md)を手動でオンにするには、次の内容で`/etc/clickhouse-server/config.d/error_log.xml`を作成してください:

```xml
<clickhouse>
    <error_log>
        <database>system</database>
        <table>error_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </error_log>
</clickhouse>
```

**無効化**

`error_log`設定を無効にするには、次の内容で`/etc/clickhouse-server/config.d/disable_error_log.xml`を作成してください:

```xml
<clickhouse>
<error_log remove="1" />
</clickhouse>
```

## custom_settings_prefixes {#custom_settings_prefixes}

[カスタム設定](../../operations/settings/index.md#custom_settings)のプレフィックスのリスト。プレフィックスはカンマで区切る必要があります。

**例**

```xml
<custom_settings_prefixes>custom_</custom_settings_prefixes>
```

**関連項目**

- [カスタム設定](../../operations/settings/index.md#custom_settings)

## core_dump {#core_dump}

コアダンプファイルサイズのソフトリミットを設定します。

可能な値:

- 正の整数。

デフォルト値: `1073741824` (1 GB)。

:::note
ハードリミットはシステムツールを使用して設定されます
:::

**例**

```xml
<core_dump>
    <size_limit>1073741824</size_limit>
</core_dump>
```

## database_atomic_delay_before_drop_table_sec {#database_atomic_delay_before_drop_table_sec}

削除されたテーブルを[UNDROP](/docs/ja/sql-reference/statements/undrop.md)ステートメントを使用して復元できるまでの遅延時間。この設定は`SYNC`モディファイア付きで`DROP TABLE`が実行された場合、無視されます。

デフォルト値: `480` (8分)。

## database_catalog_unused_dir_hide_timeout_sec {#database_catalog_unused_dir_hide_timeout_sec}

`store/`ディレクトリのガベージをクリーンアップするタスクのパラメータ。
ClickHouseサーバーで使用されていないサブディレクトリが
最後に`database_catalog_unused_dir_hide_timeout_sec`秒の間に変更されていない場合、タスクは
このディレクトリを「隠す」（すべてのアクセス権を削除することによって）ことを行います。
`store/`内にClickHouseサーバーが予期しないことも期待されるディレクトリにも適用されます。ゼロは「即時」を意味します。

デフォルト値: `3600` (1時間)。

## database_catalog_unused_dir_rm_timeout_sec {#database_catalog_unused_dir_rm_timeout_sec}

`store/`ディレクトリのガベージをクリーンアップするタスクのパラメータ。
ClickHouseサーバーで使用されておらず、以前に「隠された」
（`database_catalog_unused_dir_hide_timeout_sec`を参照）
最後に`database_catalog_unused_dir_rm_timeout_sec`秒の間に変更されていない場合、タスクはこのディレクトリを削除します。
`store/`内にClickHouseサーバーが予期しないことも期待されるディレクトリにも適用されます。ゼロは「決して」を意味します。

デフォルト値: `2592000` (30日)。

## database_catalog_unused_dir_cleanup_period_sec {#database_catalog_unused_dir_cleanup_period_sec}

`store/`ディレクトリのガベージをクリーンアップするタスクのパラメータ。
タスクのスケジューリング期間を設定します。ゼロは「決して」を意味します。

デフォルト値: `86400` (1日)。

## default_profile {#default-profile}

デフォルトの設定プロファイル。

設定プロファイルはパラメータ`user_config`で指定されたファイルに配置されます。

**例**

```xml
<default_profile>default</default_profile>
```

## default_replica_path {#default_replica_path}

ZooKeeper内のテーブルへのパス。

**例**

```xml
<default_replica_path>/clickhouse/tables/{uuid}/{shard}</default_replica_path>
```

## default_replica_name {#default_replica_name}

ZooKeeper内のレプリカ名。

**例**

```xml
<default_replica_name>{replica}</default_replica_name>
```

## dictionaries_config {#dictionaries_config}

ディクショナリの設定ファイルへのパス。

パス:

- 絶対パスまたはサーバー設定ファイルからの相対パスを指定してください。
- パスはワイルドカード\*と?を含むことができます。

「[ディクショナリ](../../sql-reference/dictionaries/index.md)」も参照してください。

**例**

```xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

## user_defined_executable_functions_config {#user_defined_executable_functions_config}

実行可能なユーザー定義関数の設定ファイルへのパス。

パス:

- 絶対パスまたはサーバー設定ファイルからの相対パスを指定してください。
- パスはワイルドカード\*と?を含むことができます。

「[実行可能ユーザー定義関数](../../sql-reference/functions/index.md#executable-user-defined-functions)」。も参照してください。

**例**

```xml
<user_defined_executable_functions_config>*_function.xml</user_defined_executable_functions_config>
```

## dictionaries_lazy_load {#dictionaries_lazy_load}

ディクショナリの遅延読み込み。

`true`の場合、各ディクショナリは初めて使用されたときに読み込まれます。読み込みが失敗した場合、ディクショナリを使用していた関数が例外をスローします。

`false`の場合、サーバーは起動時にすべてのディクショナリを読み込みます。
サーバーは接続を受ける前にすべてのディクショナリが読み込みを完了するまで起動を待ちます
（例外: `wait_dictionaries_load_at_startup`が`false`に設定されている場合 - 下記を参照）。

デフォルトは`true`です。

**例**

```xml
<dictionaries_lazy_load>true</dictionaries_lazy_load>
```

## format_schema_path {#format_schema_path}

入力データのスキームを含むディレクトリへのパス、例えば[CapnProto](../../interfaces/formats.md#capnproto)フォーマットのスキーマ。

**例**

```xml
  <!-- 様々な入力フォーマット用のスキーマファイルを含むディレクトリ。 -->
  <format_schema_path>format_schemas/</format_schema_path>
```

## graphite {#graphite}

データを[Graphite](https://github.com/graphite-project)に送信します。

設定:

- host – Graphiteサーバー。
- port – Graphiteサーバーのポート。
- interval – 送信の間隔（秒単位）。
- timeout – データ送信のタイムアウト（秒単位）。
- root_path – キーのプレフィックス。
- metrics – [system.metrics](../../operations/system-tables/metrics.md#system_tables-metrics)テーブルからのデータを送信します。
- events – 規定された期間のデルタデータを[system.events](../../operations/system-tables/events.md#system_tables-events)テーブルから送信します。
- events_cumulative – [system.events](../../operations/system-tables/events.md#system_tables-events)テーブルから累積データを送信します。
- asynchronous_metrics – [system.asynchronous_metrics](../../operations/system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics)テーブルからのデータを送信します。

複数の`<graphite>`句を設定することができます。例えば、異なるデータを異なる間隔で送信するために使用できます。

**例**

```xml
<graphite>
    <host>localhost</host>
    <port>42000</port>
    <timeout>0.1</timeout>
    <interval>60</interval>
    <root_path>one_min</root_path>
    <metrics>true</metrics>
    <events>true</events>
    <events_cumulative>false</events_cumulative>
    <asynchronous_metrics>true</asynchronous_metrics>
</graphite>
```

## graphite_rollup {#graphite-rollup}

Graphite用のデータのスリム化の設定。

詳細については、[GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md)を参照してください。

**例**

```xml
<graphite_rollup_example>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup_example>
```

## http_port/https_port {#http-porthttps-port}

サーバーにHTTP(s)で接続するためのポート。

`https_port`が指定されている場合は、[openSSL](#openssl)を設定する必要があります。

`http_port`が指定されている場合は、セキュリティ設定が無効になります（設定されていれば）。

**例**

```xml
<https_port>9999</https_port>
```

## http_server_default_response {#http_server_default_response}

ClickHouse HTTP(s)サーバーにアクセスした際にデフォルトで表示されるページ。
デフォルト値は「Ok.」（改行付き）。

**例**

`http://localhost: http_port`にアクセスすると`https://tabix.io/`を開く。

```xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

## hsts_max_age  {#hsts-max-age}

HSTSの期限切れ時間を秒単位で設定します。デフォルト値は0で、ClickHouseはHSTSを無効にしています。正の数を設定すると、HSTSが有効になり、max-ageは設定された数になります。

**例**

```xml
<hsts_max_age>600000</hsts_max_age>
```

## mlock_executable {#mlock_executable}

起動後にmlockallを実行し、最初のクエリのレイテンシーを低下させ、高IO負荷時にClickHouse実行ファイルがページアウトされるのを防ぎます。このオプションを有効にすることをお勧めしますが、起動時間が最大で数秒増加することになります。このパラメータは「CAP_IPC_LOCK」能力がないと機能しませんのでご注意ください。
**例**

```xml
<mlock_executable>false</mlock_executable>
```

## include_from {#include_from}

置換を含むファイルへのパス。XMLおよびYAML形式がサポートされています。

詳細については、「[設定ファイル](../../operations/configuration-files.md#configuration_files)」セクションを参照してください。

**例**

```xml
<include_from>/etc/metrica.xml</include_from>
```

## interserver_listen_host {#interserver-listen-host}

ClickHouseサーバー間でデータを交換できるホストの制限。
Keeperが使用されている場合、異なるKeeperインスタンス間の通信にも同じ制限が適用されます。
デフォルト値は`listen_host`設定と等しいです。

例:

```xml
<interserver_listen_host>::ffff:a00:1</interserver_listen_host>
<interserver_listen_host>10.0.0.1</interserver_listen_host>
```

## interserver_http_port {#interserver-http-port}

ClickHouseサーバー間でデータを交換するためのポート。

**例**

```xml
<interserver_http_port>9009</interserver_http_port>
```

## interserver_http_host {#interserver-http-host}

他のサーバーがこのサーバーにアクセスするために使用できるホスト名。

指定しない場合は、`hostname -f`コマンドと同じ方法で定義されます。

特定のネットワークインターフェイスからの切り離しに役立ちます。

**例**

```xml
<interserver_http_host>example.clickhouse.com</interserver_http_host>
```

## interserver_https_port {#interserver-https-port}

ClickHouseサーバー間のデータ交換用の`HTTPS`ポート。

**例**

```xml
<interserver_https_port>9010</interserver_https_port>
```

## interserver_https_host {#interserver-https-host}

`interserver_http_host`に類似しており、`HTTPS`を介して他のサーバーがこのサーバーにアクセスするために使用できるホスト名です。

**例**

```xml
<interserver_https_host>example.clickhouse.com</interserver_https_host>
```

## interserver_http_credentials {#server-settings-interserver-http-credentials}

[レプリケーション](../../engines/table-engines/mergetree-family/replication.md)の間に他のサーバーに接続するために使用されるユーザー名とパスワード。また、サーバーはこれらの資格情報を使用して他のレプリカを認証します。したがって、`interserver_http_credentials`はクラスター内のすべてのレプリカで同じでなければなりません。

デフォルトで`interserver_http_credentials`セクションが省略されている場合、レプリケーション中の認証は使用されません。

:::note
`interserver_http_credentials`設定は、ClickHouseクライアントの資格情報[設定](../../interfaces/cli.md#configuration_files)には関連しません。
:::

:::note
これらの資格情報は、`HTTP`および`HTTPS`を介したレプリケーションに共通です。
:::

セクションは次のパラメータを含みます:

- `user` — ユーザー名。
- `password` — パスワード。
- `allow_empty` — `true`の場合、資格情報が設定されていても、他のレプリカが認証なしで接続することが許可されます。`false`の場合、認証なしの接続は拒否されます。デフォルト値は`false`です。
- `old` — 資格情報を回転させる際に使用される古い`user`と`password`を含みます。複数の`old`セクションを指定可能です。

**資格情報のローテーション**

ClickHouseはすべてのレプリカを停止することなく動的なインタサーバー資格情報のローテーションをサポートします。資格情報は数ステップで変更できます。

認証を有効にするには、`interserver_http_credentials.allow_empty`を`true`に設定し、資格情報を追加します。これにより、認証ありおよびなしの接続が許可されます。

```xml
<interserver_http_credentials>
    <user>admin</user>
    <password>111</password>
    <allow_empty>true</allow_empty>
</interserver_http_credentials>
```

すべてのレプリカを構成した後、`allow_empty`を`false`に設定するか、この設定を削除します。これにより、新しい資格情報を使用した認証が必須となります。

既存の資格情報を変更するには、ユーザー名とパスワードを`interserver_http_credentials.old`セクションに移動し、新しい値で`user`と`password`を更新します。この時点で、サーバーは新しい資格情報を使用して他のレプリカに接続し、新しい資格情報または古い資格情報のいずれかで接続を受け入れます。

```xml
<interserver_http_credentials>
    <user>admin</user>
    <password>222</password>
    <old>
        <user>admin</user>
        <password>111</password>
    </old>
    <old>
        <user>temp</user>
        <password>000</password>
    </old>
</interserver_http_credentials>
```

新しい資格情報がすべてのレプリカに適用されたら、古い資格情報を削除できます。

## keep_alive_timeout {#keep-alive-timeout}

ClickHouseが接続を閉じる前に着信要求を待機する秒数。デフォルトは10秒です。

**例**

```xml
<keep_alive_timeout>10</keep_alive_timeout>
```

## max_keep_alive_requests {#max-keep-alive-requests}

1つのキープアライブ接続を介しての最大リクエスト数。クリックハウスサーバーによって接続が閉じられるまでの数。デフォルトは10000です。

**例**

```xml
<max_keep_alive_requests>10</max_keep_alive_requests>
```

## listen_host {#listen_host}

リクエストが来ることができるホストの制限。サーバーがすべてに応答するためには、`::`を指定してください。

例:

```xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

## listen_try {#listen_try}

IPv6またはIPv4ネットワークが使用できない場合でも、リスンしようとする際にサーバーが終了しないようにします。

例:

```xml
<listen_try>0</listen_try>
```

## listen_reuse_port {#listen_reuse_port}

複数のサーバーが同じアドレス:ポートでリスンすることを許可します。リクエストはOSによってランダムなサーバーにルーティングされます。この設定を有効にすることは推奨されません。

例:

```xml
<listen_reuse_port>0</listen_reuse_port>
```

## listen_backlog {#listen_backlog}

リスンソケットのバックログ（保留中の接続のキューサイズ）。

デフォルト値: `4096`（linux [5.4+](https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=19f92a030ca6d772ab44b22ee6a01378a8cb32d4)に準拠）。

通常、この値を変更する必要はありません。なぜなら:
- デフォルト値は十分大きく、
- クライアントの接続を受け入れるためにサーバーには個別のスレッドがあるからです。

したがって、ClickHouseサーバーの場合に`TcpExtListenOverflows`（`nstat`から）がゼロ以外で、このカウンタが成長しても、この値を増やす必要があるという意味ではありません。なぜなら:
- 通常、4096が十分でない場合、それは内部のClickHouseスケーリング問題を示しており、問題を報告する方が良いです。
- その時点でクライアントがいないか、切断されている可能性があるため、その後にサーバーがより多くの接続を処理できるという意味ではありません。

例:

```xml
<listen_backlog>4096</listen_backlog>
```

## logger {#logger}

ログメッセージの位置と形式。

キー:

- `level` – ログレベル。許容される値: `none`（ログをオフにします）、`fatal`、`critical`、`error`、`warning`、`notice`、`information`、
  `debug`、`trace`、`test`
- `log` – ログファイルへのパス。
- `errorlog` – エラーログファイルへのパス。
- `size` – 回転ポリシー: ログファイルの最大サイズ（バイト単位）。ログファイルサイズがこのしきい値を超えると、名前が変更されてアーカイブされ、新しいログファイルが作成されます。
- `count` – 回転ポリシー: ClickHouseが保持する過去のログファイルの最大数。
- `stream_compress` – ログメッセージをLZ4で圧縮します。有効にするには`1`または`true`を設定します。
- `console` – ログメッセージをログファイルに書き込まず、代わりにコンソールに表示します。有効にするには`1`または`true`を設定します。デフォルトは
  ClickHouseがデーモンモードで実行されていない場合に`1`です。それ以外の場合は`0`。
- `console_log_level` – コンソール出力のログレベル。`level`にデフォルトします。
- `formatting` – コンソール出力のログ形式。現在は`json`のみがサポートされています。
- `use_syslog` - ログ出力をsyslogにも転送します。
- `syslog_level` - syslogへのログのログレベル。
- `message_regexp` - この正規表現に一致するメッセージのみをログに記録します。デフォルトは`""`で、フィルタリングなしを示します。
- `message_regexp_negative` - この正規表現に一致しないメッセージのみをログに記録します。デフォルトは`""`で、フィルタリングなしを示します。

**ログ形式の指定子**

`log`および`errorLog`パス内のファイル名は、生成されるファイル名の下の形式指定子をサポートします（ディレクトリ部分ではサポートされていません）。

列「例」は`2023-07-06 18:32:07`時点での出力を示します。

| 指定子   | 説明                                                                                                         | 例                  |
|-------------|---------------------------------------------------------------------------------------------------------------------|--------------------------|
| %%          | リテラル%                                                                                                           | %                        |
| %n          | 改行文字                                                                                                           |                          |
| %t          | 水平タブ文字                                                                                            |                          |
| %Y          | 年を10進数で表したもの, 例: 2017                                                                                 | 2023                     |
| %y          | 年の最後の2桁（範囲 [00,99]）                                                           | 23                       |
| %C          | 年の最初の2桁（範囲 [00,99]）                                                          | 20                       |
| %G          | [ISO 8601週ベースの年](https://en.wikipedia.org/wiki/ISO_8601#Week_dates)の4桁, すなわち指定された週が含まれる年。通常%Vと組み合わせて使用します。  | 2023       |
| %g          | [ISO 8601週ベースの年](https://en.wikipedia.org/wiki/ISO_8601#Week_dates)の最後の2桁, すなわち指定された週が含まれる年。                         | 23         |
| %b          | 略称月名, 例: Oct（ロケール依存）                                                                 | Jul                      |
| %h          | %bの同義                                                                                                       | Jul                      |
| %B          | 完全月名, 例: October（ロケール依存）                                                                    | July                     |
| %m          | 月を10進数で表したもの（範囲 [01,12]）                                                                           | 07                       |
| %U          | 年のうちの週を10進数で表したもの（週の最初の日は日曜日） (範囲 [00,53])                          | 27                       |
| %W          | 年のうちの週を10進数で表したもの（週の最初の日は月曜日） (範囲 [00,53])                          | 27                       |
| %V          | ISO 8601週番号（範囲 [01,53]）                                                                                | 27                       |
| %j          | 年のうちの1日を10進数で表したもの（範囲 [001,366]）                                                               | 187                      |
| %d          | 月の1日を0パディングされた10進数で表したもの（範囲 [01,31]）。1桁はゼロで前方埋めされます。                 | 06                       |
| %e          | 月の1日をスペースでパディングされた10進数で表したもの（範囲 [1,31]）。1桁はスペースで前方埋めされます。              | &nbsp; 6                 |
| %a          | 略称曜日名, 例: Fri（ロケール依存）                                                               | Thu                      |
| %A          | 完全曜日名, 例: Friday（ロケール依存）                                                                   | Thursday                 |
| %w          | 週の日を整数で表したもの（0が日曜日） (範囲 [0-6])                                                          | 4                        |
| %u          | 週の日を10進数で表したもの（ISO 8601フォーマット, 月曜日が1） (範囲 [1-7])                                      | 4                        |
| %H          | 時間を10進数で表したもの (24時間制)（範囲 [00-23]）                                                             | 18                       |
| %I          | 時間を10進数で表したもの (12時間制)（範囲 [01-12]）                                                             | 06                       |
| %M          | 分を10進数で表したもの（範囲 [00-59]）                                                                          | 32                       |
| %S          | 秒を10進数で表したもの（範囲 [00-60]）                                                                          | 07                       |
| %c          | 標準日付と時間文字列、例: Sun Oct 17 04:41:13 2010（ロケール依存）                                     | Thu Jul  6 18:32:07 2023 |
| %x          | ローカライズされた日付表現（ロケール依存）                                                                        | 07/06/23                 |
| %X          | ローカライズされた時刻表現、例: 18:40:20 もしくは 6:40:20 PM（ロケール依存）                                       | 18:32:07                 |
| %D          | 短い形式の MM/DD/YY 日付、%m/%d/%y と同等                                                                         | 07/06/23                 |
| %F          | 短い形式の YYYY-MM-DD 日付、%Y-%m-%d と同等                                                                       | 2023-07-06               |
| %r          | ローカライズされた12時間形式の時刻（ロケール依存）                                                                 | 06:32:07 PM              |
| %R          | "%H:%M" に相当                                                                                                    | 18:32                    |
| %T          | "%H:%M:%S" に相当（ISO 8601時間形式）                                                                             | 18:32:07                 |
| %p          | ローカライズされた午前午後の指定（ロケール依存）                                                                   | PM                       |
| %z          | ISO 8601形式でUTCからのオフセット（例: -0430）、タイムゾーン情報がない場合は文字なし                              | +0800                    |
| %Z          | ロケール依存のタイムゾーン名または略称、タイムゾーン情報がない場合は文字なし                                     | Z AWST                   |

**例**

``` xml
<logger>
    <level>trace</level>
    <log>/var/log/clickhouse-server/clickhouse-server-%F-%T.log</log>
    <errorlog>/var/log/clickhouse-server/clickhouse-server-%F-%T.err.log</errorlog>
    <size>1000M</size>
    <count>10</count>
    <stream_compress>true</stream_compress>
</logger>
```

ログメッセージをコンソールにのみ出力するには:

``` xml
<logger>
    <level>information</level>
    <console>true</console>
</logger>
```

**レベル別のオーバーライド**

個々のログ名のログレベルをオーバーライドできます。例えば、"Backup" と "RBAC" の全てのメッセージをミュートにするには。

```xml
<logger>
    <levels>
        <logger>
            <name>Backup</name>
            <level>none</level>
        </logger>
        <logger>
            <name>RBAC</name>
            <level>none</level>
        </logger>
    </levels>
</logger>
```

**正規表現フィルタリング**

ログメッセージは `message_regexp` と `message_regexp_negative` を使用して正規表現でフィルタリングできます。これをレベルごとに、または全体として実行できます。グローバルとロガー固有のパターンが指定された場合は、グローバルパターンがオーバーライドされ（無視され）、ロガー固有のパターンのみが適用されます。この状況では、肯定パターンと否定パターンは独立して考慮されます。注意: この機能の使用は若干のパフォーマンス低下を引き起こす可能性があります。

```xml
<logger>
    <level>trace</level>
    <!-- グローバル: Traceメッセージをログしない -->
    <message_regexp_negative>.*Trace.*</message_regexp_negative>

    <message_regexps>
        <logger>
            <!-- executeQuery ロガーの場合、"Read" を含むメッセージのみをログし、"from" を含まない -->
            <name>executeQuery</name>
            <message_regexp>.*Read.*</message_regexp>
            <message_regexp_negative>.*from.*</message_regexp_negative>
        </logger>
    </message_regexps>
</logger>
```

### syslog

ログメッセージを syslog にも書き込むには:

``` xml
<logger>
    <use_syslog>1</use_syslog>
    <syslog>
        <address>syslog.remote:10514</address>
        <hostname>myhost.local</hostname>
        <facility>LOG_LOCAL6</facility>
        <format>syslog</format>
    </syslog>
</logger>
```

`<syslog>` のキー:

- `address` — syslog のアドレスの形式 `host\[:port\]` 。省略された場合、ローカルデーモンが使用されます。
- `hostname` — ログを送信するホストの名前。オプション。
- `facility` — syslog の [facility keyword](https://en.wikipedia.org/wiki/Syslog#Facility)。大文字で "LOG_" プレフィックス付きで指定する必要があります。例: `LOG_USER`, `LOG_DAEMON`, `LOG_LOCAL3` など。デフォルト値: `address` が指定された場合 `LOG_USER`, 指定されない場合 `LOG_DAEMON`。
- `format` - ログメッセージの形式。使用可能な値: `bsd` と `syslog.`

### ログ形式

コンソールログに出力されるログ形式を指定できます。現在、JSONのみがサポートされています。以下はJSONログの出力例です。

```json
{
  "date_time": "1650918987.180175",
  "thread_name": "#1",
  "thread_id": "254545",
  "level": "Trace",
  "query_id": "",
  "logger_name": "BaseDaemon",
  "message": "受信した信号 2",
  "source_file": "../base/daemon/BaseDaemon.cpp; virtual void SignalListener::run()",
  "source_line": "192"
}
```

JSONロギングサポートを有効にするには、以下のスニペットを使用します。

```xml
<logger>
    <formatting>
        <type>json</type>
        <names>
            <date_time>date_time</date_time>
            <thread_name>thread_name</thread_name>
            <thread_id>thread_id</thread_id>
            <level>level</level>
            <query_id>query_id</query_id>
            <logger_name>logger_name</logger_name>
            <message>message</message>
            <source_file>source_file</source_file>
            <source_line>source_line</source_line>
        </names>
    </formatting>
</logger>
```

**JSONログのキー名の変更**

キー名は、`<names>` タグ内のタグ値を変更することで修正できます。例えば、`DATE_TIME` を `MY_DATE_TIME` に変更するには、`<date_time>MY_DATE_TIME</date_time>` を使用します。

**JSONログのキーの省略**

ログプロパティは、プロパティをコメントアウトすることで省略できます。例えば、`query_id` をログに出力したくない場合、`<query_id>` タグをコメントアウトします。

## send_crash_reports {#send_crash_reports}

[Send] ClickHouse Core Developers Team via [Sentry](https://sentry.io)中の設定を使用して、クラッシュレポートをオプトインで送信する設定。

特にプレプロダクション環境での有効化が推奨されます。

この機能が正常に動作するには、サーバーがIPv4を介してパブリックインターネットにアクセスできる必要があります（執筆時点ではIPv6はSentryによってサポートされていません）。

キー:

- `enabled` – この機能を有効にするためのブールフラグ。デフォルトは `false`。クラッシュレポートの送信を許可するには `true` に設定します。
- `send_logical_errors` – `LOGICAL_ERROR` は `assert` のようなもので、ClickHouse のバグです。このブールフラグはこの例外をsentryに送信することを可能にします（デフォルト: `false`）。
- `endpoint` – クラッシュレポートを送信するためのsentryエンドポイントURLをオーバーライドできます。これには個別のSentryアカウントまたは自己ホスト済みのSentryインスタンスを使用できます。 [Sentry DSN](https://docs.sentry.io/error-reporting/quickstart/?platform=native#configure-the-sdk) 構文を使用します。
- `anonymize` - クラッシュレポートにサーバーホスト名を添付するのを回避します。
- `http_proxy` - クラッシュレポート送信用のHTTPプロキシを設定します。
- `debug` - Sentryクライアントをデバッグモードに設定します。
- `tmp_path` - 一時的なクラッシュレポート状態を保存するためのファイルシステムパス。
- `environment` - ClickHouseサーバーが動作している環境の任意の名前。各クラッシュレポートに記載されます。デフォルト値はClickHouseのバージョンに応じて `test` か `prod` です。

**推奨使用方法**

``` xml
<send_crash_reports>
    <enabled>true</enabled>
</send_crash_reports>
```

## macros {#macros}

レプリケートテーブルのパラメータ置換。

レプリケートテーブルが使用されていない場合は省略可能。

詳しくは、[Creating replicated tables](../../engines/table-engines/mergetree-family/replication.md#creating-replicated-tables) のセクションを参照してください。

**例**

``` xml
<macros incl="macros" optional="true" />
```

## replica_group_name {#replica_group_name}

データベースReplicatedのレプリカグループ名。

Replicatedデータベースによって作成されたクラスターは同じグループ内のレプリカで構成されます。
DDLクエリは同じグループ内のレプリカのみを待ちます。

デフォルトでは空です。

**例**

``` xml
<replica_group_name>backups</replica_group_name>
```

デフォルト値: ``。

## max_open_files {#max-open-files}

最大のオープンファイル数。

デフォルト: `maximum`。

このオプションは、macOSで `getrlimit()` 関数が不正確な値を返すために使用を推奨します。

**例**

``` xml
<max_open_files>262144</max_open_files>
```

## max_table_size_to_drop {#max-table-size-to-drop}

テーブル削除の制限。

[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) テーブルのサイズが `max_table_size_to_drop`（バイト単位）を超える場合、[DROP](../../sql-reference/statements/drop.md) クエリや [TRUNCATE](../../sql-reference/statements/truncate.md) クエリを使用して削除することはできません。

この設定は、ClickHouseサーバーの再起動なしで適用されます。制限を解除する別の方法は、 `<clickhouse-path>/flags/force_drop_table` ファイルを作成することです。

デフォルト値: 50 GB。

値が0の場合、すべてのテーブルを制限なしに削除できます。

**例**

``` xml
<max_table_size_to_drop>0</max_table_size_to_drop>
```

## max_partition_size_to_drop {#max-partition-size-to-drop}

パーティションの削除の制限。

[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) テーブルのサイズが `max_partition_size_to_drop`（バイト単位）を超える場合、[DROP PARTITION](../../sql-reference/statements/alter/partition.md#drop-partitionpart) クエリを使用してパーティションを削除できません。

この設定は、ClickHouseサーバーの再起動なしで適用されます。制限を解除するもう一つの方法は、 `<clickhouse-path>/flags/force_drop_table` ファイルを作成することです。

デフォルト値: 50 GB。

値が0の場合、パーティションを制限なしに削除できます。

:::note
この制限は、テーブルの削除やテーブルの切り捨てには制限を設けません。詳しくは、[max_table_size_to_drop](#max-table-size-to-drop) を参照してください。
:::

**例**

``` xml
<max_partition_size_to_drop>0</max_partition_size_to_drop>
```

## max_thread_pool_size {#max-thread-pool-size}

ClickHouseは、グローバルスレッドプールからクエリを処理するためのスレッドを使用します。クエリを処理するためのアイドルスレッドがない場合、プールに新しいスレッドが作成されます。`max_thread_pool_size` は、プール内の最大スレッド数を制限します。

可能な値:

- 正の整数。

デフォルト値: `10000`。

**例**

``` xml
<max_thread_pool_size>12000</max_thread_pool_size>
```

## max_thread_pool_free_size {#max-thread-pool-free-size}

グローバルスレッドプール内の**アイドル**スレッドの数が `max_thread_pool_free_size` を超える場合、ClickHouseは一部のスレッドが占有するリソースを解放し、プールサイズを減少させます。必要に応じてスレッドを再作成できます。

可能な値:

- 正の整数。

デフォルト値: `1000`。

**例**

``` xml
<max_thread_pool_free_size>1200</max_thread_pool_free_size>
```

## thread_pool_queue_size {#thread-pool-queue-size}

グローバルスレッドプールでスケジュールできるジョブの最大数。キューサイズを増やすと、より大きなメモリ使用につながります。この値を [max_thread_pool_size](#max-thread-pool-size) と同じにしておくことが推奨されます。

可能な値:

- 正の整数。
- 0 — 制限なし。

デフォルト値: `10000`。

**例**

``` xml
<thread_pool_queue_size>12000</thread_pool_queue_size>
```

## max_io_thread_pool_size {#max-io-thread-pool-size}

ClickHouseはIO操作を行うためにIOスレッドプールのスレッドを使用します（例: S3とのインタラクション）。`max_io_thread_pool_size` はプール内のスレッドの最大数を制限します。

可能な値:

- 正の整数。

デフォルト値: `100`。

## max_io_thread_pool_free_size {#max-io-thread-pool-free-size}

IOスレッドプール内の**アイドル**スレッドの数が `max_io_thread_pool_free_size` を超える場合、ClickHouseはアイドルスレッドが占有するリソースを解放し、プールサイズを減少させます。必要に応じてスレッドを再作成できます。

可能な値:

- 正の整数。

デフォルト値: `0`。

## io_thread_pool_queue_size {#io-thread-pool-queue-size}

IOスレッドプールでスケジュールできるジョブの最大数。

可能な値:

- 正の整数。
- 0 — 制限なし。

デフォルト値: `10000`。

## max_backups_io_thread_pool_size {#max-backups-io-thread-pool-size}

ClickHouseはS3バックアップのIO操作を行うためにバックアップIOスレッドプールのスレッドを使用します。`max_backups_io_thread_pool_size` はプール内のスレッドの最大数を制限します。

可能な値:

- 正の整数。

デフォルト値: `1000`。

## max_backups_io_thread_pool_free_size {#max-backups-io-thread-pool-free-size}

バックアップIOスレッドプール内の**アイドル**スレッドの数が `max_backup_io_thread_pool_free_size` を超える場合、ClickHouseはアイドルスレッドが占有するリソースを解放し、プールサイズを減少させます。必要に応じてスレッドを再作成できます。

可能な値:

- 正の整数。
- ゼロ。

デフォルト値: `0`。

## backups_io_thread_pool_queue_size {#backups-io-thread-pool-queue-size}

バックアップIOスレッドプールでスケジュールできるジョブの最大数。現在のS3バックアップロジックのため、このキューを無制限にしておくことが推奨されます。

可能な値:

- 正の整数。
- 0 — 制限なし。

デフォルト値: `0`。

## background_pool_size {#background_pool_size}

MergeTreeエンジンを使用するテーブルのバックグラウンドマージと変異を実行するスレッド数を設定します。この設定はClickHouseサーバーの起動時に `default` プロファイル設定から後方互換性のためにも適用できます。ランタイム中にスレッド数を増やすことしかできません。スレッド数を減らすにはサーバーを再起動する必要があります。この設定を調整することで、CPUとディスクの負荷を管理できます。プールサイズが小さいほど、CPUとディスクのリソースをより少なく利用しますが、バックグラウンドプロセスはより遅く進行し、最終的にはクエリ性能に影響を与える可能性があります。

変更する前に、[number_of_free_entries_in_pool_to_lower_max_size_of_merge](../../operations/settings/merge-tree-settings.md#number-of-free-entries-in-pool-to-lower-max-size-of-merge) や [number_of_free_entries_in_pool_to_execute_mutation](../../operations/settings/merge-tree-settings.md#number-of-free-entries-in-pool-to-execute-mutation) などの関連するMergeTree設定も確認してください。

可能な値:

- 任意の正の整数。

デフォルト値: 16。

**例**

```xml
<background_pool_size>16</background_pool_size>
```

## background_merges_mutations_concurrency_ratio {#background_merges_mutations_concurrency_ratio}

スレッド数と同時に実行できるバックグラウンドマージと変異の数との比率を設定します。例えば、比率が2で `background_pool_size` が16に設定されている場合、ClickHouseは32のバックグラウンドマージを同時に実行できます。これは、バックグラウンド操作が一時停止され延期される可能性があるからです。これにより小さなマージにより高い実行優先度が与えられる必要があります。この比率をランタイム中に増やすことしかできません。比率を下げるにはサーバーを再起動する必要があります。`background_pool_size` 設定と同様に、`background_merges_mutations_concurrency_ratio` は `default` プロファイルから適用でき、後方互換性を保ちます。

可能な値:

- 任意の正の整数。

デフォルト値: 2。

**例**

```xml
<background_merges_mutations_concurrency_ratio>3</background_merges_mutations_concurrency_ratio>
```

## merges_mutations_memory_usage_soft_limit {#merges_mutations_memory_usage_soft_limit}

マージと変異の操作を実行するために使用が許可されているRAMの制限を設定します。ゼロは無制限を意味します。ClickHouseがこの制限に達すると、新しいバックグラウンドマージまたは変異操作をスケジュールしませんが、すでにスケジュールされているタスクは実行し続けます。

可能な値:

- 任意の正の整数。

**例**

```xml
<merges_mutations_memory_usage_soft_limit>0</merges_mutations_memory_usage_soft_limit>
```

## merges_mutations_memory_usage_to_ram_ratio {#merges_mutations_memory_usage_to_ram_ratio}

デフォルトの `merges_mutations_memory_usage_soft_limit` 値は `memory_amount * merges_mutations_memory_usage_to_ram_ratio` として計算されます。

デフォルト値: `0.5`。

**関連項目**

-   [max_memory_usage](../../operations/settings/query-complexity.md#settings_max_memory_usage)
-   [merges_mutations_memory_usage_soft_limit](#merges_mutations_memory_usage_soft_limit)

## async_load_databases {#async_load_databases}

データベースとテーブルの非同期ロード。

`true` の場合、`Ordinary`、`Atomic`、`Replicated` エンジンを持つすべての非システムデータベースがClickHouseサーバーの起動後に非同期でロードされます。`system.asynchronous_loader` テーブル、`tables_loader_background_pool_size`、`tables_loader_foreground_pool_size` サーバー設定を参照してください。まだロードされていないテーブルにアクセスしようとするクエリは、正確にこのテーブルが起動されるまで待機します。ロードジョブが失敗した場合、そのクエリはエラーを再スローします（`async_load_databases = false` の場合にサーバー全体をシャットダウンする代わりに）。少なくとも1つのクエリによって待機されているテーブルは高い優先度でロードされます。データベースに関するDDLクエリは正確にそのデータベースが起動するまで待機します。待機クエリの総数を制限するために `max_waiting_queries` 設定を考慮してください。

`false` の場合、サーバーが起動した際にすべてのデータベースがロードされます。

デフォルトは `false` です。

**例**

``` xml
<async_load_databases>true</async_load_databases>
```

## async_load_system_database {#async_load_system_database}

システムテーブルの非同期ロード。`system` データベース内に大量のログテーブルやパーツがある場合に便利です。`async_load_databases` 設定とは独立しています。

`true` に設定すると、`Ordinary`、`Atomic`、`Replicated` エンジンを持つすべてのシステムデータベースがClickHouseサーバーの起動後に非同期でロードされます。`system.asynchronous_loader` テーブル、`tables_loader_background_pool_size` 、 `tables_loader_foreground_pool_size` サーバー設定を参照してください。まだロードされていないシステムテーブルにアクセスしようとするクエリは、正確にこのテーブルが起動されるまで待機します。少なくとも1つのクエリによって待機されているテーブルは高い優先度でロードされます。また、待機クエリの総数を制限するために `max_waiting_queries` 設定を考慮してください。

`false` の場合、システムデータベースはサーバーの開始前にロードされます。

デフォルトは `false` です。

**例**

``` xml
<async_load_system_database>true</async_load_system_database>
```

## tables_loader_foreground_pool_size {#tables_loader_foreground_pool_size}

フォアグラウンドプールでロードジョブを実行するスレッド数を設定します。フォアグラウンドプールは、サーバーがポートのリッスンを開始する前にテーブルを同期的にロードし、テーブルのロードを待機しているクエリを処理するために使用されます。フォアグラウンドプールはバックグラウンドプールよりも優先度が高いです。フォアグラウンドプールでジョブが実行中の場合、バックグラウンドプールでジョブが開始されません。

可能な値:

-   任意の正の整数。
-   ゼロ。利用可能なすべてのCPUを使用します。

デフォルト値: 0。

## tables_loader_background_pool_size {#tables_loader_background_pool_size}

バックグラウンドプールで非同期ロードジョブを実行するスレッド数を設定します。バックグラウンドプールは、サーバーの開始後に、テーブルのロードが待機されていない場合にテーブルを非同期でロードするために使用されます。テーブルが多い場合、バックグラウンドプールのスレッド数を低く保つことが利益をもたらす可能性があります。この場合、同時クエリ実行のためのCPUリソースが確保されます。

可能な値:

-   任意の正の整数。
-   ゼロ。利用可能なすべてのCPUを使用します。

デフォルト値: 0。

## merge_tree {#merge_tree}

[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) テーブルの微調整。

詳しくは、MergeTreeSettings.h ヘッダーファイルを参照してください。

**例**

``` xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

## metric_log {#metric_log}

デフォルトでは無効です。

**有効化**

メトリクス履歴収集を手動でオンにするには [`system.metric_log`](../../operations/system-tables/metric_log.md) を使用し、次の内容で `/etc/clickhouse-server/config.d/metric_log.xml` を作成します。

``` xml
<clickhouse>
    <metric_log>
        <database>system</database>
        <table>metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </metric_log>
</clickhouse>
```

**無効化**

`metric_log` 設定を無効にするには、次の内容で `/etc/clickhouse-server/config.d/disable_metric_log.xml` を作成します。

``` xml
<clickhouse>
<metric_log remove="1" />
</clickhouse>
```

## replicated_merge_tree {#replicated_merge_tree}

[ReplicatedMergeTree](../../engines/table-engines/mergetree-family/mergetree.md) テーブルの微調整。

この設定は優先順位が高いです。

詳しくは、MergeTreeSettings.h ヘッダーファイルを参照してください。

**例**

``` xml
<replicated_merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</replicated_merge_tree>
```

## openSSL {#openssl}

SSLクライアント/サーバー設定。

SSLのサポートは `libpoco` ライブラリにより提供されます。利用可能な設定オプションは [SSLManager.h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h) に説明されています。デフォルト値は [SSLManager.cpp](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/src/SSLManager.cpp) にあります。

サーバー/クライアント設定のキー:

- privateKeyFile – PEM証明書の秘密鍵ファイルへのパス。このファイルには同時に鍵と証明書が含まれることがあります。
- certificateFile – PEM形式のクライアント/サーバー証明書ファイルへのパス。`privateKeyFile` が証明書を含む場合は省略可能です。
- caConfig (デフォルト: なし) – 信頼できるCA証明書を含むファイルまたはディレクトリへのパス。これがファイルを指す場合、PEM形式であり複数のCA証明書を含むことが可能です。ディレクトリを指す場合、CA証明書ごとに一つの.pemファイルを含む必要があります。ファイル名はCAサブジェクト名のハッシュ値で検索されます。詳細は [SSL_CTX_load_verify_locations](https://www.openssl.org/docs/man3.0/man3/SSL_CTX_load_verify_locations.html) のマニュアルページを参照してください。
- verificationMode (デフォルト: relaxed) – ノードの証明書を確認する方法。詳細は [Context](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h) クラスの説明を参照してください。使用可能な値: `none`, `relaxed`, `strict`, `once`.
- verificationDepth (デフォルト: 9) – 検証チェーンの最大長。証明書チェーンの長さが設定値を超えると検証は失敗します。
- loadDefaultCAFile (デフォルト: true) – OpenSSLのビルトインCA証明書を使用するかどうか。ClickHouseはビルトインCA証明書がファイル `/etc/ssl/cert.pem`（もしくはディレクトリ `/etc/ssl/certs`）にある、または環境変数 `SSL_CERT_FILE`（もしくは `SSL_CERT_DIR`）で指定されたファイル（もしくはディレクトリ）にあると見なします。
- cipherList (デフォルト: `ALL:!ADH:!LOW:!EXP:!MD5:!3DES:@STRENGTH`) - 対応するOpenSSLの暗号化方式。
- cacheSessions (デフォルト: false) – セッションのキャッシュを有効または無効にします。`sessionIdContext` と組み合わせて使用する必要があります。受容可能な値: `true`, `false`.
- sessionIdContext (デフォルト: `${application.name}`) – サーバーが各生成された識別子に追加するランダム文字列のユニークセット。文字列の長さは `SSL_MAX_SSL_SESSION_ID_LENGTH` を超えてはなりません。このパラメータは、セッションをキャッシュするサーバーとクライアントがキャッシュを要求する場合に役立ちます。デフォルト値: `${application.name}`.
- sessionCacheSize (デフォルト: [1024\*20](https://github.com/ClickHouse/boringssl/blob/master/include/openssl/ssl.h#L1978)) – サーバーがキャッシュするセッションの最大数。値が0の場合、無制限になります。
- sessionTimeout (デフォルト: [2h](https://github.com/ClickHouse/boringssl/blob/master/include/openssl/ssl.h#L1926)) – サーバーでセッションをキャッシュする時間。
- extendedVerification (デフォルト: false) – 有効化されている場合、証明書のCNまたはSANがピアのホスト名と一致することを確認します。
- requireTLSv1 (デフォルト: false) – TLSv1接続を要求します。受容可能な値: `true`, `false`.
- requireTLSv1_1 (デフォルト: false) – TLSv1.1接続を要求します。受容可能な値: `true`, `false`.
- requireTLSv1_2 (デフォルト: false) – TLSv1.2接続を要求します。受容可能な値: `true`, `false`.
- fips (デフォルト: false) – OpenSSLのFIPSモードをアクティブにします。ライブラリのOpenSSLバージョンがFIPSをサポートしている場合に対応しています。
- privateKeyPassphraseHandler (デフォルト: `KeyConsoleHandler`)– 秘密鍵にアクセスするためのパスフレーズを要求するクラス（PrivateKeyPassphraseHandlerサブクラス）。例: `<privateKeyPassphraseHandler>`, `<name>KeyFileHandler</name>`, `<options><password>test</password></options>`, `</privateKeyPassphraseHandler>`.
- invalidCertificateHandler (デフォルト: `RejectCertificateHandler`) – 無効な証明書を確認するためのクラス（CertificateHandlerサブクラス）。例: `<invalidCertificateHandler> <name>RejectCertificateHandler</name> </invalidCertificateHandler>`.
- disableProtocols (デフォルト: "") – 使用が許可されていないプロトコル。
- preferServerCiphers (デフォルト: false) – クライアントに対してサーバー暗号化方式を優先します。

**設定例**

``` xml
<openSSL>
    <server>
        <!-- openssl req -subj "/CN=localhost" -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout /etc/clickhouse-server/server.key -out /etc/clickhouse-server/server.crt -->
        <certificateFile>/etc/clickhouse-server/server.crt</certificateFile>
        <privateKeyFile>/etc/clickhouse-server/server.key</privateKeyFile>
        <!-- openssl dhparam -out /etc/clickhouse-server/dhparam.pem 4096 -->
        <dhParamsFile>/etc/clickhouse-server/dhparam.pem</dhParamsFile>
        <verificationMode>none</verificationMode>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
    </server>
    <client>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
        <!-- セルフサイン用: <verificationMode>none</verificationMode> -->
        <invalidCertificateHandler>
            <!-- セルフサイン用: <name>AcceptCertificateHandler</name> -->
            <name>RejectCertificateHandler</name>
        </invalidCertificateHandler>
    </client>
</openSSL>
```

## part_log {#part-log}

[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) に関連するイベントのロギング。たとえば、データの追加やマージ。このログを使用してマージアルゴリズムをシミュレートし、その特性を比較できます。また、マージプロセスを視覚化できます。

ログは [system.part_log](../../operations/system-tables/part_log.md#system_tables-part-log) テーブルに記録され、別のファイルには記録されません。このテーブルの名前は `table` パラメーターで構成可能です（以下を参照）。

ロギングを構成するには、以下のパラメーターを使用します:

- `database` – データベース名。
- `table` – システムテーブル名。
- `partition_by` — システムテーブルの [カスタムパーティションキー](../../engines/table-engines/mergetree-family/custom-partitioning-key.md)。`engine` が定義されている場合は使用できません。
- `order_by` - システムテーブルの [カスタムソートキー](../../engines/table-engines/mergetree-family/mergetree.md#order_by)。`engine` が定義されている場合は使用できません。
- `engine` - システムテーブルの [MergeTreeエンジン定義](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)。`partition_by` または `order_by` が定義されている場合は使用できません。
- `flush_interval_milliseconds` – メモリ中のバッファからテーブルにデータをフラッシュするための間隔。
- `max_size_rows` – ログの最大行数。未フラッシュのログ数が最大サイズに達すると、ログはディスクにダンプされます。
デフォルト: 1048576。
- `reserved_size_rows` –  ログのために事前に割り当てられたメモリサイズ（行数）。
デフォルト: 8192。
- `buffer_size_rows_flush_threshold` – 行数のしきい値で、これに達すると、ログをバックグラウンドでディスクにフラッシュします。
デフォルト: `max_size_rows / 2`。
- `flush_on_crash` - クラッシュ時にログをディスクにダンプすべきかどうかを示す。
デフォルト: false。
- `storage_policy` – テーブルで使用するストレージポリシーの名前（オプション）
- `settings` - MergeTreeの動作を制御する [追加パラメーター](../../engines/table-engines/mergetree-family/mergetree.md/#settings)（オプション）。

**例**

``` xml
<part_log>
    <database>system</database>
    <table>part_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</part_log>
```

## path {#path}

データを含むディレクトリへのパス。

:::note
トレイリングスラッシュは必須です。
:::

**例**

``` xml
<path>/var/lib/clickhouse/</path>
```

## query_log {#query-log}

[log_queries=1](../../operations/settings/settings.md) 設定を使用して受信したクエリのロギング設定。

クエリは、 [system.query_log](../../operations/system-tables/query_log.md#system_tables-query_log) テーブルに記録され、別のファイルには記録されません。テーブルの名前を以下の `table` パラメーターで変更できます。

ロギングを構成するには、以下のパラメーターを使用します:

- `database` – データベース名。
- `table` – クエリが記録されるシステムテーブルの名前。
- `partition_by` — システムテーブルの [カスタムパーティションキー](../../engines/table-engines/mergetree-family/custom-partitioning-key.md)。`engine` が定義されている場合は使用できません。
- `order_by` - システムテーブルの [カスタムソートキー](../../engines/table-engines/mergetree-family/mergetree.md#order_by)。`engine` が定義されている場合は使用できません。
- `engine` - システムテーブルの [MergeTreeエンジン定義](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)。`partition_by` または `order_by` が定義されている場合は使用できません。
- `flush_interval_milliseconds` – メモリ中のバッファからテーブルにデータをフラッシュするための間隔。
- `max_size_rows` – ログの最大行数。未フラッシュのログ数が最大サイズに達すると、ログはディスクにダンプされます。
デフォルト: 1048576。
- `reserved_size_rows` – ログのために事前に割り当てられたメモリサイズ（行数）。
デフォルト: 8192。
- `buffer_size_rows_flush_threshold` – 行数のしきい値で、これに達すると、ログをバックグラウンドでディスクにフラッシュします。
デフォルト: `max_size_rows / 2`。
- `flush_on_crash` - クラッシュ時にログをディスクにダンプするかどうかの指示。
デフォルト: false。
- `storage_policy` – テーブルで使用するストレージポリシーの名前（オプション）
- `settings` - MergeTreeの動作を制御する [追加パラメーター](../../engines/table-engines/mergetree-family/mergetree.md/#settings)（オプション）。

テーブルが存在しない場合、ClickHouseはそれを作成します。ClickHouseサーバーが更新された際にクエリログの構造が変更された場合、古い構造のテーブルはリネームされ、新しいテーブルが自動的に作成されます。

**例**

``` xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_log>
```

# query_metric_log {#query_metric_log}

デフォルトでは無効です。

**有効化**

メトリクス履歴収集を手動でオンにするには [`system.query_metric_log`](../../operations/system-tables/query_metric_log.md) を使用し、次の内容で `/etc/clickhouse-server/config.d/query_metric_log.xml` を作成します。

``` xml
<clickhouse>
    <query_metric_log>
        <database>system</database>
        <table>query_metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </query_metric_log>
</clickhouse>
```

**無効化**

`query_metric_log` 設定を無効にするには、次の内容で `/etc/clickhouse-server/config.d/disable_query_metric_log.xml` を作成します。

``` xml
<clickhouse>
<query_metric_log remove="1" />
</clickhouse>
```

## query_cache {#server_configuration_parameters_query-cache}

[クエリキャッシュ](../query-cache.md) の設定。

次の設定が使用可能です:

- `max_size_in_bytes`: キャッシュの最大サイズ（バイト単位）。0はクエリキャッシュが無効であることを意味します。デフォルト値: `1073741824`（1 GiB）。
- `max_entries`: キャッシュに保存される `SELECT` クエリ結果の最大数。デフォルト値: `1024`。
- `max_entry_size_in_bytes`: キャッシュに保存される `SELECT` クエリ結果が持つ可能な最大サイズ（バイト単位）。デフォルト値: `1048576`（1 MiB）。
- `max_entry_size_in_rows`: キャッシュに保存される `SELECT` クエリ結果が持つ可能な最大行数。デフォルト値: `30000000`（3000万）。

変更された設定は直ちに適用されます。

:::note
クエリキャッシュのデータはDRAMに割り当てられます。メモリが不足している場合は、`max_size_in_bytes` の値を小さく設定するか、クエリキャッシュを完全に無効にしてください。
:::

**例**

```xml
<query_cache>
    <max_size_in_bytes>1073741824</max_size_in_bytes>
    <max_entries>1024</max_entries>
    <max_entry_size_in_bytes>1048576</max_entry_size_in_bytes>
    <max_entry_size_in_rows>30000000</max_entry_size_in_rows>
</query_cache>
```

## query_thread_log {#query_thread_log}

[log_query_threads=1](../../operations/settings/settings.md#log-query-threads) 設定を使用して受信したクエリのスレッドをロギングするための設定。

クエリは [system.query_thread_log](../../operations/system-tables/query_thread_log.md#system_tables-query_thread_log) テーブルに記録され、別のファイルには記録されません。テーブルの名前を以下の `table` パラメーターで変更できます。

ロギングを構成するには、以下のパラメーターを使用します:

- `database` – データベース名。
- `table` – クエリが記録されるシステムテーブルの名前。
- `partition_by` — システムテーブルの [カスタムパーティションキー](../../engines/table-engines/mergetree-family/custom-partitioning-key.md)。`engine` が定義されている場合は使用できません。
- `order_by` - システムテーブルの [カスタムソートキー](../../engines/table-engines/mergetree-family/mergetree.md#order_by)。`engine` が定義されている場合は使用できません。
- `engine` - システムテーブルの [MergeTreeエンジン定義](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)。`partition_by` または `order_by` が定義されている場合は使用できません。
- `flush_interval_milliseconds` – メモリ中のバッファからテーブルにデータをフラッシュするための間隔。
- `max_size_rows` – ログの最大行数。未フラッシュのログ数が最大サイズに達すると、ログはディスクにダンプされます。
デフォルト: 1048576。
- `reserved_size_rows` – ログのために事前に割り当てられたメモリサイズ（行数）。
デフォルト: 8192。
- `buffer_size_rows_flush_threshold` – 行数のしきい値で、これに達すると、ログをバックグラウンドでディスクにフラッシュします。
デフォルト: `max_size_rows / 2`。
- `flush_on_crash` - クラッシュ時にログをディスクにダンプするかどうかの指示。
デフォルト: false。
- `storage_policy` – テーブルで使用するストレージポリシーの名前（オプション）
- `settings` - MergeTreeの動作を制御する [追加パラメーター](../../engines/table-engines/mergetree-family/mergetree.md/#settings)（オプション）。

テーブルが存在しない場合、ClickHouseはそれを作成します。ClickHouseサーバーが更新された際にクエリスレッドログの構造が変更された場合、古い構造のテーブルはリネームされ、新しいテーブルが自動的に作成されます。

**例**

``` xml
<query_thread_log>
    <database>system</database>
    <table>query_thread_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_thread_log>
```

## query_views_log {#query_views_log}

[log_query_views=1](../../operations/settings/settings.md#log-query-views) 設定を使用して受信したクエリに依存するビュー（ライブビュー、Materialized View など）をロギングするための設定。

クエリは [system.query_views_log](../../operations/system-tables/query_views_log.md#system_tables-query_views_log) テーブルに記録され、別のファイルには記録されません。テーブルの名前を以下の `table` パラメーターで変更できます。

ロギングを構成するには、以下のパラメーターを使用します:

- `database` – データベース名。
- `table` – クエリが記録されるシステムテーブルの名前。
- `partition_by` — システムテーブルの [カスタムパーティションキー](../../engines/table-engines/mergetree-family/custom-partitioning-key.md)。`engine` が定義されている場合は使用できません。
- `order_by` - システムテーブルの [カスタムソートキー](../../engines/table-engines/mergetree-family/mergetree.md#order_by)。`engine` が定義されている場合は使用できません。
- `engine` - システムテーブルの [MergeTreeエンジン定義](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)。`partition_by` または `order_by` が定義されている場合は使用できません。
- `flush_interval_milliseconds` – メモリ中のバッファからテーブルにデータをフラッシュするための間隔。
- `max_size_rows` – ログの最大行数。未フラッシュのログ数が最大サイズに達すると、ログはディスクにダンプされます。
デフォルト: 1048576。
- `reserved_size_rows` – ログのために事前に割り当てられたメモリサイズ（行数）。
デフォルト: 8192。
- `buffer_size_rows_flush_threshold` – 行数のしきい値で、これに達すると、ログをバックグラウンドでディスクにフラッシュします。
デフォルト: `max_size_rows / 2`。
- `flush_on_crash` - クラッシュ時にログをディスクにダンプするかどうかの指示。
デフォルト: false。
- `storage_policy` – テーブルで使用するストレージポリシーの名前（オプション）
- `settings` - MergeTreeの動作を制御する [追加パラメーター](../../engines/table-engines/mergetree-family/mergetree.md/#settings)（オプション）。

テーブルが存在しない場合、ClickHouseはそれを作成します。ClickHouseサーバーが更新された際にクエリビューのログの構造が変更された場合、古い構造のテーブルはリネームされ、新しいテーブルが自動的に作成されます。

**例**

``` xml
<query_views_log>
    <database>system</database>
    <table>query_views_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_views_log>
```

## text_log {#text_log}

テキストメッセージをログに記録するための [text_log](../../operations/system-tables/text_log.md#system_tables-text_log) システムテーブルの設定。

パラメータ:

- `level` — テーブルに保存される最大メッセージレベル（デフォルトは `Trace`）。
- `database` — データベース名。
- `table` — テーブル名。
- `partition_by` — システムテーブルの [カスタムパーティションキー](../../engines/table-engines/mergetree-family/custom-partitioning-key.md)。`engine` が定義されている場合は使用できません。
- `order_by` - システムテーブルの [カスタムソートキー](../../engines/table-engines/mergetree-family/mergetree.md#order_by)。`engine` が定義されている場合は使用できません。
- `engine` - システムテーブルの [MergeTreeエンジン定義](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)。`partition_by` または `order_by` が定義されている場合は使用できません。
- `flush_interval_milliseconds` — メモリ中のバッファからテーブルにデータをフラッシュするための間隔。
- `max_size_rows` – ログの最大行数。未フラッシュのログ数が最大サイズに達すると、ログはディスクにダンプされます。
デフォルト: 1048576。
- `reserved_size_rows` – ログのために事前に割り当てられたメモリサイズ（行数）。
デフォルト: 8192。
- `buffer_size_rows_flush_threshold` – 行数のしきい値で、これに達すると、ログをバックグラウンドでディスクにフラッシュします。
デフォルト: `max_size_rows / 2`。
- `flush_on_crash` - クラッシュ時にログをディスクにダンプするかどうかを示す。
デフォルト: false。
- `storage_policy` – テーブルで使用するストレージポリシーの名前（オプション）
- `settings` - MergeTreeの動作を制御する [追加パラメーター](../../engines/table-engines/mergetree-family/mergetree.md/#settings)（オプション）。

**例**
```xml
<clickhouse>
    <text_log>
        <level>notice</level>
        <database>system</database>
        <table>text_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <partition_by>event_date</partition_by> -->
        <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    </text_log>
</clickhouse>
```

## trace_log {#trace_log}

[trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log) システムテーブルの操作に関する設定。

パラメータ:

- `database` — テーブルを保存するためのデータベース。
- `table` — テーブル名。
- `partition_by` — システムテーブルの [カスタムパーティションキー](../../engines/table-engines/mergetree-family/custom-partitioning-key.md)。`engine` が定義されている場合は使用できません。
- `order_by` - システムテーブルの [カスタムソートキー](../../engines/table-engines/mergetree-family/mergetree.md#order_by)。`engine` が定義されている場合は使用できません。
- `engine` - システムテーブルの [MergeTreeエンジン定義](../../engines/table-engines/mergetree-family/index.md)。`partition_by` または `order_by` が定義されている場合は使用できません。
- `flush_interval_milliseconds` — メモリ中のバッファからテーブルにデータをフラッシュするための間隔。
- `max_size_rows` – ログの最大行数。未フラッシュのログ数が最大サイズに達すると、ログはディスクにダンプされます。
デフォルト: 1048576。
- `reserved_size_rows` – ログのために事前に割り当てられたメモリサイズ（行数）。
デフォルト: 8192。
- `buffer_size_rows_flush_threshold` – 行数のしきい値で、これに達すると、ログをバックグラウンドでディスクにフラッシュします。
デフォルト: `max_size_rows / 2`。
- `storage_policy` – テーブルで使用するストレージポリシーの名前（オプション）
- `settings` - MergeTreeの動作を制御する [追加パラメーター](../../engines/table-engines/mergetree-family/mergetree.md/#settings)（オプション）。

デフォルトのサーバー構成ファイル `config.xml` には、以下の設定セクションが含まれます。

``` xml
<trace_log>
    <database>system</database>
    <table>trace_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</trace_log>
```

## asynchronous_insert_log {#asynchronous_insert_log}

非同期インサートを記録するための [asynchronous_insert_log](../../operations/system-tables/asynchronous_insert_log.md#system_tables-asynchronous_insert_log) システムテーブルの設定。

パラメータ:

- `database` — データベース名。
- `table` — テーブル名。
- `partition_by` — システムテーブルの [カスタムパーティションキー](../../engines/table-engines/mergetree-family/custom-partitioning-key.md)。`engine` が定義されている場合は使用できません。
- `engine` - システムテーブルの [MergeTreeエンジン定義](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)。`partition_by` が定義されている場合は使用できません。
- `flush_interval_milliseconds` — メモリ中のバッファからテーブルにデータをフラッシュするための間隔。
- `max_size_rows` – ログの最大行数。未フラッシュのログ数が最大サイズに達すると、ログはディスクにダンプされます。
デフォルト: 1048576。
- `reserved_size_rows` – ログのために事前に割り当てられたメモリサイズ（行数）。
デフォルト: 8192。
- `buffer_size_rows_flush_threshold` – 行数のしきい値で、これに達すると、ログをバックグラウンドでディスクにフラッシュします。
デフォルト: `max_size_rows / 2`。
- `flush_on_crash` - クラッシュ時にログをディスクにダンプするかどうかを示す。
デフォルト: false。
- `storage_policy` – テーブルで使用するストレージポリシーの名前（オプション）

**例**

```xml
<clickhouse>
    <asynchronous_insert_log>
        <database>system</database>
        <table>asynchronous_insert_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine> -->
    </asynchronous_insert_log>
</clickhouse>
```

## crash_log {#crash_log}

[crash_log](../../operations/system-tables/crash-log.md) システムテーブルの操作に関する設定。

パラメータ:

- `database` — テーブルを保存するためのデータベース。
- `table` — テーブル名。
- `partition_by` — システムテーブルの [カスタムパーティションキー](../../engines/table-engines/mergetree-family/custom-partitioning-key.md)。`engine` が定義されている場合は使用できません。
- `order_by` - システムテーブルの [カスタムソートキー](../../engines/table-engines/mergetree-family/mergetree.md#order_by)。`engine` が定義されている場合は使用できません。
- `engine` - システムテーブルの [MergeTreeエンジン定義](../../engines/table-engines/mergetree-family/index.md)。`partition_by` または `order_by` が定義されている場合は使用できません。
- `flush_interval_milliseconds` — メモリ中のバッファからテーブルにデータをフラッシュするための間隔。
- `max_size_rows` – ログの最大行数。未フラッシュのログ数が最大サイズに達すると、ログはディスクにダンプされます。
デフォルト: 1048576。
- `reserved_size_rows` – ログのために事前に割り当てられたメモリサイズ（行数）。
デフォルト: 8192。
- `buffer_size_rows_flush_threshold` – 行数のしきい値で、これに達すると、ログをバックグラウンドでディスクにフラッシュします。
デフォルト: `max_size_rows / 2`。
- `flush_on_crash` - クラッシュ時にログをディスクにダンプするかどうかを示す。
デフォルト: false。
- `storage_policy` – テーブルで使用するストレージポリシーの名前（オプション）
- `settings` - MergeTreeの動作を制御する [追加パラメーター](../../engines/table-engines/mergetree-family/mergetree.md/#settings)（オプション）。

デフォルトのサーバー構成ファイル `config.xml` には、以下の設定セクションが含まれます。

``` xml
<crash_log>
    <database>system</database>
    <table>crash_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1024</max_size_rows>
    <reserved_size_rows>1024</reserved_size_rows>
    <buffer_size_rows_flush_threshold>512</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</crash_log>
```

## backup_log {#backup_log}

`BACKUP` および `RESTORE` 操作を記録するための [backup_log](../../operations/system-tables/backup_log.md) システムテーブルの設定。

パラメータ:

- `database` — データベース名。
- `table` — テーブル名。
- `partition_by` — システムテーブルの [カスタムパーティションキー](../../engines/table-engines/mergetree-family/custom-partitioning-key.md)。`engine` が定義されている場合は使用できません。
- `order_by` - システムテーブルの [カスタムソートキー](../../engines/table-engines/mergetree-family/mergetree.md#order_by)。`engine` が定義されている場合は使用できません。
- `engine` - システムテーブルの [MergeTreeエンジン定義](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)。`partition_by` または `order_by` が定義されている場合は使用できません。
- `flush_interval_milliseconds` — メモリ中のバッファからテーブルにデータをフラッシュするための間隔。
- `max_size_rows` – ログの最大行数。未フラッシュのログ数が最大サイズに達すると、ログはディスクにダンプされます。
デフォルト: 1048576。
- `reserved_size_rows` – ログのために事前に割り当てられたメモリサイズ（行数）。
デフォルト: 8192。
- `buffer_size_rows_flush_threshold` – 行数のしきい値で、これに達すると、ログをバックグラウンドでディスクにフラッシュします。
デフォルト: `max_size_rows / 2`。
- `flush_on_crash` - クラッシュ時にログをディスクにダンプするかどうかを示す。
デフォルト: false。
- `storage_policy` – テーブルで使用するストレージポリシーの名前（オプション）。
- `settings` - MergeTreeの動作を制御する [追加パラメーター](../../engines/table-engines/mergetree-family/mergetree.md#settings)（オプション）。

**例**

```xml
<clickhouse>
    <backup_log>
```
```xml
<database>system</database>
<table>backup_log</table>
<flush_interval_milliseconds>1000</flush_interval_milliseconds>
<partition_by>toYYYYMM(event_date)</partition_by>
<max_size_rows>1048576</max_size_rows>
<reserved_size_rows>8192</reserved_size_rows>
<buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
<flush_on_crash>false</flush_on_crash>
<!-- <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine> -->
</backup_log>
</clickhouse>
```

## query_masking_rules {#query-masking-rules}

正規表現に基づくルールで、クエリや全てのログメッセージに適用され、サーバーログである`system.query_log`、`system.text_log`、`system.processes`テーブル、およびクライアントに送信されるログに保存される前に適用されます。これにより、SQLクエリからの機密データ（名前、メール、個人識別子やクレジットカード番号など）がログに漏洩するのを防ぐことができます。

**例**

```xml
<query_masking_rules>
    <rule>
        <name>hide SSN</name>
        <regexp>(^|\D)\d{3}-\d{2}-\d{4}($|\D)</regexp>
        <replace>000-00-0000</replace>
    </rule>
</query_masking_rules>
```

設定フィールド:
- `name` - ルールの名前（オプショナル）
- `regexp` - RE2互換の正規表現（必須）
- `replace` - 機密データのための置換文字列（オプショナル、デフォルトでは六つのアスタリスク）

マスキングルールは、誤った形式のクエリや非解析可能なクエリからの機密データの漏洩を防ぐために、クエリ全体に適用されます。

`system.events`テーブルには、全体のクエリマスキングルールの一致数を示すカウンター`QueryMaskingRulesMatch`があります。

分散クエリ用には、各サーバーを別々に設定する必要があります。そうしないと、他のノードに渡されるサブクエリがマスキングされずに保存されます。

## remote_servers {#server-settings-remote-servers}

[分散テーブル](../../engines/table-engines/special/distributed.md)エンジンおよび`cluster`テーブル関数で使用されるクラスターの設定。

**例**

```xml
<remote_servers incl="clickhouse_remote_servers" />
```

`incl`属性の値については、セクション「[設定ファイル](../../operations/configuration-files.md#configuration_files)」を参照してください。

**参照**

- [skip_unavailable_shards](../../operations/settings/settings.md#skip_unavailable_shards)
- [クラスター検出](../../operations/cluster-discovery.md)
- [レプリケーテッドデータベースエンジン](../../engines/database-engines/replicated.md)

## timezone {#timezone}

サーバーのタイムゾーン。

UTCのタイムゾーンまたは地理的な場所のIANA識別子として指定します（例えば、Africa/Abidjan）。

タイムゾーンは、DateTimeフィールドをテキスト形式で出力する際（画面表示やファイルへの出力）や、文字列からDateTimeを取得する際に、文字列とDateTime形式の間の変換に必要です。また、入力パラメータにタイムゾーンが指定されていない場合は、時間と日付を操作する関数で使用されます。

**例**

```xml
<timezone>Asia/Istanbul</timezone>
```

**関連情報**

- [session_timezone](../settings/settings.md#session_timezone)

## tcp_port {#tcp_port}

TCPプロトコルを介してクライアントと通信するためのポート。

**例**

```xml
<tcp_port>9000</tcp_port>
```

## tcp_port_secure {#tcp_port_secure}

クライアントとのセキュア通信のためのTCPポート。[OpenSSL](#openssl)設定とともに使用してください。

**可能な値**

正の整数。

**デフォルト値**

```xml
<tcp_port_secure>9440</tcp_port_secure>
```

## mysql_port {#mysql_port}

MySQLプロトコルを介してクライアントと通信するためのポート。

**可能な値**

リッスンするポート番号を指定する正の整数、または無効にするための空の値。

例

```xml
<mysql_port>9004</mysql_port>
```

## postgresql_port {#postgresql_port}

PostgreSQLプロトコルを介してクライアントと通信するためのポート。

**可能な値**

リッスンするポート番号を指定する正の整数、または無効にするための空の値。

例

```xml
<postgresql_port>9005</postgresql_port>
```

## tmp_path {#tmp-path}

大規模なクエリ処理のための一時データを保存するローカルファイルシステム上のパス。

:::note
- 一時データの保存先を設定するためのオプションは、`tmp_path` 、`tmp_policy`、`temporary_data_in_cache`の一つしか使用できません。
- トレーリングスラッシュは必須です。
:::

**例**

```xml
<tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
```

## user_files_path {#user_files_path}

ユーザーファイルを含むディレクトリ。[file()](../../sql-reference/table-functions/file.md)テーブル関数、[fileCluster()](../../sql-reference/table-functions/fileCluster.md)テーブル関数で使用されます。

**例**

```xml
<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```

## user_scripts_path {#user_scripts_path}

ユーザースクリプトファイルを含むディレクトリ。可執行ユーザー定義関数[Executable User Defined Functions](../../sql-reference/functions/index.md#executable-user-defined-functions)で使用されます。

**例**

```xml
<user_scripts_path>/var/lib/clickhouse/user_scripts/</user_scripts_path>
```

## user_defined_path {#user_defined_path}

ユーザー定義ファイルを含むディレクトリ。[SQLユーザー定義関数](../../sql-reference/functions/index.md#user-defined-functions)で使用されます。

**例**

```xml
<user_defined_path>/var/lib/clickhouse/user_defined/</user_defined_path>
```

## users_config {#users-config}

以下を含むファイルのパス：

- ユーザー設定。
- アクセス権。
- 設定プロファイル。
- クオータ設定。

**例**

```xml
<users_config>users.xml</users_config>
```

## wait_dictionaries_load_at_startup {#wait_dictionaries_load_at_startup}

この設定により、`dictionaries_lazy_load`が`false`の場合の動作を指定できます。
（`dictionaries_lazy_load`が`true`の場合、この設定は何にも影響しません。）

`wait_dictionaries_load_at_startup`が`false`の場合、サーバーは起動時にすべてのDictionaryの読み込みを開始し、読み込みと並行して接続を受け入れます。
Dictionaryが初めてクエリに使用されると、そのクエリはDictionaryが読み込まれるまで待機します。
`wait_dictionaries_load_at_startup`を`false`に設定すると、ClickHouseの起動が速くなる可能性がありますが、一部のクエリは遅く実行される可能性があります（Dictionaryの読み込みを待機する必要があるためです）。

`wait_dictionaries_load_at_startup`が`true`の場合、サーバーはすべてのDictionaryの読み込みが完了するまで起動時に待機し（正常に読み込まれるかどうかは問わず）、その後に接続を受け入れるようになります。

デフォルトは`true`です。

**例**

```xml
<wait_dictionaries_load_at_startup>true</wait_dictionaries_load_at_startup>
```

## zookeeper {#server-settings_zookeeper}

ClickHouseが[ZooKeeper](http://zookeeper.apache.org/)クラスターと連携するために必要な設定を含んでいます。

ClickHouseは、レプリケーテッドテーブルを使用する際にレプリカのメタデータを保存するためにZooKeeperを使用します。レプリケーテッドテーブルを使用しない場合、このパラメータセクションは省略できます。

このセクションには以下のパラメータが含まれます：

- `node` — ZooKeeperエンドポイント。複数のエンドポイントを設定できます。

    例：

```xml
    <node index="1">
        <host>example_host</host>
        <port>2181</port>
    </node>
```

    `index`属性は、ZooKeeperクラスターに接続しようとするときのノードの順序を指定します。

- `session_timeout_ms` — クライアントセッションの最大タイムアウトをミリ秒で指定。
- `operation_timeout_ms` — 1つの操作の最大タイムアウトをミリ秒で指定。
- `root` — ClickHouseサーバーが使用するznodeのルートとして使用される[znode](http://zookeeper.apache.org/doc/r3.5.5/zookeeperOver.html#Nodes+and+ephemeral+nodes)。オプション。
- `fallback_session_lifetime.min` - zookeeper_load_balancing戦略によって解決された最初のzookeeperホストが利用できない場合、zookeeperセッションの寿命をフォールバックノードに限定。これは負荷分散目的で、特定のzookeeperホストへの過剰な負荷を避けるために行われます。この設定はフォールバックセッションの最小期間を設定します。秒で設定。オプション。デフォルトは3時間。
- `fallback_session_lifetime.max` - zookeeper_load_balancing戦略によって解決された最初のzookeeperホストが利用できない場合、zookeeperセッションの寿命をフォールバックノードに限定。これは負荷分散目的で、特定のzookeeperホストへの過剰な負荷を避けるために行われます。この設定はフォールバックセッションの最大期間を設定します。秒で設定。オプション。デフォルトは6時間。
- `identity` — ClickHouseサーバーが要求されたznodeにアクセスするために必要なユーザーとパスワード。オプション。
- zookeeper_load_balancing - ZooKeeperノードの選択アルゴリズムを指定。
  * random - ZooKeeperノードをランダムに選択。
  * in_order - 最初のZooKeeperノードを選択し、利用できない場合は次のノードを選択。
  * nearest_hostname - サーバーのホスト名に最も近いホスト名を持つZooKeeperノードを選択、ホスト名は名前の接頭辞で比較されます。
  * hostname_levenshtein_distance - nearest_hostnameと同じですが、レーベンシュタイン距離形式でホスト名を比較。
  * first_or_random - 最初のZooKeeperノードを選択し、利用できない場合は残るZooKeeperノードをランダムに選択。
  * round_robin - 最初のZooKeeperノードを選択し、再接続が発生すると次のノードを選択。
- `use_compression` — セキュリティープロトコルにおいて、Keeperプロトコル内の圧縮を有効にします。

**設定の例**

```xml
<zookeeper>
    <node>
        <host>example1</host>
        <port>2181</port>
    </node>
    <node>
        <host>example2</host>
        <port>2181</port>
    </node>
    <session_timeout_ms>30000</session_timeout_ms>
    <operation_timeout_ms>10000</operation_timeout_ms>
    <!-- オプショナル。Chrootサフィックス。存在する必要があります。 -->
    <root>/path/to/zookeeper/node</root>
    <!-- オプショナル。ZookeeperダイジェストACL文字列。 -->
    <identity>user:password</identity>
    <!--<zookeeper_load_balancing>random / in_order / nearest_hostname / hostname_levenshtein_distance / first_or_random / round_robin</zookeeper_load_balancing>-->
    <zookeeper_load_balancing>random</zookeeper_load_balancing>
</zookeeper>
```

**関連リンク**

- [レプリケーション](../../engines/table-engines/mergetree-family/replication.md)
- [ZooKeeperプログラマーズガイド](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)
- [ClickHouseとZookeeper間のオプションでのセキュア通信](../ssl-zookeeper.md#secured-communication-with-zookeeper)

## use_minimalistic_part_header_in_zookeeper {#server-settings-use_minimalistic_part-header-in-zookeeper}

ZooKeeperにおけるデータパートヘッダーの保存方法。

この設定は`MergeTree`ファミリーにのみ適用されます。次の方法で指定できます：

- `config.xml`ファイルの[merge_tree](#merge_tree)セクションでのグローバル指定。

    ClickHouseはサーバー上の全てのテーブルに対してこの設定を使用します。いつでも設定を変更できます。既存のテーブルは設定が変更されたときに動作を変更します。

- 各テーブルごと。

    テーブルを作成するときに、対応する[エンジン設定](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)を指定します。この設定を持つ既存のテーブルの動作は、グローバル設定が変更されても変わりません。

**可能な値**

- 0 — 機能はオフです。
- 1 — 機能はオンです。

`use_minimalistic_part_header_in_zookeeper = 1`の場合、[レプリケーテッド](../../engines/table-engines/mergetree-family/replication.md)テーブルは、単一の`znode`を使用してデータパートのヘッダーをコンパクトに保存します。テーブルに多くのカラムが含まれている場合、この保存方法はZooKeeper内で保存されるデータのボリュームを大幅に減少させます。

:::note
`use_minimalistic_part_header_in_zookeeper = 1`を適用した後、ClickHouseサーバーをこの設定をサポートしないバージョンにダウングレードすることはできません。クラスター内のサーバーをアップグレードする際には慎重に行ってください。一度にすべてのサーバーをアップグレードすることは避けてください。ClickHouseの新しいバージョンをテスト環境またはクラスター内の一部のサーバーでテストすることがより安全です。

この設定で既に保存されたデータパートヘッダーは、以前の（非コンパクトな）表現に復元することはできません。
:::

**デフォルト値:** 0。

## distributed_ddl {#server-settings-distributed_ddl}

クラスター上で[分散DDLクエリ](../../sql-reference/distributed-ddl.md)（CREATE、DROP、ALTER、RENAME）を実行します。
[ZooKeeper](#server-settings_zookeeper)が有効な場合にのみ機能します。

`<distributed_ddl>`内で構成可能な設定は次のとおりです：

- **path**: Keeper内で分散DDLクエリのための`task_queue`のパス
- **profile**: DDLクエリを実行するために使用されるプロファイル
- **pool_size**: 同時に実行できる`ON CLUSTER`クエリの数
- **max_tasks_in_queue**: キュー内に存在できるタスクの最大数。デフォルトは1,000
- **task_max_lifetime**: ノードの年齢がこの値を超えた場合に削除。デフォルトは`7 * 24 * 60 * 60`（1週間）
- **cleanup_delay_period**: 最後のクリーンアップがこの期間以来行われていない場合、新しいノードイベントを受信した後に清掃が開始されます。デフォルトは60秒

**例**

```xml
<distributed_ddl>
    <!-- ZooKeeperのDDLクエリのキューへのパス -->
    <path>/clickhouse/task_queue/ddl</path>

    <!-- DDLクエリを実行するために使用される設定 -->
    <profile>default</profile>

    <!-- 同時に実行できるON CLUSTERクエリの数を制御します。 -->
    <pool_size>1</pool_size>

    <!-- クリーンアップ設定（アクティブなタスクは削除されません） -->
    <!-- タスクのTTLを制御（デフォルト1週間） -->
    <task_max_lifetime>604800</task_max_lifetime>

    <!-- クリーンアップが行われる頻度を制御（秒） -->
    <cleanup_delay_period>60</cleanup_delay_period>

    <!-- キュー内にあるタスクの最大数を制御します -->
    <max_tasks_in_queue>1000</max_tasks_in_queue>
</distributed_ddl>
```

## access_control_path {#access_control_path}

SQLコマンドによって作成されたユーザーやロールの設定をClickHouseサーバーが保存するフォルダへのパス。

デフォルト値: `/var/lib/clickhouse/access/`。

**関連情報**

- [アクセス制御とアカウント管理](../../guides/sre/user-management/index.md#access-control)

## user_directories {#user_directories}

設定ファイルに含まれるセクション：
- 事前定義されたユーザーを含む設定ファイルへのパス。
- SQLコマンドによって作成されたユーザーが保存されるフォルダへのパス。
- SQLコマンドによって作成され、複製されたユーザーが保存されるZooKeeperノードのパス（エクスペリメンタル）。

このセクションが指定されている場合、[`users_config`](../../operations/server-configuration-parameters/settings.md#users-config)および[`access_control_path`](../../operations/server-configuration-parameters/settings.md#access_control_path)のパスは使用されません。

`user_directories`セクションは任意の数の項目を含むことができ、項目の順序はその優先順位を意味します（上位の項目は優先順位が高い）。

**例**

```xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <local_directory>
        <path>/var/lib/clickhouse/access/</path>
    </local_directory>
</user_directories>
```

ユーザー、ロール、行ポリシー、クオータ、プロファイルはZooKeeperにも保存できます：

```xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <replicated>
        <zookeeper_path>/clickhouse/access/</zookeeper_path>
    </replicated>
</user_directories>
```

`memory`セクション — 情報の保存をメモリにのみ行い、ディスクへの書き込みを行わない、`ldap`セクション — 情報の保存をLDAPサーバーに依存する、というセクションも定義できます。

LDAPサーバーをリモートユーザーディレクトリとして、ローカルに定義されていないユーザーとして追加するには、単一の`ldap`セクションを指定し、以下のパラメータを定義します:
- `server` — `ldap_servers`設定セクションで定義されたLDAPサーバー名の一つ。このパラメータは必須で空にできません。
- `roles` — LDAPサーバーから取得した各ユーザーに割り当てられるローカルで定義されたロールのリストを持つセクション。ロールが指定されていない場合、認証後ユーザーは何のアクションも実行できません。列挙されたロールのいずれかが認証時にローカルで定義されていない場合、認証は不正なパスワードであるかのように失敗します。

**例**

```xml
<ldap>
    <server>my_ldap_server</server>
    <roles>
        <my_local_role1 />
        <my_local_role2 />
    </roles>
</ldap>
```

## total_memory_profiler_step {#total-memory-profiler-step}

メモリのピーク割り当て手順ごとにスタックトレースを取得するメモリサイズ（バイト単位）を設定します。データは`query_id`が空文字列に等しい`system.trace_log`システムテーブルに保存されます。

可能な値：

- 正の整数。

デフォルト値：`4194304`。

## total_memory_tracker_sample_probability {#total-memory-tracker-sample-probability}

ランダム割り当ておよび開放の収集を有効にし、`system.trace_log`システムテーブルに`MemorySample`として指定された確率で書き込みます。確率は、割り当てまたは開放時のサイズに関係なく、各操作に適用されます。事前にトラッキングされていないメモリ量が無視されないメモリ制限（デフォルト値は`4`MiB）を超えるときにのみサンプリングが行われます。`total_memory_profiler_step`が低い場合、これをさらに低くすることができます。非常に詳細にサンプリングするには、`total_memory_profiler_step`を`1`に設定できます。

可能な値：

- 正の整数。
- 0 — ランダムな割り当ておよび開放の`system.trace_log`システムテーブルへの書き込みは無効。

デフォルト値：`0`。

## compiled_expression_cache_size {#compiled-expression-cache-size}

[コンパイルされた式](../../operations/caches.md)のキャッシュサイズ（バイト単位）を設定します。

可能な値：

- 正の整数。

デフォルト値：`134217728`。

## compiled_expression_cache_elements_size {#compiled_expression_cache_elements_size}

[コンパイルされた式](../../operations/caches.md)のキャッシュサイズ（要素単位）を設定します。

可能な値：

- 正の整数。

デフォルト値：`10000`。

## display_secrets_in_show_and_select {#display_secrets_in_show_and_select}

`SHOW`および`SELECT`クエリで、テーブル、データベース、テーブル関数、およびDictionaryの機密情報の表示を有効または無効にします。

機密情報を表示したいユーザーは
[`format_display_secrets_in_show_and_select`フォーマット設定](../settings/formats#format_display_secrets_in_show_and_select)
をオンにし、
[`displaySecretsInShowAndSelect`](../../sql-reference/statements/grant#display-secrets)特権を持たねばなりません。

可能な値：

- 0 — 無効。
- 1 — 有効。

デフォルト値：0。

## proxy {#proxy}

S3ストレージ、S3テーブル関数、そしてURL関数で現在サポートされているHTTPおよびHTTPSリクエストのプロキシサーバーを定義します。

プロキシサーバーは、環境変数、プロキシリスト、リモートプロキシリゾルバで定義できます。

特定のホストに対するプロキシサーバーのバイパスも、`no_proxy`を使用してサポートされています。

### 環境変数

`http_proxy`と`https_proxy`の環境変数を指定してください。これにより、特定のプロトコル用のプロキシサーバーを指定できます。システム上で設定されている場合は、シームレスに機能します。

これは、特定のプロトコルに対して1つのプロキシサーバーがあり、そのプロキシサーバーが変更されない場合に最もシンプルなアプローチです。

### プロキシリスト

このアプローチでは、1つまたは複数のプロトコルに対するプロキシサーバーを指定することができます。複数のプロキシサーバーが定義されている場合、ClickHouseはサーバー間で負荷を分散するためにラウンドロビンベースでさまざまなプロキシを使用します。特定のプロトコルに対して複数のプロキシサーバーがあり、プロキシサーバーのリストが変更されない場合には、最もシンプルなアプローチです。

### 設定テンプレート

```xml
<proxy>
    <http>
        <uri>http://proxy1</uri>
        <uri>http://proxy2:3128</uri>
    </http>
    <https>
        <uri>http://proxy1:3128</uri>
    </https>
</proxy>
```

`<proxy>` フィールド

* `<http>` - 1つ以上のHTTPプロキシのリスト
* `<https>` - 1つ以上のHTTPSプロキシのリスト

`<http>`および`<https>`フィールド

* `<uri>` - プロキシのURI

### リモートプロキシリゾルバ

プロキシサーバーが動的に変わる可能性があります。その場合、リゾルバのエンドポイントを定義できます。ClickHouseはそのエンドポイントに空のGETリクエストを送信し、リモートリゾルバはプロキシのホストを返すべきです。それをClickHouseは次のテンプレートを使用してプロキシURIを形成します: `{proxy_scheme}://{proxy_host}:{proxy_port}`

### 設定テンプレート

```xml
<proxy>
    <http>
        <resolver>
            <endpoint>http://resolver:8080/hostname</endpoint>
            <proxy_scheme>http</proxy_scheme>
            <proxy_port>80</proxy_port>
            <proxy_cache_time>10</proxy_cache_time>
        </resolver>
    </http>

    <https>
        <resolver>
            <endpoint>http://resolver:8080/hostname</endpoint>
            <proxy_scheme>http</proxy_scheme>
            <proxy_port>3128</proxy_port>
            <proxy_cache_time>10</proxy_cache_time>
        </resolver>
    </https>
</proxy>
```

`<proxy>` フィールド

* `<http>` - 1つ以上のリゾルバのリスト
* `<https>` - 1つ以上のリゾルバのリスト

`<http>`および`<https>`フィールド

* `<resolver>` - リゾルバのエンドポイントなどの詳細情報。
  複数の`<resolver>`要素を配置できますが、特定のプロトコルに対しては最初の`<resolver>`のみが使用されます。それ以降の`<resolver>`要素は無視されます。つまり、必要であれば、ロードバランシングはリモートリゾルバによって実装する必要があります。

`<resolver>` フィールド

* `<endpoint>` - プロキシリゾルバのURI
* `<proxy_scheme>` - 最終プロキシURIのプロトコル。 `http`か`https`のいずれかが使用可能です。
* `<proxy_port>` - プロキシリゾルバのポート番号
* `<proxy_cache_time>` - リゾルバからの値をClickHouseがキャッシュする時間（秒単位）。この値を0に設定すると、ClickHouseは各HTTPまたはHTTPSリクエストごとにリゾルバにアクセスします。

### 優先順位

プロキシ設定の優先順位は以下の通りです：

1. リモートプロキシリゾルバ
2. プロキシリスト
3. 環境変数

ClickHouseは要求されたプロトコルについて最も優先されるリゾルバタイプを確認します。それが定義されていない場合、次に優先されるリゾルバタイプを確認し、環境リゾルバに到達します。よって、リゾルバタイプはミックスで使用できます。

### disable_tunneling_for_https_requests_over_http_proxy {#disable_tunneling_for_https_requests_over_http_proxy}

デフォルトでは、トンネリング（すなわち`HTTP CONNECT`）は`HTTP`プロキシ経由での`HTTPS`リクエスト作成に使用されます。この設定でそれを無効にできます。

### no_proxy
デフォルトでは、すべてのリクエストはプロキシを通じて行われます。特定のホストに対してそれを無効にするために、`no_proxy`変数を設定する必要があります。
プロキシ設定でリストおよびリモートリゾルバと一緒に使うためには`<proxy>`句の中で設定し、環境リゾルバには環境変数として設定できます。
IPアドレス、ドメイン、サブドメイン、そして完全バイパス用の`'*'`ワイルドカードをサポートしています。先頭のドットはカールと同様に削除されます。

例:

以下の設定では、`clickhouse.cloud`とそのすべてのサブドメイン（例、`auth.clickhouse.cloud`）へのプロキシリクエストがバイパスされます。
GitLabに関しても、先頭にドットがあっても同様です。`gitlab.com`および`about.gitlab.com`はどちらもプロキシをバイパスします。

```xml
<proxy>
    <no_proxy>clickhouse.cloud,.gitlab.com</no_proxy>
    <http>
        <uri>http://proxy1</uri>
        <uri>http://proxy2:3128</uri>
    </http>
    <https>
        <uri>http://proxy1:3128</uri>
    </https>
</proxy>
```

## max_materialized_views_count_for_table {#max_materialized_views_count_for_table}

テーブルにアタッチされたマテリアライズドビューの数の制限。
ここでは直接依存するビューのみが考慮されており、1つのビューの上にもう1つのビューを作成することは考慮されません。

デフォルト値：`0`。

## format_alter_operations_with_parentheses {#format_alter_operations_with_parentheses}

真の場合、フォーマットされたクエリで変更操作が括弧で囲まれます。これにより、フォーマットされた変更クエリの解析が曖昧さを減少させます。

タイプ: Bool

デフォルト: 0

## ignore_empty_sql_security_in_create_view_query {#ignore_empty_sql_security_in_create_view_query}

真の場合、ClickHouseはCREATE VIEWクエリの空のSQLセキュリティステートメントのデフォルトを書き込みません。

:::note
この設定は移行期間にのみ必要であり、24.4で廃止予定です。
:::

タイプ: Bool

デフォルト: 1

## merge_workload {#merge_workload}

マージと他のワークロード間でのリソースの利用および共有方法を調整するために使用されます。指定された値はすべてのバックグラウンドマージに対して`workload`設定値として使用されます。マージツリー設定によってオーバーライド可能です。

デフォルト値: "default"

**参照**
- [ワークロードスケジューリング](/docs/ja/operations/workload-scheduling.md)

## mutation_workload {#mutation_workload}

突然のトランスフォーメーションと他のワークロード間でのリソースの利用および共有方法を調整するために使用されます。指定された値はすべてのバックグラウンド突然のトランスフォーメーションに対して`workload`設定値として使用されます。マージツリー設定によってオーバーライド可能です。

デフォルト値: "default"

**参照**
- [ワークロードスケジューリング](/docs/ja/operations/workload-scheduling.md)

## workload_path {#workload_path}

すべての`CREATE WORKLOAD`および`CREATE RESOURCE`クエリのためのストレージとして使用されるディレクトリ。デフォルトでは、サーバーの作業ディレクトリ内の`/workload/`フォルダが使用されます。

**例**

```xml
<workload_path>/var/lib/clickhouse/workload/</workload_path>
```

**参照**
- [ワークロード階層](/docs/ja/operations/workload-scheduling.md#workloads)
- [workload_zookeeper_path](#workload_zookeeper_path)

## workload_zookeeper_path {#workload_zookeeper_path}

すべての`CREATE WORKLOAD`および`CREATE RESOURCE`クエリを保存するために使用されるZooKeeperノードへのパス。整合性のために、すべてのSQL定義がこの単一のznodeの値として保存されます。デフォルトでは、ZooKeeperは使用されず、定義は[ディスク](#workload_path)に保存されます。

**例**

```xml
<workload_zookeeper_path>/clickhouse/workload/definitions.sql</workload_zookeeper_path>
```

**参照**
- [ワークロード階層](/docs/ja/operations/workload-scheduling.md#workloads)
- [workload_path](#workload_path)

## max_authentication_methods_per_user {#max_authentication_methods_per_user}

ユーザーが作成される際や認証メソッドを変更する際に指定できる認証メソッドの最大数。
この設定を変更しても既存のユーザーには影響しません。制限を超える認証に関連する作成/変更クエリは失敗しますが、認証に関連しない作成/変更クエリは成功します。

タイプ: UInt64

デフォルト値: 100

0は無制限を意味します

## use_legacy_mongodb_integration

レガシーMongoDB統合実装を使用します。非推奨です。

タイプ: Bool

デフォルト値: `true`。
```
