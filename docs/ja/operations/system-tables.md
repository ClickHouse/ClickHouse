---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 52
toc_title: "\u30B7\u30B9\u30C6\u30E0\u8868"
---

# システム表 {#system-tables}

システムテーブルは、システムの機能の一部を実装したり、システムの動作に関する情報へのアクセスを提供するために使用されます。
システムテーブルを削除することはできません（ただし、DETACHを実行できます）。
システムテーブルのないファイルデータのハードディスクまたはファイルとメタデータを指すものとします。 サーバーは起動時にすべてのシステムテーブルを作成します。
システムテーブルは読み取り専用です。
彼らはに位置しています ‘system’ データベース。

## システムasynchronous\_metrics {#system_tables-asynchronous_metrics}

バックグラウンドで定期的に計算される指標が含まれます。 例えば、使用中のRAMの量。

列:

-   `metric` ([文字列](../sql-reference/data-types/string.md)) — Metric name.
-   `value` ([Float64](../sql-reference/data-types/float.md)) — Metric value.

**例**

``` sql
SELECT * FROM system.asynchronous_metrics LIMIT 10
```

``` text
┌─metric──────────────────────────────────┬──────value─┐
│ jemalloc.background_thread.run_interval │          0 │
│ jemalloc.background_thread.num_runs     │          0 │
│ jemalloc.background_thread.num_threads  │          0 │
│ jemalloc.retained                       │  422551552 │
│ jemalloc.mapped                         │ 1682989056 │
│ jemalloc.resident                       │ 1656446976 │
│ jemalloc.metadata_thp                   │          0 │
│ jemalloc.metadata                       │   10226856 │
│ UncompressedCacheCells                  │          0 │
│ MarkCacheFiles                          │          0 │
└─────────────────────────────────────────┴────────────┘
```

**も参照。**

-   [監視](monitoring.md) — Base concepts of ClickHouse monitoring.
-   [システムメトリック](#system_tables-metrics) — Contains instantly calculated metrics.
-   [システムイベント](#system_tables-events) — Contains a number of events that have occurred.
-   [システムmetric\_log](#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.

## システムクラスタ {#system-clusters}

についての情報が含まれてクラスターのコンフィグファイルをサーバーです。

列:

-   `cluster` (String) — The cluster name.
-   `shard_num` (UInt32) — The shard number in the cluster, starting from 1.
-   `shard_weight` (UInt32) — The relative weight of the shard when writing data.
-   `replica_num` (UInt32) — The replica number in the shard, starting from 1.
-   `host_name` (String) — The host name, as specified in the config.
-   `host_address` (String) — The host IP address obtained from DNS.
-   `port` (UInt16) — The port to use for connecting to the server.
-   `user` (String) — The name of the user for connecting to the server.
-   `errors_count` (UInt32)-このホストがレプリカに到達できなかった回数。
-   `estimated_recovery_time` (UInt32)-レプリカエラーカウントがゼロになり、正常に戻るまでの秒数。

ご注意ください `errors_count` クラスタに対するクエリごとに一度updatedされますが `estimated_recovery_time` オンデマンドで再計算されます。 だから、ゼロ以外の場合があるかもしれません `errors_count` とゼロ `estimated_recovery_time` 次のクエリはゼロになります `errors_count` エラーがないかのようにreplicaを使用してみてください。

**も参照。**

-   [分散テーブルエンジン](../engines/table-engines/special/distributed.md)
-   [distributed\_replica\_error\_cap設定](settings/settings.md#settings-distributed_replica_error_cap)
-   [distributed\_replica\_error\_half\_life設定](settings/settings.md#settings-distributed_replica_error_half_life)

## システム列 {#system-columns}

すべてのテーブルの列に関する情報を格納します。

このテーブルを使用すると、次のような情報を取得できます [DESCRIBE TABLE](../sql-reference/statements/misc.md#misc-describe-table) クエリが、一度に複数のテーブルのために。

その `system.columns` テーブルを含む以下のカラムのカラムタイプはブラケット):

-   `database` (String) — Database name.
-   `table` (String) — Table name.
-   `name` (String) — Column name.
-   `type` (String) — Column type.
-   `default_kind` (String) — Expression type (`DEFAULT`, `MATERIALIZED`, `ALIAS` デフォルト値の場合は)、定義されていない場合は空の文字列。
-   `default_expression` (String) — Expression for the default value, or an empty string if it is not defined.
-   `data_compressed_bytes` (UInt64) — The size of compressed data, in bytes.
-   `data_uncompressed_bytes` (UInt64) — The size of decompressed data, in bytes.
-   `marks_bytes` (UInt64) — The size of marks, in bytes.
-   `comment` (String) — Comment on the column, or an empty string if it is not defined.
-   `is_in_partition_key` (UInt8) — Flag that indicates whether the column is in the partition expression.
-   `is_in_sorting_key` (UInt8) — Flag that indicates whether the column is in the sorting key expression.
-   `is_in_primary_key` (UInt8) — Flag that indicates whether the column is in the primary key expression.
-   `is_in_sampling_key` (UInt8) — Flag that indicates whether the column is in the sampling key expression.

## システム貢献者 {#system-contributors}

を含むに関する情報提供者が保持しています。 ランダムな順序ですべてのconstributors。 順序は、クエリ実行時にランダムです。

列:

-   `name` (String) — Contributor (author) name from git log.

**例**

``` sql
SELECT * FROM system.contributors LIMIT 10
```

``` text
┌─name─────────────┐
│ Olga Khvostikova │
│ Max Vetrov       │
│ LiuYangkuan      │
│ svladykin        │
│ zamulla          │
│ Šimon Podlipský  │
│ BayoNet          │
│ Ilya Khomutov    │
│ Amy Krishnevsky  │
│ Loud_Scream      │
└──────────────────┘
```

テーブル内で自分自身を知るには、クエリを使用します:

``` sql
SELECT * FROM system.contributors WHERE name='Olga Khvostikova'
```

``` text
┌─name─────────────┐
│ Olga Khvostikova │
└──────────────────┘
```

## システムデータ {#system-databases}

このテーブルを含む単一の文字列カラムと呼ばれ ‘name’ – the name of a database.
各データベースのサーバーについて知っていて対応するエントリの表に示す。
このシステムテーブルは、 `SHOW DATABASES` クエリ。

## システムdetached\_parts {#system_tables-detached_parts}

についての情報が含まれて外部 [メルゲツリー](../engines/table-engines/mergetree-family/mergetree.md) テーブル その `reason` column部品が切り離された理由を指定します。 ユーザーが取り外した部品の場合、その理由は空です。 このような部品は、 [ALTER TABLE ATTACH PARTITION\|PART](../sql-reference/statements/alter.md#alter_attach-partition) コマンド その他の列の説明については、 [システム部品](#system_tables-parts). パーツ名が無効な場合、一部のカラムの値は次のようになります `NULL`. このような部分は、以下で削除できます [ALTER TABLE DROP DETACHED PART](../sql-reference/statements/alter.md#alter_drop-detached).

## システム辞書 {#system_tables-dictionaries}

についての情報が含まれて [外部辞書](../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

列:

-   `database` ([文字列](../sql-reference/data-types/string.md)) — Name of the database containing the dictionary created by DDL query. Empty string for other dictionaries.
-   `name` ([文字列](../sql-reference/data-types/string.md)) — [辞書名](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict.md).
-   `status` ([Enum8](../sql-reference/data-types/enum.md)) — Dictionary status. Possible values:
    -   `NOT_LOADED` — Dictionary was not loaded because it was not used.
    -   `LOADED` — Dictionary loaded successfully.
    -   `FAILED` — Unable to load the dictionary as a result of an error.
    -   `LOADING` — Dictionary is loading now.
    -   `LOADED_AND_RELOADING` — Dictionary is loaded successfully, and is being reloaded right now (frequent reasons: [SYSTEM RELOAD DICTIONARY](../sql-reference/statements/system.md#query_language-system-reload-dictionary) クエリ、タイムアウト、辞書の設定が変更されました）。
    -   `FAILED_AND_RELOADING` — Could not load the dictionary as a result of an error and is loading now.
-   `origin` ([文字列](../sql-reference/data-types/string.md)) — Path to the configuration file that describes the dictionary.
-   `type` ([文字列](../sql-reference/data-types/string.md)) — Type of a dictionary allocation. [メモリへの辞書の格納](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md).
-   `key` — [キータイプ](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-key):数値キー ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) or Сomposite key ([文字列](../sql-reference/data-types/string.md)) — form “(type 1, type 2, …, type n)”.
-   `attribute.names` ([配列](../sql-reference/data-types/array.md)([文字列](../sql-reference/data-types/string.md))) — Array of [属性名](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes) 辞書によって提供されます。
-   `attribute.types` ([配列](../sql-reference/data-types/array.md)([文字列](../sql-reference/data-types/string.md))) — Corresponding array of [属性タイプ](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes) それは辞書によって提供されます。
-   `bytes_allocated` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Amount of RAM allocated for the dictionary.
-   `query_count` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of queries since the dictionary was loaded or since the last successful reboot.
-   `hit_rate` ([Float64](../sql-reference/data-types/float.md)) — For cache dictionaries, the percentage of uses for which the value was in the cache.
-   `element_count` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of items stored in the dictionary.
-   `load_factor` ([Float64](../sql-reference/data-types/float.md)) — Percentage filled in the dictionary (for a hashed dictionary, the percentage filled in the hash table).
-   `source` ([文字列](../sql-reference/data-types/string.md)) — Text describing the [データソース](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md) 辞書のために。
-   `lifetime_min` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Minimum [生涯](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) その後、ClickHouseは辞書をリロードしようとします（もし `invalidate_query` それが変更された場合にのみ、設定されています）。 秒単位で設定します。
-   `lifetime_max` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Maximum [生涯](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) その後、ClickHouseは辞書をリロードしようとします（もし `invalidate_query` それが変更された場合にのみ、設定されています）。 秒単位で設定します。
-   `loading_start_time` ([DateTime](../sql-reference/data-types/datetime.md)) — Start time for loading the dictionary.
-   `last_successful_update_time` ([DateTime](../sql-reference/data-types/datetime.md)) — End time for loading or updating the dictionary. Helps to monitor some troubles with external sources and investigate causes.
-   `loading_duration` ([Float32](../sql-reference/data-types/float.md)) — Duration of a dictionary loading.
-   `last_exception` ([文字列](../sql-reference/data-types/string.md)) — Text of the error that occurs when creating or reloading the dictionary if the dictionary couldn't be created.

**例**

辞書を設定します。

``` sql
CREATE DICTIONARY dictdb.dict
(
    `key` Int64 DEFAULT -1,
    `value_default` String DEFAULT 'world',
    `value_expression` String DEFAULT 'xxx' EXPRESSION 'toString(127 * 172)'
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'dicttbl' DB 'dictdb'))
LIFETIME(MIN 0 MAX 1)
LAYOUT(FLAT())
```

辞書が読み込まれていることを確認します。

``` sql
SELECT * FROM system.dictionaries
```

``` text
┌─database─┬─name─┬─status─┬─origin──────┬─type─┬─key────┬─attribute.names──────────────────────┬─attribute.types─────┬─bytes_allocated─┬─query_count─┬─hit_rate─┬─element_count─┬───────────load_factor─┬─source─────────────────────┬─lifetime_min─┬─lifetime_max─┬──loading_start_time─┌──last_successful_update_time─┬──────loading_duration─┬─last_exception─┐
│ dictdb   │ dict │ LOADED │ dictdb.dict │ Flat │ UInt64 │ ['value_default','value_expression'] │ ['String','String'] │           74032 │           0 │        1 │             1 │ 0.0004887585532746823 │ ClickHouse: dictdb.dicttbl │            0 │            1 │ 2020-03-04 04:17:34 │   2020-03-04 04:30:34        │                 0.002 │                │
└──────────┴──────┴────────┴─────────────┴──────┴────────┴──────────────────────────────────────┴─────────────────────┴─────────────────┴─────────────┴──────────┴───────────────┴───────────────────────┴────────────────────────────┴──────────────┴──────────────┴─────────────────────┴──────────────────────────────┘───────────────────────┴────────────────┘
```

## システムイベント {#system_tables-events}

システムで発生したイベントの数に関する情報が含まれます。 例えば、テーブルでどのように多くの `SELECT` ClickHouseサーバーが起動してからクエリが処理されました。

列:

-   `event` ([文字列](../sql-reference/data-types/string.md)) — Event name.
-   `value` ([UInt64](../sql-reference/data-types/int-uint.md)) — Number of events occurred.
-   `description` ([文字列](../sql-reference/data-types/string.md)) — Event description.

**例**

``` sql
SELECT * FROM system.events LIMIT 5
```

``` text
┌─event─────────────────────────────────┬─value─┬─description────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Query                                 │    12 │ Number of queries to be interpreted and potentially executed. Does not include queries that failed to parse or were rejected due to AST size limits, quota limits or limits on the number of simultaneously running queries. May include internal queries initiated by ClickHouse itself. Does not count subqueries.                  │
│ SelectQuery                           │     8 │ Same as Query, but only for SELECT queries.                                                                                                                                                                                                                │
│ FileOpen                              │    73 │ Number of files opened.                                                                                                                                                                                                                                    │
│ ReadBufferFromFileDescriptorRead      │   155 │ Number of reads (read/pread) from a file descriptor. Does not include sockets.                                                                                                                                                                             │
│ ReadBufferFromFileDescriptorReadBytes │  9931 │ Number of bytes read from file descriptors. If the file is compressed, this will show the compressed data size.                                                                                                                                              │
└───────────────────────────────────────┴───────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**も参照。**

-   [システムasynchronous\_metrics](#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [システムメトリック](#system_tables-metrics) — Contains instantly calculated metrics.
-   [システムmetric\_log](#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.
-   [監視](monitoring.md) — Base concepts of ClickHouse monitoring.

## システム関数 {#system-functions}

標準関数と集計関数に関する情報が含まれます。

列:

-   `name`(`String`) – The name of the function.
-   `is_aggregate`(`UInt8`) — Whether the function is aggregate.

## システムgraphite\_retentions {#system-graphite-retentions}

パラメ [graphite\_rollup](server-configuration-parameters/settings.md#server_configuration_parameters-graphite) テーブルで使用される [\*GraphiteMergeTree](../engines/table-engines/mergetree-family/graphitemergetree.md) エンジンだ

列:

-   `config_name` (文字列) - `graphite_rollup` パラメータ名。
-   `regexp` (String)-メトリック名のパターン。
-   `function` (String)-集計関数の名前。
-   `age` (UInt64)-データの最小年齢を秒単位で表します。
-   `precision` (UInt64)-データの年齢を秒単位で正確に定義する方法。
-   `priority` (UInt16)-パターンの優先度。
-   `is_default` (UInt8)-パターンがデフォルトかどうか。
-   `Tables.database` (Array(String))-データベーステーブルの名前の配列。 `config_name` パラメータ。
-   `Tables.table` (Array(String))-テーブル名の配列 `config_name` パラメータ。

## システムマージ {#system-merges}

MergeTreeファミリー内のテーブルで現在処理中のマージおよびパートの変異に関する情報が含まれます。

列:

-   `database` (String) — The name of the database the table is in.
-   `table` (String) — Table name.
-   `elapsed` (Float64) — The time elapsed (in seconds) since the merge started.
-   `progress` (Float64) — The percentage of completed work from 0 to 1.
-   `num_parts` (UInt64) — The number of pieces to be merged.
-   `result_part_name` (String) — The name of the part that will be formed as the result of merging.
-   `is_mutation` (UInt8)-1このプロセスが部分突然変異である場合。
-   `total_size_bytes_compressed` (UInt64) — The total size of the compressed data in the merged chunks.
-   `total_size_marks` (UInt64) — The total number of marks in the merged parts.
-   `bytes_read_uncompressed` (UInt64) — Number of bytes read, uncompressed.
-   `rows_read` (UInt64) — Number of rows read.
-   `bytes_written_uncompressed` (UInt64) — Number of bytes written, uncompressed.
-   `rows_written` (UInt64) — Number of rows written.

## システムメトリック {#system_tables-metrics}

即座に計算できるメトリック、または現在の値が含まれます。 たとえば、同時に処理されたクエリの数や現在のレプリカ遅延などです。 このテーブルは常に最新です。

列:

-   `metric` ([文字列](../sql-reference/data-types/string.md)) — Metric name.
-   `value` ([Int64](../sql-reference/data-types/int-uint.md)) — Metric value.
-   `description` ([文字列](../sql-reference/data-types/string.md)) — Metric description.

サポートされている指標のリストは、次のとおりです [src/Common/CurrentMetrics。cpp](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/CurrentMetrics.cpp) ClickHouseのソースファイル。

**例**

``` sql
SELECT * FROM system.metrics LIMIT 10
```

``` text
┌─metric─────────────────────┬─value─┬─description──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Query                      │     1 │ Number of executing queries                                                                                                                                                                      │
│ Merge                      │     0 │ Number of executing background merges                                                                                                                                                            │
│ PartMutation               │     0 │ Number of mutations (ALTER DELETE/UPDATE)                                                                                                                                                        │
│ ReplicatedFetch            │     0 │ Number of data parts being fetched from replicas                                                                                                                                                │
│ ReplicatedSend             │     0 │ Number of data parts being sent to replicas                                                                                                                                                      │
│ ReplicatedChecks           │     0 │ Number of data parts checking for consistency                                                                                                                                                    │
│ BackgroundPoolTask         │     0 │ Number of active tasks in BackgroundProcessingPool (merges, mutations, fetches, or replication queue bookkeeping)                                                                                │
│ BackgroundSchedulePoolTask │     0 │ Number of active tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old data parts, altering data parts, replica re-initialization, etc.   │
│ DiskSpaceReservedForMerge  │     0 │ Disk space reserved for currently running background merges. It is slightly more than the total size of currently merging parts.                                                                     │
│ DistributedSend            │     0 │ Number of connections to remote servers sending data that was INSERTed into Distributed tables. Both synchronous and asynchronous mode.                                                          │
└────────────────────────────┴───────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**も参照。**

-   [システムasynchronous\_metrics](#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [システムイベント](#system_tables-events) — Contains a number of events that occurred.
-   [システムmetric\_log](#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.
-   [監視](monitoring.md) — Base concepts of ClickHouse monitoring.

## システムmetric\_log {#system_tables-metric_log}

を含む履歴メトリクスの値からテーブル `system.metrics` と `system.events`、定期的にディスクにフラッシュ。
メトリック履歴の収集を有効にするには `system.metric_log`,作成 `/etc/clickhouse-server/config.d/metric_log.xml` 次の内容を使って:

``` xml
<yandex>
    <metric_log>
        <database>system</database>
        <table>metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
    </metric_log>
</yandex>
```

**例**

``` sql
SELECT * FROM system.metric_log LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
event_date:                                                 2020-02-18
event_time:                                                 2020-02-18 07:15:33
milliseconds:                                               554
ProfileEvent_Query:                                         0
ProfileEvent_SelectQuery:                                   0
ProfileEvent_InsertQuery:                                   0
ProfileEvent_FileOpen:                                      0
ProfileEvent_Seek:                                          0
ProfileEvent_ReadBufferFromFileDescriptorRead:              1
ProfileEvent_ReadBufferFromFileDescriptorReadFailed:        0
ProfileEvent_ReadBufferFromFileDescriptorReadBytes:         0
ProfileEvent_WriteBufferFromFileDescriptorWrite:            1
ProfileEvent_WriteBufferFromFileDescriptorWriteFailed:      0
ProfileEvent_WriteBufferFromFileDescriptorWriteBytes:       56
...
CurrentMetric_Query:                                        0
CurrentMetric_Merge:                                        0
CurrentMetric_PartMutation:                                 0
CurrentMetric_ReplicatedFetch:                              0
CurrentMetric_ReplicatedSend:                               0
CurrentMetric_ReplicatedChecks:                             0
...
```

**も参照。**

-   [システムasynchronous\_metrics](#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [システムイベント](#system_tables-events) — Contains a number of events that occurred.
-   [システムメトリック](#system_tables-metrics) — Contains instantly calculated metrics.
-   [監視](monitoring.md) — Base concepts of ClickHouse monitoring.

## システム数字 {#system-numbers}

このテーブルを一UInt64カラム名 ‘number’ ゼロから始まるほぼすべての自然数が含まれています。
このテーブルは、テストのため、またはブルートフォース検索を行う必要がある場合に使用できます。
この表からの読み取りは並列化されません。

## システムnumbers\_mt {#system-numbers-mt}

と同じ ‘system.numbers’ しかし、読み取りは並列処理されます。 番号は任意の順序で返すことができます。
テストに使用されます。

## システムワン {#system-one}

このテーブルには、単一の行が含まれています。 ‘dummy’ UInt8値を含む列0。
このテーブルは、SELECTクエリでFROM句が指定されていない場合に使用されます。
これは、他のDbmsにあるデュアルテーブルに似ています。

## システム部品 {#system_tables-parts}

についての情報が含まれて部品の [メルゲツリー](../engines/table-engines/mergetree-family/mergetree.md) テーブル

各行は、一つのデータ部分を記述します。

列:

-   `partition` (String) – The partition name. To learn what a partition is, see the description of the [ALTER](../sql-reference/statements/alter.md#query_language_queries_alter) クエリ。

    形式:

    -   `YYYYMM` 月別の自動パーティション分割の場合。
    -   `any_string` 手動で分割する場合。

-   `name` (`String`) – Name of the data part.

-   `active` (`UInt8`) – Flag that indicates whether the data part is active. If a data part is active, it's used in a table. Otherwise, it's deleted. Inactive data parts remain after merging.

-   `marks` (`UInt64`) – The number of marks. To get the approximate number of rows in a data part, multiply `marks` インデックスの粒度（通常は8192）で指定します（このヒントは適応的な粒度では機能しません）。

-   `rows` (`UInt64`) – The number of rows.

-   `bytes_on_disk` (`UInt64`) – Total size of all the data part files in bytes.

-   `data_compressed_bytes` (`UInt64`) – Total size of compressed data in the data part. All the auxiliary files (for example, files with marks) are not included.

-   `data_uncompressed_bytes` (`UInt64`) – Total size of uncompressed data in the data part. All the auxiliary files (for example, files with marks) are not included.

-   `marks_bytes` (`UInt64`) – The size of the file with marks.

-   `modification_time` (`DateTime`) – The time the directory with the data part was modified. This usually corresponds to the time of data part creation.\|

-   `remove_time` (`DateTime`) – The time when the data part became inactive.

-   `refcount` (`UInt32`) – The number of places where the data part is used. A value greater than 2 indicates that the data part is used in queries or merges.

-   `min_date` (`Date`) – The minimum value of the date key in the data part.

-   `max_date` (`Date`) – The maximum value of the date key in the data part.

-   `min_time` (`DateTime`) – The minimum value of the date and time key in the data part.

-   `max_time`(`DateTime`) – The maximum value of the date and time key in the data part.

-   `partition_id` (`String`) – ID of the partition.

-   `min_block_number` (`UInt64`) – The minimum number of data parts that make up the current part after merging.

-   `max_block_number` (`UInt64`) – The maximum number of data parts that make up the current part after merging.

-   `level` (`UInt32`) – Depth of the merge tree. Zero means that the current part was created by insert rather than by merging other parts.

-   `data_version` (`UInt64`) – Number that is used to determine which mutations should be applied to the data part (mutations with a version higher than `data_version`).

-   `primary_key_bytes_in_memory` (`UInt64`) – The amount of memory (in bytes) used by primary key values.

-   `primary_key_bytes_in_memory_allocated` (`UInt64`) – The amount of memory (in bytes) reserved for primary key values.

-   `is_frozen` (`UInt8`) – Flag that shows that a partition data backup exists. 1, the backup exists. 0, the backup doesn't exist. For more details, see [FREEZE PARTITION](../sql-reference/statements/alter.md#alter_freeze-partition)

-   `database` (`String`) – Name of the database.

-   `table` (`String`) – Name of the table.

-   `engine` (`String`) – Name of the table engine without parameters.

-   `path` (`String`) – Absolute path to the folder with data part files.

-   `disk` (`String`) – Name of a disk that stores the data part.

-   `hash_of_all_files` (`String`) – [sipHash128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) 圧縮されたファイルの。

-   `hash_of_uncompressed_files` (`String`) – [sipHash128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) 非圧縮ファイル（マーク付きファイル、インデックスファイルなど。).

-   `uncompressed_hash_of_compressed_files` (`String`) – [sipHash128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) それらが圧縮されていないかのように圧縮されたファイル内のデータの。

-   `bytes` (`UInt64`) – Alias for `bytes_on_disk`.

-   `marks_size` (`UInt64`) – Alias for `marks_bytes`.

## システムpart\_log {#system_tables-part-log}

その `system.part_log` テーブルが作成されるのは、 [part\_log](server-configuration-parameters/settings.md#server_configuration_parameters-part-log) サーバ設定を指定します。

このテーブルについての情報が含まれてイベントが発生した [データパーツ](../engines/table-engines/mergetree-family/custom-partitioning-key.md) で [メルゲツリー](../engines/table-engines/mergetree-family/mergetree.md) データの追加やマージなどのファミリテーブル。

その `system.part_log` テーブルを含む以下のカラム:

-   `event_type` (Enum) — Type of the event that occurred with the data part. Can have one of the following values:
    -   `NEW_PART` — Inserting of a new data part.
    -   `MERGE_PARTS` — Merging of data parts.
    -   `DOWNLOAD_PART` — Downloading a data part.
    -   `REMOVE_PART` — Removing or detaching a data part using [DETACH PARTITION](../sql-reference/statements/alter.md#alter_detach-partition).
    -   `MUTATE_PART` — Mutating of a data part.
    -   `MOVE_PART` — Moving the data part from the one disk to another one.
-   `event_date` (Date) — Event date.
-   `event_time` (DateTime) — Event time.
-   `duration_ms` (UInt64) — Duration.
-   `database` (String) — Name of the database the data part is in.
-   `table` (String) — Name of the table the data part is in.
-   `part_name` (String) — Name of the data part.
-   `partition_id` (String) — ID of the partition that the data part was inserted to. The column takes the ‘all’ パーティション分割が `tuple()`.
-   `rows` (UInt64) — The number of rows in the data part.
-   `size_in_bytes` (UInt64) — Size of the data part in bytes.
-   `merged_from` (Array(String)) — An array of names of the parts which the current part was made up from (after the merge).
-   `bytes_uncompressed` (UInt64) — Size of uncompressed bytes.
-   `read_rows` (UInt64) — The number of rows was read during the merge.
-   `read_bytes` (UInt64) — The number of bytes was read during the merge.
-   `error` (UInt16) — The code number of the occurred error.
-   `exception` (String) — Text message of the occurred error.

その `system.part_log` テーブルは、最初にデータを挿入した後に作成されます。 `MergeTree` テーブル。

## システムプロセス {#system_tables-processes}

このシステムテーブルは、 `SHOW PROCESSLIST` クエリ。

列:

-   `user` (String) – The user who made the query. Keep in mind that for distributed processing, queries are sent to remote servers under the `default` ユーザー。 の分野のユーザー名で特定のクエリは、クエリはこのクエリも開始しています。
-   `address` (String) – The IP address the request was made from. The same for distributed processing. To track where a distributed query was originally made from, look at `system.processes` クエリ要求サーバー上。
-   `elapsed` (Float64) – The time in seconds since request execution started.
-   `rows_read` (UInt64) – The number of rows read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.
-   `bytes_read` (UInt64) – The number of uncompressed bytes read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.
-   `total_rows_approx` (UInt64) – The approximation of the total number of rows that should be read. For distributed processing, on the requestor server, this is the total for all remote servers. It can be updated during request processing, when new sources to process become known.
-   `memory_usage` (UInt64) – Amount of RAM the request uses. It might not include some types of dedicated memory. See the [max\_memory\_usage](../operations/settings/query-complexity.md#settings_max_memory_usage) 設定。
-   `query` (String) – The query text. For `INSERT`,挿入するデータは含まれません。
-   `query_id` (String) – Query ID, if defined.

## システムtext\_log {#system-tables-text-log}

を含むログイン作品の応募がありました。 ログレベルがこのテーブルで限定 `text_log.level` サーバー設定。

列:

-   `event_date` (`Date`）-エントリの日付。
-   `event_time` (`DateTime`）-エントリの時間。
-   `microseconds` (`UInt32`)-エントリのマイクロ秒。
-   `thread_name` (String) — Name of the thread from which the logging was done.
-   `thread_id` (UInt64) — OS thread ID.
-   `level` (`Enum8`）-エントリーレベル。
    -   `'Fatal' = 1`
    -   `'Critical' = 2`
    -   `'Error' = 3`
    -   `'Warning' = 4`
    -   `'Notice' = 5`
    -   `'Information' = 6`
    -   `'Debug' = 7`
    -   `'Trace' = 8`
-   `query_id` (`String`)-クエリのID。
-   `logger_name` (`LowCardinality(String)`) - Name of the logger (i.e. `DDLWorker`)
-   `message` (`String`）-メッセージ自体。
-   `revision` (`UInt32`）-ClickHouseリビジョン。
-   `source_file` (`LowCardinality(String)`)-ロギングが行われたソースファイル。
-   `source_line` (`UInt64`)-ロギングが行われたソース行。

## システムquery\_log {#system_tables-query_log}

クエリの実行に関する情報が含まれます。 クエリごとに、処理開始時間、処理時間、エラーメッセージおよびその他の情報を確認できます。

!!! note "注"
    テーブルには以下の入力データは含まれません `INSERT` クエリ。

ClickHouseはこのテーブルを作成します。 [query\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) serverパラメータを指定します。 このパラメーターは、クエリがログインするテーブルのログ間隔や名前などのログルールを設定します。

クエリロギングを有効にするには、 [log\_queries](settings/settings.md#settings-log-queries) パラメータは1。 詳細については、 [設定](settings/settings.md) セクション

その `system.query_log` テーブルレジスタの種類は問合せ:

1.  クライアントによって直接実行された初期クエリ。
2.  他のクエリによって開始された子クエリ(分散クエリ実行用)。 これらのタイプのクエリについては、親クエリに関する情報が `initial_*` 列。

列:

-   `type` (`Enum8`) — Type of event that occurred when executing the query. Values:
    -   `'QueryStart' = 1` — Successful start of query execution.
    -   `'QueryFinish' = 2` — Successful end of query execution.
    -   `'ExceptionBeforeStart' = 3` — Exception before the start of query execution.
    -   `'ExceptionWhileProcessing' = 4` — Exception during the query execution.
-   `event_date` (Date) — Query starting date.
-   `event_time` (DateTime) — Query starting time.
-   `query_start_time` (DateTime) — Start time of query execution.
-   `query_duration_ms` (UInt64) — Duration of query execution.
-   `read_rows` (UInt64) — Number of read rows.
-   `read_bytes` (UInt64) — Number of read bytes.
-   `written_rows` (UInt64) — For `INSERT` クエリ、書き込まれた行の数。 その他のクエリの場合、列の値は0です。
-   `written_bytes` (UInt64) — For `INSERT` クエリ、書き込まれたバイト数。 その他のクエリの場合、列の値は0です。
-   `result_rows` (UInt64) — Number of rows in the result.
-   `result_bytes` (UInt64) — Number of bytes in the result.
-   `memory_usage` (UInt64) — Memory consumption by the query.
-   `query` (String) — Query string.
-   `exception` (String) — Exception message.
-   `stack_trace` (String) — Stack trace (a list of methods called before the error occurred). An empty string, if the query is completed successfully.
-   `is_initial_query` (UInt8) — Query type. Possible values:
    -   1 — Query was initiated by the client.
    -   0 — Query was initiated by another query for distributed query execution.
-   `user` (String) — Name of the user who initiated the current query.
-   `query_id` (String) — ID of the query.
-   `address` (IPv6) — IP address that was used to make the query.
-   `port` (UInt16) — The client port that was used to make the query.
-   `initial_user` (String) — Name of the user who ran the initial query (for distributed query execution).
-   `initial_query_id` (String) — ID of the initial query (for distributed query execution).
-   `initial_address` (IPv6) — IP address that the parent query was launched from.
-   `initial_port` (UInt16) — The client port that was used to make the parent query.
-   `interface` (UInt8) — Interface that the query was initiated from. Possible values:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` (String) — OS's username who runs [clickhouse-クライアント](../interfaces/cli.md).
-   `client_hostname` (String) — Hostname of the client machine where the [clickhouse-クライアント](../interfaces/cli.md) または他のTCPクライアントが実行されます。
-   `client_name` (String) — The [clickhouse-クライアント](../interfaces/cli.md) または別のTCPクライアント名。
-   `client_revision` (UInt32) — Revision of the [clickhouse-クライアント](../interfaces/cli.md) または別のTCPクライアント。
-   `client_version_major` (UInt32) — Major version of the [clickhouse-クライアント](../interfaces/cli.md) または別のTCPクライアント。
-   `client_version_minor` (UInt32) — Minor version of the [clickhouse-クライアント](../interfaces/cli.md) または別のTCPクライアント。
-   `client_version_patch` (UInt32) — Patch component of the [clickhouse-クライアント](../interfaces/cli.md) 別のTCPクライアントバージョン。
-   `http_method` (UInt8) — HTTP method that initiated the query. Possible values:
    -   0 — The query was launched from the TCP interface.
    -   1 — `GET` 方法を用いた。
    -   2 — `POST` 方法を用いた。
-   `http_user_agent` (String) — The `UserAgent` HTTP要求で渡されるヘッダー。
-   `quota_key` (String) — The “quota key” で指定される。 [クォータ](quotas.md) 設定(参照 `keyed`).
-   `revision` (UInt32) — ClickHouse revision.
-   `thread_numbers` (Array(UInt32)) — Number of threads that are participating in query execution.
-   `ProfileEvents.Names` (Array(String)) — Counters that measure different metrics. The description of them could be found in the table [システムイベント](#system_tables-events)
-   `ProfileEvents.Values` (Array(UInt64)) — Values of metrics that are listed in the `ProfileEvents.Names` 列。
-   `Settings.Names` (Array(String)) — Names of settings that were changed when the client ran the query. To enable logging changes to settings, set the `log_query_settings` パラメータは1。
-   `Settings.Values` (Array(String)) — Values of settings that are listed in the `Settings.Names` 列。

それぞれのクエリでは、一つまたは二つの行が `query_log` クエリのステータスに応じて、テーブル:

1.  クエリの実行が成功すると、タイプ1とタイプ2のイベントが作成されます。 `type` 列）。
2.  クエリ処理中にエラーが発生した場合は、タイプ1と4のイベントが作成されます。
3.  クエリを起動する前にエラーが発生した場合は、タイプ3の単一のイベントが作成されます。

既定では、ログは7.5秒間隔でテーブルに追加されます。 この間隔は [query\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) サーバ設定(参照 `flush_interval_milliseconds` 変数）。 ログをメモリバッファからテーブルに強制的にフラッシュするには、 `SYSTEM FLUSH LOGS` クエリ。

テーブルを手動で削除すると、その場で自動的に作成されます。 以前のログはすべて削除されます。

!!! note "注"
    ログの保存期間は無制限です。 ログはテーブルから自動的には削除されません。 古いログの削除を自分で整理する必要があります。

パーティショニングキーを指定できます。 `system.query_log` のテーブル [query\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) サーバ設定(参照 `partition_by` 変数）。

## システムquery\_thread\_log {#system_tables-query-thread-log}

のテーブルについての情報が含まれてそれぞれの検索キーワード実行スレッド.

ClickHouseはこのテーブルを作成します。 [query\_thread\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) serverパラメータを指定します。 このパラメーターは、クエリがログインするテーブルのログ間隔や名前などのログルールを設定します。

クエリロギングを有効にするには、 [log\_query\_threads](settings/settings.md#settings-log-query-threads) パラメータは1。 詳細については、 [設定](settings/settings.md) セクション

列:

-   `event_date` (Date) — the date when the thread has finished execution of the query.
-   `event_time` (DateTime) — the date and time when the thread has finished execution of the query.
-   `query_start_time` (DateTime) — Start time of query execution.
-   `query_duration_ms` (UInt64) — Duration of query execution.
-   `read_rows` (UInt64) — Number of read rows.
-   `read_bytes` (UInt64) — Number of read bytes.
-   `written_rows` (UInt64) — For `INSERT` クエリ、書き込まれた行の数。 その他のクエリの場合、列の値は0です。
-   `written_bytes` (UInt64) — For `INSERT` クエリ、書き込まれたバイト数。 その他のクエリの場合、列の値は0です。
-   `memory_usage` (Int64) — The difference between the amount of allocated and freed memory in context of this thread.
-   `peak_memory_usage` (Int64) — The maximum difference between the amount of allocated and freed memory in context of this thread.
-   `thread_name` (String) — Name of the thread.
-   `thread_number` (UInt32) — Internal thread ID.
-   `os_thread_id` (Int32) — OS thread ID.
-   `master_thread_id` (UInt64) — OS initial ID of initial thread.
-   `query` (String) — Query string.
-   `is_initial_query` (UInt8) — Query type. Possible values:
    -   1 — Query was initiated by the client.
    -   0 — Query was initiated by another query for distributed query execution.
-   `user` (String) — Name of the user who initiated the current query.
-   `query_id` (String) — ID of the query.
-   `address` (IPv6) — IP address that was used to make the query.
-   `port` (UInt16) — The client port that was used to make the query.
-   `initial_user` (String) — Name of the user who ran the initial query (for distributed query execution).
-   `initial_query_id` (String) — ID of the initial query (for distributed query execution).
-   `initial_address` (IPv6) — IP address that the parent query was launched from.
-   `initial_port` (UInt16) — The client port that was used to make the parent query.
-   `interface` (UInt8) — Interface that the query was initiated from. Possible values:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` (String) — OS's username who runs [clickhouse-クライアント](../interfaces/cli.md).
-   `client_hostname` (String) — Hostname of the client machine where the [clickhouse-クライアント](../interfaces/cli.md) または他のTCPクライアントが実行されます。
-   `client_name` (String) — The [clickhouse-クライアント](../interfaces/cli.md) または別のTCPクライアント名。
-   `client_revision` (UInt32) — Revision of the [clickhouse-クライアント](../interfaces/cli.md) または別のTCPクライアント。
-   `client_version_major` (UInt32) — Major version of the [clickhouse-クライアント](../interfaces/cli.md) または別のTCPクライアント。
-   `client_version_minor` (UInt32) — Minor version of the [clickhouse-クライアント](../interfaces/cli.md) または別のTCPクライアント。
-   `client_version_patch` (UInt32) — Patch component of the [clickhouse-クライアント](../interfaces/cli.md) 別のTCPクライアントバージョン。
-   `http_method` (UInt8) — HTTP method that initiated the query. Possible values:
    -   0 — The query was launched from the TCP interface.
    -   1 — `GET` 方法を用いた。
    -   2 — `POST` 方法を用いた。
-   `http_user_agent` (String) — The `UserAgent` HTTP要求で渡されるヘッダー。
-   `quota_key` (String) — The “quota key” で指定される。 [クォータ](quotas.md) 設定(参照 `keyed`).
-   `revision` (UInt32) — ClickHouse revision.
-   `ProfileEvents.Names` (Array(String)) — Counters that measure different metrics for this thread. The description of them could be found in the table [システムイベント](#system_tables-events)
-   `ProfileEvents.Values` (Array(UInt64)) — Values of metrics for this thread that are listed in the `ProfileEvents.Names` 列。

既定では、ログは7.5秒間隔でテーブルに追加されます。 この間隔は [query\_thread\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) サーバ設定(参照 `flush_interval_milliseconds` 変数）。 ログをメモリバッファからテーブルに強制的にフラッシュするには、 `SYSTEM FLUSH LOGS` クエリ。

テーブルを手動で削除すると、その場で自動的に作成されます。 以前のログはすべて削除されます。

!!! note "注"
    ログの保存期間は無制限です。 ログはテーブルから自動的には削除されません。 古いログの削除を自分で整理する必要があります。

パーティショニングキーを指定できます。 `system.query_thread_log` のテーブル [query\_thread\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) サーバ設定(参照 `partition_by` 変数）。

## システムtrace\_log {#system_tables-trace_log}

を含むスタックトレースの収集、サンプリングクロファイラ.

ClickHouseはこのテーブルを作成します。 [trace\_log](server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) サーバの設定が設定されます。 また、 [query\_profiler\_real\_time\_period\_ns](settings/settings.md#query_profiler_real_time_period_ns) と [query\_profiler\_cpu\_time\_period\_ns](settings/settings.md#query_profiler_cpu_time_period_ns) 設定は設定する必要があります。

ログを分析するには、 `addressToLine`, `addressToSymbol` と `demangle` イントロスペクション関数。

列:

-   `event_date` ([日付](../sql-reference/data-types/date.md)) — Date of sampling moment.

-   `event_time` ([DateTime](../sql-reference/data-types/datetime.md)) — Timestamp of the sampling moment.

-   `timestamp_ns` ([UInt64](../sql-reference/data-types/int-uint.md)) — Timestamp of the sampling moment in nanoseconds.

-   `revision` ([UInt32](../sql-reference/data-types/int-uint.md)) — ClickHouse server build revision.

    サーバーに接続するとき `clickhouse-client` のような文字列を参照してください `Connected to ClickHouse server version 19.18.1 revision 54429.`. このフィールドには `revision` ではなく、 `version` サーバーの。

-   `timer_type` ([Enum8](../sql-reference/data-types/enum.md)) — Timer type:

    -   `Real` 壁時計の時間を表します。
    -   `CPU` CPU時間を表します。

-   `thread_number` ([UInt32](../sql-reference/data-types/int-uint.md)) — Thread identifier.

-   `query_id` ([文字列](../sql-reference/data-types/string.md)) — Query identifier that can be used to get details about a query that was running from the [query\_log](#system_tables-query_log) システムテーブル。

-   `trace` ([配列(UInt64)](../sql-reference/data-types/array.md)) — Stack trace at the moment of sampling. Each element is a virtual memory address inside ClickHouse server process.

**例**

``` sql
SELECT * FROM system.trace_log LIMIT 1 \G
```

``` text
Row 1:
──────
event_date:    2019-11-15
event_time:    2019-11-15 15:09:38
revision:      54428
timer_type:    Real
thread_number: 48
query_id:      acc4d61f-5bd1-4a3e-bc91-2180be37c915
trace:         [94222141367858,94222152240175,94222152325351,94222152329944,94222152330796,94222151449980,94222144088167,94222151682763,94222144088167,94222151682763,94222144088167,94222144058283,94222144059248,94222091840750,94222091842302,94222091831228,94222189631488,140509950166747,140509942945935]
```

## システムレプリカ {#system_tables-replicas}

情報および状況を再現しテーブル在住の地元のサーバーです。
このテーブルは監視に使用することができる。 のテーブルが含まれて行毎に再現\*ます。

例:

``` sql
SELECT *
FROM system.replicas
WHERE table = 'visits'
FORMAT Vertical
```

``` text
Row 1:
──────
database:                   merge
table:                      visits
engine:                     ReplicatedCollapsingMergeTree
is_leader:                  1
can_become_leader:          1
is_readonly:                0
is_session_expired:         0
future_parts:               1
parts_to_check:             0
zookeeper_path:             /clickhouse/tables/01-06/visits
replica_name:               example01-06-1.yandex.ru
replica_path:               /clickhouse/tables/01-06/visits/replicas/example01-06-1.yandex.ru
columns_version:            9
queue_size:                 1
inserts_in_queue:           0
merges_in_queue:            1
part_mutations_in_queue:    0
queue_oldest_time:          2020-02-20 08:34:30
inserts_oldest_time:        1970-01-01 00:00:00
merges_oldest_time:         2020-02-20 08:34:30
part_mutations_oldest_time: 1970-01-01 00:00:00
oldest_part_to_get:
oldest_part_to_merge_to:    20200220_20284_20840_7
oldest_part_to_mutate_to:
log_max_index:              596273
log_pointer:                596274
last_queue_update:          2020-02-20 08:34:32
absolute_delay:             0
total_replicas:             2
active_replicas:            2
```

列:

-   `database` (`String`)-データベース名
-   `table` (`String`)-テーブル名
-   `engine` (`String`)-テーブルエンジン名
-   `is_leader` (`UInt8`）-レプリカがリーダーであるかどうか。
    一度に一つのレプリカだけがリーダーになれます。 リーダーは、実行するバックグラウンドマージを選択します。
    書き込みは、リーダーであるかどうかに関係なく、使用可能でZKにセッションがある任意のレプリカに対して実行できます。
-   `can_become_leader` (`UInt8`）-レプリカがリーダーとして選出できるかどうか。
-   `is_readonly` (`UInt8`)-レプリカが読み取り専用モードであるかどうか。
    このモードは、設定にZooKeeperのセクションがない場合、zookeeperでセッションを再初期化するとき、およびZooKeeperでセッションを再初期化するときに不明なエラーが発生
-   `is_session_expired` (`UInt8`）-ZooKeeperとのセッションが終了しました。 基本的には `is_readonly`.
-   `future_parts` (`UInt32`)-まだ行われていない挿入またはマージの結果として表示されるデータパーツの数。
-   `parts_to_check` (`UInt32`)-検証のためのキュー内のデータ部分の数。 破損の疑いがある場合は、部品を検証キューに入れます。
-   `zookeeper_path` (`String`）-ZooKeeperのテーブルデータへのパス。
-   `replica_name` (`String`）-飼育係のレプリカ名。 同じテーブルの異なるレプリカの名前は異なります。
-   `replica_path` (`String`）-ZooKeeperのレプリカデータへのパス。 連結と同じ ‘zookeeper\_path/replicas/replica\_path’.
-   `columns_version` (`Int32`)-テーブル構造のバージョン番号。 変更が実行された回数を示します。 場合にレプリカは異なるバージョンで一部のレプリカさんのすべての変更はまだない。
-   `queue_size` (`UInt32`)-実行待ちの操作のキューのサイズ。 操作には、データのブロックの挿入、マージ、その他の特定の操作が含まれます。 それは通常と一致します `future_parts`.
-   `inserts_in_queue` (`UInt32`)-必要なデータブロックの挿入数。 挿入は通常、かなり迅速に複製されます。 この数が大きい場合は、何かが間違っていることを意味します。
-   `merges_in_queue` (`UInt32`)-行われるのを待っているマージの数。 時にはマージが長いので、この値は長い間ゼロより大きくなることがあります。
-   `part_mutations_in_queue` (`UInt32`）-作られるのを待っている突然変異の数。
-   `queue_oldest_time` (`DateTime`)-If `queue_size` 0より大きい場合は、最も古い操作がいつキューに追加されたかを示します。
-   `inserts_oldest_time` (`DateTime`）-参照 `queue_oldest_time`
-   `merges_oldest_time` (`DateTime`）-参照 `queue_oldest_time`
-   `part_mutations_oldest_time` (`DateTime`）-参照 `queue_oldest_time`

次の4つの列は、zkとのアクティブなセッションがある場合にのみゼロ以外の値を持ちます。

-   `log_max_index` (`UInt64`）-一般的な活動のログ内の最大エントリ番号。
-   `log_pointer` (`UInt64`)-レプリカがその実行キューにコピーした一般的なアクティビティのログ内の最大エントリ番号を加えたもの。 もし `log_pointer` はるかに小さい `log_max_index` 何かおかしい
-   `last_queue_update` (`DateTime`)-キューが前回updatedされたとき。
-   `absolute_delay` (`UInt64`）-現在のレプリカが持っている秒単位の大きな遅れ。
-   `total_replicas` (`UInt8`)-このテーブルの既知のレプリカの総数。
-   `active_replicas` (`UInt8`)-ZooKeeperにセッションがあるこのテーブルのレプリカの数(つまり、機能しているレプリカの数)。

すべての列を要求すると、ZooKeeperからのいくつかの読み取りが行ごとに行われるため、テーブルは少し遅く動作する可能性があります。
最後の4つの列（log\_max\_index、log\_pointer、total\_replicas、active\_replicas）を要求しないと、テーブルはすぐに機能します。

たとえば、次のようにすべてが正常に動作していることを確認できます:

``` sql
SELECT
    database,
    table,
    is_leader,
    is_readonly,
    is_session_expired,
    future_parts,
    parts_to_check,
    columns_version,
    queue_size,
    inserts_in_queue,
    merges_in_queue,
    log_max_index,
    log_pointer,
    total_replicas,
    active_replicas
FROM system.replicas
WHERE
       is_readonly
    OR is_session_expired
    OR future_parts > 20
    OR parts_to_check > 10
    OR queue_size > 20
    OR inserts_in_queue > 10
    OR log_max_index - log_pointer > 10
    OR total_replicas < 2
    OR active_replicas < total_replicas
```

このクエリが何も返さない場合は、すべて正常であることを意味します。

## システム設定 {#system-tables-system-settings}

現在のユ

列:

-   `name` ([文字列](../sql-reference/data-types/string.md)) — Setting name.
-   `value` ([文字列](../sql-reference/data-types/string.md)) — Setting value.
-   `changed` ([UInt8](../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether a setting is changed from its default value.
-   `description` ([文字列](../sql-reference/data-types/string.md)) — Short setting description.
-   `min` ([Null可能](../sql-reference/data-types/nullable.md)([文字列](../sql-reference/data-types/string.md))) — Minimum value of the setting, if any is set via [制約](settings/constraints-on-settings.md#constraints-on-settings). 最小値が設定されていない場合は、以下を含みます [NULL](../sql-reference/syntax.md#null-literal).
-   `max` ([Null可能](../sql-reference/data-types/nullable.md)([文字列](../sql-reference/data-types/string.md))) — Maximum value of the setting, if any is set via [制約](settings/constraints-on-settings.md#constraints-on-settings). 最大値が設定されていない場合は、以下を含みます [NULL](../sql-reference/syntax.md#null-literal).
-   `readonly` ([UInt8](../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether the current user can change the setting:
    -   `0` — Current user can change the setting.
    -   `1` — Current user can't change the setting.

**例**

次の例は、名前に含まれる設定に関する情報を取得する方法を示しています `min_i`.

``` sql
SELECT *
FROM system.settings
WHERE name LIKE '%min_i%'
```

``` text
┌─name────────────────────────────────────────┬─value─────┬─changed─┬─description───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─min──┬─max──┬─readonly─┐
│ min_insert_block_size_rows                  │ 1048576   │       0 │ Squash blocks passed to INSERT query to specified size in rows, if blocks are not big enough.                                                                         │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
│ min_insert_block_size_bytes                 │ 268435456 │       0 │ Squash blocks passed to INSERT query to specified size in bytes, if blocks are not big enough.                                                                        │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
│ read_backoff_min_interval_between_events_ms │ 1000      │       0 │ Settings to reduce the number of threads in case of slow reads. Do not pay attention to the event, if the previous one has passed less than a certain amount of time. │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
└─────────────────────────────────────────────┴───────────┴─────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────┴──────┴──────────┘
```

の使用 `WHERE changed` たとえば、チェックしたいときに便利です:

-   構成ファイルの設定が正しく読み込まれ、使用されているかどうか。
-   現在のセッションで変更された設定。

<!-- -->

``` sql
SELECT * FROM system.settings WHERE changed AND name='load_balancing'
```

**も参照。**

-   [設定](settings/index.md#session-settings-intro)
-   [クエリの権限](settings/permissions-for-queries.md#settings_readonly)
-   [設定の制約](settings/constraints-on-settings.md)

## システムtable\_engines {#system.table_engines}

``` text
┌─name───────────────────┬─value───────┐
│ max_threads            │ 8           │
│ use_uncompressed_cache │ 0           │
│ load_balancing         │ random      │
│ max_memory_usage       │ 10000000000 │
└────────────────────────┴─────────────┘
```

## システムmerge\_tree\_settings {#system-merge_tree_settings}

の設定に関する情報が含まれます `MergeTree` テーブル

列:

-   `name` (String) — Setting name.
-   `value` (String) — Setting value.
-   `description` (String) — Setting description.
-   `type` (String) — Setting type (implementation specific string value).
-   `changed` (UInt8) — Whether the setting was explicitly defined in the config or explicitly changed.

## システムtable\_engines {#system-table-engines}

を含むの記述のテーブルエンジンをサポートサーバーとその特徴を支援す。

このテーブル以下のカラムのカラムタイプはブラケット):

-   `name` (String) — The name of table engine.
-   `supports_settings` (UInt8) — Flag that indicates if table engine supports `SETTINGS` 句。
-   `supports_skipping_indices` (UInt8) — Flag that indicates if table engine supports [索引のスキップ](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-data_skipping-indexes).
-   `supports_ttl` (UInt8) — Flag that indicates if table engine supports [TTL](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).
-   `supports_sort_order` (UInt8) — Flag that indicates if table engine supports clauses `PARTITION_BY`, `PRIMARY_KEY`, `ORDER_BY` と `SAMPLE_BY`.
-   `supports_replication` (UInt8) — Flag that indicates if table engine supports [データ複製](../engines/table-engines/mergetree-family/replication.md).
-   `supports_duduplication` (UInt8) — Flag that indicates if table engine supports data deduplication.

例:

``` sql
SELECT *
FROM system.table_engines
WHERE name in ('Kafka', 'MergeTree', 'ReplicatedCollapsingMergeTree')
```

``` text
┌─name──────────────────────────┬─supports_settings─┬─supports_skipping_indices─┬─supports_sort_order─┬─supports_ttl─┬─supports_replication─┬─supports_deduplication─┐
│ Kafka                         │                 1 │                         0 │                   0 │            0 │                    0 │                      0 │
│ MergeTree                     │                 1 │                         1 │                   1 │            1 │                    0 │                      0 │
│ ReplicatedCollapsingMergeTree │                 1 │                         1 │                   1 │            1 │                    1 │                      1 │
└───────────────────────────────┴───────────────────┴───────────────────────────┴─────────────────────┴──────────────┴──────────────────────┴────────────────────────┘
```

**も参照。**

-   メルゲツリー族 [クエリ句](../engines/table-engines/mergetree-family/mergetree.md#mergetree-query-clauses)
-   カフカ [設定](../engines/table-engines/integrations/kafka.md#table_engine-kafka-creating-a-table)
-   参加 [設定](../engines/table-engines/special/join.md#join-limitations-and-settings)

## システムテーブル {#system-tables}

を含むメタデータは各テーブルサーバーに知っています。 デタッチされたテーブルは `system.tables`.

このテーブル以下のカラムのカラムタイプはブラケット):

-   `database` (String) — The name of the database the table is in.

-   `name` (String) — Table name.

-   `engine` (String) — Table engine name (without parameters).

-   `is_temporary` (UInt8)-テーブルが一時的かどうかを示すフラグ。

-   `data_path` (String)-ファイルシステム内のテーブルデータへのパス。

-   `metadata_path` (String)-ファイルシステム内のテーブルメタデータへのパス。

-   `metadata_modification_time` (DateTime)-テーブルメタデータの最新の変更時刻。

-   `dependencies_database` (Array(String))-データベースの依存関係。

-   `dependencies_table` (Array(String))-テーブルの依存関係 ([マテリアライズドビュー](../engines/table-engines/special/materializedview.md) 現在のテーブルに基づくテーブル）。

-   `create_table_query` (String)-テーブルの作成に使用されたクエリ。

-   `engine_full` (String)-テーブルエンジンのパラメータ。

-   `partition_key` (String)-テーブルで指定されたパーティションキー式。

-   `sorting_key` (String)-テーブルで指定されたソートキー式。

-   `primary_key` (String)-テーブルで指定された主キー式。

-   `sampling_key` (String)-テーブルで指定されたサンプリングキー式。

-   `storage_policy` (String)-ストレージポリシー:

    -   [メルゲツリー](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)
    -   [分散](../engines/table-engines/special/distributed.md#distributed)

-   `total_rows` (Nullable(UInt64))-テーブル内の正確な行数をすばやく決定できる場合は、行の合計数です。 `Null` （アンダーイングを含む `Buffer` 表）。

-   `total_bytes` (Nullable(UInt64))-ストレージ上のテーブルの正確なバイト数を迅速に決定できる場合は、合計バイト数。 `Null` (**ない** 基になるストレージを含む)。

    -   If the table stores data on disk, returns used space on disk (i.e. compressed).
    -   テーブルがメモリにデータを格納している場合は、メモリ内の使用バイト数の概算を返します。

その `system.tables` テーブルはで使用されます `SHOW TABLES` クエリの実装。

## システム飼育係 {#system-zookeeper}

ZooKeeperが設定されていない場合、テーブルは存在しません。 できるデータを読み込んで飼育係クラスタで定義され、config.
クエリには ‘path’ WHERE句の等価条件。 これは、データを取得する子供のためのZooKeeperのパスです。

クエリ `SELECT * FROM system.zookeeper WHERE path = '/clickhouse'` すべての子のデータを出力します。 `/clickhouse` ノード
すべてのルートノードのデータを出力するには、path= ‘/’.
指定されたパスの場合 ‘path’ 存在しない場合、例外がスローされます。

列:

-   `name` (String) — The name of the node.
-   `path` (String) — The path to the node.
-   `value` (String) — Node value.
-   `dataLength` (Int32) — Size of the value.
-   `numChildren` (Int32) — Number of descendants.
-   `czxid` (Int64) — ID of the transaction that created the node.
-   `mzxid` (Int64) — ID of the transaction that last changed the node.
-   `pzxid` (Int64) — ID of the transaction that last deleted or added descendants.
-   `ctime` (DateTime) — Time of node creation.
-   `mtime` (DateTime) — Time of the last modification of the node.
-   `version` (Int32) — Node version: the number of times the node was changed.
-   `cversion` (Int32) — Number of added or removed descendants.
-   `aversion` (Int32) — Number of changes to the ACL.
-   `ephemeralOwner` (Int64) — For ephemeral nodes, the ID of the session that owns this node.

例:

``` sql
SELECT *
FROM system.zookeeper
WHERE path = '/clickhouse/tables/01-08/visits/replicas'
FORMAT Vertical
```

``` text
Row 1:
──────
name:           example01-08-1.yandex.ru
value:
czxid:          932998691229
mzxid:          932998691229
ctime:          2015-03-27 16:49:51
mtime:          2015-03-27 16:49:51
version:        0
cversion:       47
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021031383
path:           /clickhouse/tables/01-08/visits/replicas

Row 2:
──────
name:           example01-08-2.yandex.ru
value:
czxid:          933002738135
mzxid:          933002738135
ctime:          2015-03-27 16:57:01
mtime:          2015-03-27 16:57:01
version:        0
cversion:       37
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021252247
path:           /clickhouse/tables/01-08/visits/replicas
```

## システム突然変異 {#system_tables-mutations}

のテーブルについての情報が含まれて [突然変異](../sql-reference/statements/alter.md#alter-mutations) マージツリーテーブルとその進捗状況。 各突然変異コマンドは単一の行で表されます。 テーブルには次の列があります:

**データ**, **テーブル** -突然変異が適用されたデータベースとテーブルの名前。

**mutation\_id** -突然変異のID。 レプリケートされたテーブルの場合、これらのIdは `<table_path_in_zookeeper>/mutations/` 飼育係のディレクトリ。 未複製テーブルの場合、Idはテーブルのデータディレクトリ内のファイル名に対応します。

**コマンド** -突然変異コマンド文字列（後のクエリの部分 `ALTER TABLE [db.]table`).

**create\_time** -この突然変異コマンドが実行のために提出されたとき。

**ブロック番号partition\_id**, **ブロック番号番号** -入れ子になった列。 パーティションIDと、その変異によって取得されたブロック番号(各パーティションでは、そのパーティション内の変異によって取得されたブロック番号 非複製のテーブル、ブロック番号の全ての仕切りがひとつのシーケンスです。 こないということを意味している変異体再現し、テーブルの列として展開しているのが記録するとともにシングルブロック番号の取得による突然変異が原因です。

**parts\_to\_do** -突然変異が完了するために突然変異する必要があるデータ部分の数。

**is\_done** -突然変異は？ たとえ `parts_to_do = 0` 変更する必要がある新しいデータパーツを作成する長時間実行されるINSERTのために、複製されたテーブルの突然変異がまだ行われていない可能性があり

一部のパーツの変更に問題がある場合は、次の列に追加情報が含まれています:

**latest\_failed\_part** -変異することができなかった最新の部分の名前。

**latest\_fail\_time** -最も最近の部分突然変異の失敗の時間。

**latest\_fail\_reason** -最新の部品突然変異の失敗を引き起こした例外メッセージ。

## システムディスク {#system_tables-disks}

ディス [サーバー構成](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

列:

-   `name` ([文字列](../sql-reference/data-types/string.md)) — Name of a disk in the server configuration.
-   `path` ([文字列](../sql-reference/data-types/string.md)) — Path to the mount point in the file system.
-   `free_space` ([UInt64](../sql-reference/data-types/int-uint.md)) — Free space on disk in bytes.
-   `total_space` ([UInt64](../sql-reference/data-types/int-uint.md)) — Disk volume in bytes.
-   `keep_free_space` ([UInt64](../sql-reference/data-types/int-uint.md)) — Amount of disk space that should stay free on disk in bytes. Defined in the `keep_free_space_bytes` ディスク構成のパラメータ。

## システムストレージポリシー {#system_tables-storage_policies}

ストレージポリシ [サーバー構成](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

列:

-   `policy_name` ([文字列](../sql-reference/data-types/string.md)) — Name of the storage policy.
-   `volume_name` ([文字列](../sql-reference/data-types/string.md)) — Volume name defined in the storage policy.
-   `volume_priority` ([UInt64](../sql-reference/data-types/int-uint.md)) — Volume order number in the configuration.
-   `disks` ([配列(文字列)](../sql-reference/data-types/array.md)) — Disk names, defined in the storage policy.
-   `max_data_part_size` ([UInt64](../sql-reference/data-types/int-uint.md)) — Maximum size of a data part that can be stored on volume disks (0 — no limit).
-   `move_factor` ([Float64](../sql-reference/data-types/float.md)) — Ratio of free disk space. When the ratio exceeds the value of configuration parameter, ClickHouse start to move data to the next volume in order.

ストレージポリシーに複数のボリュームが含まれている場合は、各ボリュームの情報がテーブルの個々の行に格納されます。

[元の記事](https://clickhouse.tech/docs/en/operations/system_tables/) <!--hide-->
