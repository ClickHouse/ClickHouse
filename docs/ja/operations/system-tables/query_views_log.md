---
slug: /ja/operations/system-tables/query_views_log
---
# query_views_log

クエリを実行した際に実行された依存ビューに関する情報を含みます。例えば、ビューの種類や実行時間を示します。

ログを開始するには次の手順に従います：

1. [query_views_log](../../operations/server-configuration-parameters/settings.md#query_views_log) セクションでパラメータを設定します。
2. [log_query_views](../../operations/settings/settings.md#log-query-views) を1に設定します。

データのフラッシュ間隔は、サーバー設定セクションの [query_views_log](../../operations/server-configuration-parameters/settings.md#query_views_log) の `flush_interval_milliseconds` パラメータで設定します。強制的にフラッシュするには、[SYSTEM FLUSH LOGS](../../sql-reference/statements/system.md#query_language-system-flush_logs) クエリを使用します。

ClickHouseはテーブルからデータを自動的に削除しません。詳細は[概要](../../operations/system-tables/index.md#system-tables-introduction)を参照してください。

`query_views_log` テーブルに登録されるクエリの数を減らすには、[log_queries_probability](../../operations/settings/settings.md#log-queries-probability) 設定を使用できます。

カラム:

- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — クエリを実行しているサーバーのホスト名。
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — ビューの最後のイベントが発生した日付。
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — ビューが実行を終了した日時。
- `event_time_microseconds` ([DateTime](../../sql-reference/data-types/datetime.md)) — マイクロ秒精度でビューが実行を終了した日時。
- `view_duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — ビューの実行時間 (各ステージの合計) をミリ秒単位で。
- `initial_query_id` ([String](../../sql-reference/data-types/string.md)) — 初期クエリのID (分散クエリ実行のため)。
- `view_name` ([String](../../sql-reference/data-types/string.md)) — ビューの名前。
- `view_uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — ビューのUUID。
- `view_type` ([Enum8](../../sql-reference/data-types/enum.md)) — ビューの種類。値:
    - `'Default' = 1` — [デフォルトビュー](../../sql-reference/statements/create/view.md#normal)。このログには表示されません。
    - `'Materialized' = 2` — [マテリアライズドビュー](../../sql-reference/statements/create/view.md#materialized)。
    - `'Live' = 3` — [ライブビュー](../../sql-reference/statements/create/view.md#live-view)。
- `view_query` ([String](../../sql-reference/data-types/string.md)) — ビューが実行したクエリ。
- `view_target` ([String](../../sql-reference/data-types/string.md)) — ビューのターゲットテーブルの名前。
- `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 読み取った行数。
- `read_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 読み取ったバイト数。
- `written_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 書き込まれた行数。
- `written_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 書き込まれたバイト数。
- `peak_memory_usage` ([Int64](../../sql-reference/data-types/int-uint.md)) — このビューのコンテキスト内で割り当てられたメモリと解放されたメモリの最大差。
- `ProfileEvents` ([Map(String, UInt64)](../../sql-reference/data-types/array.md)) — 異なるメトリクスを測定するProfileEvents。詳細は [system.events](../../operations/system-tables/events.md#system_tables-events) テーブルで見つかります。
- `status` ([Enum8](../../sql-reference/data-types/enum.md)) — ビューのステータス。値:
    - `'QueryStart' = 1` — ビュー実行の成功した開始。表示されません。
    - `'QueryFinish' = 2` — ビュー実行の成功した終了。
    - `'ExceptionBeforeStart' = 3` — ビュー実行開始前の例外。
    - `'ExceptionWhileProcessing' = 4` — ビュー実行中の例外。
- `exception_code` ([Int32](../../sql-reference/data-types/int-uint.md)) — 例外コード。
- `exception` ([String](../../sql-reference/data-types/string.md)) — 例外メッセージ。
- `stack_trace` ([String](../../sql-reference/data-types/string.md)) — [スタックトレース](https://en.wikipedia.org/wiki/Stack_trace)。クエリが正常に完了した場合は空。

**例**

クエリ:

``` sql
SELECT * FROM system.query_views_log LIMIT 1 \G;
```

結果:

``` text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2021-06-22
event_time:              2021-06-22 13:23:07
event_time_microseconds: 2021-06-22 13:23:07.738221
view_duration_ms:        0
initial_query_id:        c3a1ac02-9cad-479b-af54-9e9c0a7afd70
view_name:               default.matview_inner
view_uuid:               00000000-0000-0000-0000-000000000000
view_type:               Materialized
view_query:              SELECT * FROM default.table_b
view_target:             default.`.inner.matview_inner`
read_rows:               4
read_bytes:              64
written_rows:            2
written_bytes:           32
peak_memory_usage:       4196188
ProfileEvents:           {'FileOpen':2,'WriteBufferFromFileDescriptorWrite':2,'WriteBufferFromFileDescriptorWriteBytes':187,'IOBufferAllocs':3,'IOBufferAllocBytes':3145773,'FunctionExecute':3,'DiskWriteElapsedMicroseconds':13,'InsertedRows':2,'InsertedBytes':16,'SelectedRows':4,'SelectedBytes':48,'ContextLock':16,'RWLockAcquiredReadLocks':1,'RealTimeMicroseconds':698,'SoftPageFaults':4,'OSReadChars':463}
status:                  QueryFinish
exception_code:          0
exception:
stack_trace:
```

**参照**

- [system.query_log](../../operations/system-tables/query_log.md#system_tables-query_log) — クエリ実行に関する一般情報を含む `query_log` システムテーブルの説明。
- [system.query_thread_log](../../operations/system-tables/query_thread_log.md#system_tables-query_thread_log) — 各クエリ実行スレッドに関する情報を含むこのテーブル。
