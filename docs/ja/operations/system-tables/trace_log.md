---
slug: /ja/operations/system-tables/trace_log
---
# trace_log

[sampling query profiler](../../operations/optimizing-performance/sampling-query-profiler.md) によって収集されたスタックトレースが含まれています。

このテーブルは、[trace_log](../../operations/server-configuration-parameters/settings.md#trace_log) サーバー設定セクションがセットされると ClickHouse によって作成されます。設定も参照してください：[query_profiler_real_time_period_ns](../../operations/settings/settings.md#query_profiler_real_time_period_ns)、[query_profiler_cpu_time_period_ns](../../operations/settings/settings.md#query_profiler_cpu_time_period_ns)、[memory_profiler_step](../../operations/settings/settings.md#memory_profiler_step)、[memory_profiler_sample_probability](../../operations/settings/settings.md#memory_profiler_sample_probability)、[trace_profile_events](../../operations/settings/settings.md#trace_profile_events)。

ログを解析するには、`addressToLine`、`addressToLineWithInlines`、`addressToSymbol`、および `demangle` のイントロスペクション関数を使用します。

カラム:

- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — クエリを実行しているサーバーのホスト名。
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — サンプリング時点の日付。
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — サンプリング時点のタイムスタンプ。
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — マイクロ秒精度のサンプリング時点のタイムスタンプ。
- `timestamp_ns` ([UInt64](../../sql-reference/data-types/int-uint.md)) — ナノ秒精度のサンプリング時点のタイムスタンプ。
- `revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ClickHouse サーバーのビルドリビジョン。

    `clickhouse-client` でサーバーに接続すると、`Connected to ClickHouse server version 19.18.1.` のような文字列が表示されます。このフィールドにはサーバーの `revision` が含まれており、`version` は含まれていません。

- `trace_type` ([Enum8](../../sql-reference/data-types/enum.md)) — トレースの種類:
    - `Real` は、ウォールクロック時間でのスタックトレースの収集を表します。
    - `CPU` は、CPU時間でのスタックトレースの収集を表します。
    - `Memory` は、メモリアロケーションがサブシークエントウォーターマークを超えたときのアロケーションとデアロケーションの収集を表します。
    - `MemorySample` は、ランダムなアロケーションとデアロケーションの収集を表します。
    - `MemoryPeak` は、ピークメモリ使用量の更新の収集を表します。
    - `ProfileEvent` は、プロフィールイベントのインクリメントの収集を表します。
- `thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — スレッド識別子。
- `query_id` ([String](../../sql-reference/data-types/string.md)) — 実行中のクエリの詳細を [query_log](#system_tables-query_log) システムテーブルから取得するためのクエリ識別子。
- `trace` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — サンプリング時点のスタックトレース。各要素は ClickHouse サーバープロセス内の仮想メモリアドレスです。
- `size` ([Int64](../../sql-reference/data-types/int-uint.md)) - `Memory`、`MemorySample` または `MemoryPeak` のトレースタイプではアロケートされたメモリの量、他のトレースタイプでは0です。
- `event` ([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md)) - `ProfileEvent` のトレースタイプでは更新されたプロフィールイベントの名前、他のトレースタイプでは空文字列です。
- `increment` ([UInt64](../../sql-reference/data-types/int-uint.md)) - `ProfileEvent` のトレースタイプではプロフィールイベントの増加量、他のトレースタイプでは0です。

**例**

``` sql
SELECT * FROM system.trace_log LIMIT 1 \G
```

``` text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2020-09-10
event_time:              2020-09-10 11:23:09
event_time_microseconds: 2020-09-10 11:23:09.872924
timestamp_ns:            1599762189872924510
revision:                54440
trace_type:              Memory
thread_id:               564963
query_id:
trace:                   [371912858,371912789,371798468,371799717,371801313,371790250,624462773,566365041,566440261,566445834,566460071,566459914,566459842,566459580,566459469,566459389,566459341,566455774,371993941,371988245,372158848,372187428,372187309,372187093,372185478,140222123165193,140222122205443]
size:                    5244400
```
