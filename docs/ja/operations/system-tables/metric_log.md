---
slug: /ja/operations/system-tables/metric_log
---
# metric_log

`system.metrics`および`system.events`テーブルからのメトリクス値の履歴を含み、定期的にディスクにフラッシュされます。

カラム:
- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — クエリを実行しているサーバーのホスト名。
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — イベントの日付。
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — イベントの時刻。
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — マイクロ秒単位の解像度のイベント時刻。

**例**

``` sql
SELECT * FROM system.metric_log LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
hostname:                                                        clickhouse.eu-central1.internal
event_date:                                                      2020-09-05
event_time:                                                      2020-09-05 16:22:33
event_time_microseconds:                                         2020-09-05 16:22:33.196807
milliseconds:                                                    196
ProfileEvent_Query:                                              0
ProfileEvent_SelectQuery:                                        0
ProfileEvent_InsertQuery:                                        0
ProfileEvent_FailedQuery:                                        0
ProfileEvent_FailedSelectQuery:                                  0
...
...
CurrentMetric_Revision:                                          54439
CurrentMetric_VersionInteger:                                    20009001
CurrentMetric_RWLockWaitingReaders:                              0
CurrentMetric_RWLockWaitingWriters:                              0
CurrentMetric_RWLockActiveReaders:                               0
CurrentMetric_RWLockActiveWriters:                               0
CurrentMetric_GlobalThread:                                      74
CurrentMetric_GlobalThreadActive:                                26
CurrentMetric_LocalThread:                                       0
CurrentMetric_LocalThreadActive:                                 0
CurrentMetric_DistributedFilesToInsert:                          0
```

**関連項目**

- [metric_log 設定](../../operations/server-configuration-parameters/settings.md#metric_log) — 設定の有効化と無効化。
- [system.asynchronous_metrics](../../operations/system-tables/asynchronous_metrics.md) — 定期的に計算されるメトリクスを含みます。
- [system.events](../../operations/system-tables/events.md#system_tables-events) — 発生したイベント数を含みます。
- [system.metrics](../../operations/system-tables/metrics.md) — 即座に計算されるメトリクスを含みます。
- [モニタリング](../../operations/monitoring.md) — ClickHouseモニタリングの基本概念。
