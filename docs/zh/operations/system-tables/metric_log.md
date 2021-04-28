# system.metric_log {#system_tables-metric_log}

包含来自表`system.metrics` 和 `system.events` 的指标值的历史记录，并定期刷新到磁盘。

列：
-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — 事件的日期。
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — 事件时间。
-   `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — 事件时间（以微秒为单位）。

**示例**

``` sql
SELECT * FROM system.metric_log LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
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

**另请参阅**

-   [metric_log setting](../../operations/server-configuration-parameters/settings.md#metric_log) — 启用和禁用设置。
-   [system.asynchronous_metrics](../../operations/system-tables/asynchronous_metrics.md) — 包含定期计算的指标。
-   [system.events](../../operations/system-tables/events.md#system_tables-events) — 包含已发生的事件。
-   [system.metrics](../../operations/system-tables/metrics.md) — 包含实时计算的指标。
-   [监测](../../operations/monitoring.md) — ClickHouse监测的基本概念。

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/metric_log) <!--hide-->
