# metric_log {#system_tables-metric_log}

Contains history of metrics values from tables `system.metrics` and `system.events`, periodically flushed to disk.

Columns:
-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — Event date.
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Event time.
-   `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Event time with microseconds resolution.

**Example**

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

**See also**

-   [metric_log setting](../../operations/server-configuration-parameters/settings.md#metric_log) — Enabling and disabling the setting.
-   [system.asynchronous_metrics](../../operations/system-tables/asynchronous_metrics.md) — Contains periodically calculated metrics.
-   [system.events](../../operations/system-tables/events.md#system_tables-events) — Contains a number of events that occurred.
-   [system.metrics](../../operations/system-tables/metrics.md) — Contains instantly calculated metrics.
-   [Monitoring](../../operations/monitoring.md) — Base concepts of ClickHouse monitoring.

[Original article](https://clickhouse.com/docs/en/operations/system-tables/metric_log) <!--hide-->
