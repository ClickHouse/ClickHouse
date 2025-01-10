---
slug: /en/operations/system-tables/query_metric_log
---
# query_metric_log

Contains history of memory and metric values from table `system.events` for individual queries, periodically flushed to disk.

Once a query starts, data is collected at periodic intervals of `query_metric_log_interval` milliseconds (which is set to 1000
by default). The data is also collected when the query finishes if the query takes longer than `query_metric_log_interval`.

Columns:
- `query_id` ([String](../../sql-reference/data-types/string.md)) — ID of the query.
- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Hostname of the server executing the query.
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — Event date.
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Event time.
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Event time with microseconds resolution.

**Example**

``` sql
SELECT * FROM system.query_metric_log LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
query_id:                                                        97c8ba04-b6d4-4bd7-b13e-6201c5c6e49d
hostname:                                                        clickhouse.eu-central1.internal
event_date:                                                      2020-09-05
event_time:                                                      2020-09-05 16:22:33
event_time_microseconds:                                         2020-09-05 16:22:33.196807
memory_usage:                                                    313434219
peak_memory_usage:                                               598951986
ProfileEvent_Query:                                              0
ProfileEvent_SelectQuery:                                        0
ProfileEvent_InsertQuery:                                        0
ProfileEvent_FailedQuery:                                        0
ProfileEvent_FailedSelectQuery:                                  0
...
```

**See also**

- [query_metric_log setting](../../operations/server-configuration-parameters/settings.md#query_metric_log) — Enabling and disabling the setting.
- [query_metric_log_interval](../../operations/settings/settings.md#query_metric_log_interval)
- [system.asynchronous_metrics](../../operations/system-tables/asynchronous_metrics.md) — Contains periodically calculated metrics.
- [system.events](../../operations/system-tables/events.md#system_tables-events) — Contains a number of events that occurred.
- [system.metrics](../../operations/system-tables/metrics.md) — Contains instantly calculated metrics.
- [Monitoring](../../operations/monitoring.md) — Base concepts of ClickHouse monitoring.
