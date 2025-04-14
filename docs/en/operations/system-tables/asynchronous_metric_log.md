---
slug: /en/operations/system-tables/asynchronous_metric_log
---
# asynchronous_metric_log

Contains the historical values for `system.asynchronous_metrics`, which are saved once per time interval (one second by default). Enabled by default.

Columns:

- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Hostname of the server executing the query.
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — Event date.
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Event time.
- `metric` ([String](../../sql-reference/data-types/string.md)) — Metric name.
- `value` ([Float64](../../sql-reference/data-types/float.md)) — Metric value.

**Example**

``` sql
SELECT * FROM system.asynchronous_metric_log LIMIT 3 \G
```

``` text
Row 1:
──────
hostname:   clickhouse.eu-central1.internal
event_date: 2023-11-14
event_time: 2023-11-14 14:39:07
metric:     AsynchronousHeavyMetricsCalculationTimeSpent
value:      0.001

Row 2:
──────
hostname:   clickhouse.eu-central1.internal
event_date: 2023-11-14
event_time: 2023-11-14 14:39:08
metric:     AsynchronousHeavyMetricsCalculationTimeSpent
value:      0

Row 3:
──────
hostname:   clickhouse.eu-central1.internal
event_date: 2023-11-14
event_time: 2023-11-14 14:39:09
metric:     AsynchronousHeavyMetricsCalculationTimeSpent
value:      0
```

**See Also**

- [system.asynchronous_metrics](../system-tables/asynchronous_metrics.md) — Contains metrics, calculated periodically in the background.
- [system.metric_log](../system-tables/metric_log.md) — Contains history of metrics values from tables `system.metrics` and `system.events`, periodically flushed to disk.
