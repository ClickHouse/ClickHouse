---
slug: /en/operations/system-tables/metrics
---
import SystemTableCloud from '@site/docs/en/_snippets/_system_table_cloud.md';

# histogram_metrics

<SystemTableCloud/>

Contains histogram metrics which can be calculated instantly and exported in the Prometheus format. This table is always up to date.

Columns:

- `metric` ([String](../../sql-reference/data-types/string.md)) — Metric name.
- `value` ([Int64](../../sql-reference/data-types/int-uint.md)) — Metric value.
- `description` ([String](../../sql-reference/data-types/string.md)) — Metric description.
- `labels` ([Map(String, String)](../../sql-reference/data-types/map.md)) — Metric labels.
- `name` ([String](../../sql-reference/data-types/string.md)) — Alias for `metric`.

**Example**

You can use a query like this to export all the histogram metrics in the Prometheus format.
``` sql
SELECT
  metric AS name,
  toFloat64(value) AS value,
  description AS help,
  labels,
  'histogram' AS type
FROM system.histogram_metrics
FORMAT Prometheus
```

## Metric descriptions

### keeper_response_time_ms_bucket
The response time of Keeper, in milliseconds.

**See Also**
- [system.asynchronous_metrics](../../operations/system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
- [system.events](../../operations/system-tables/events.md#system_tables-events) — Contains a number of events that occurred.
- [system.metric_log](../../operations/system-tables/metric_log.md#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` and `system.events`.
- [Monitoring](../../operations/monitoring.md) — Base concepts of ClickHouse monitoring.
