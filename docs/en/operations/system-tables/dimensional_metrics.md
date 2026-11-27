---
description: 'This table contains dimensional metrics that can be calculated instantly
  and exported in the Prometheus format. It is always up to date.'
keywords: ['system table', 'dimensional_metrics']
slug: /operations/system-tables/dimensional_metrics
title: 'system.dimensional_metrics'
doc_type: 'reference'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# dimensional_metrics {#dimensional_metrics}

<SystemTableCloud/>

This table contains dimensional metrics that can be calculated instantly and exported in the Prometheus format. It is always up to date.

Columns:

- `metric` ([String](../../sql-reference/data-types/string.md)) — Metric name.
- `value` ([Int64](../../sql-reference/data-types/int-uint.md)) — Metric value.
- `description` ([String](../../sql-reference/data-types/string.md)) — Metric description.
- `labels` ([Map(String, String)](../../sql-reference/data-types/map.md)) — Metric labels.
- `name` ([String](../../sql-reference/data-types/string.md)) — Alias for `metric`.

**Example**

You can use a query like this to export all the dimensional metrics in the Prometheus format.
```sql
SELECT
  metric AS name,
  toFloat64(value) AS value,
  description AS help,
  labels,
  'gauge' AS type
FROM system.dimensional_metrics
FORMAT Prometheus
```

## Metric descriptions {#metric_descriptions}

### merge_failures {#merge_failures}
Number of all failed merges since startup.

### startup_scripts_failure_reason {#startup_scripts_failure_reason}
Indicates startup scripts failures by error type. Set to 1 when a startup script fails, labelled with the error name.

### merge_tree_parts {#merge_tree_parts}
Number of merge tree data parts, labelled by part state, part type, and whether it is a projection part.

**See Also**
- [system.asynchronous_metrics](/operations/system-tables/asynchronous_metrics) — Contains periodically calculated metrics.
- [system.events](/operations/system-tables/events) — Contains a number of events that occurred.
- [system.metric_log](/operations/system-tables/metric_log) — Contains a history of metrics values from tables `system.metrics` and `system.events`.
- [Monitoring](../../operations/monitoring.md) — Base concepts of ClickHouse monitoring.
