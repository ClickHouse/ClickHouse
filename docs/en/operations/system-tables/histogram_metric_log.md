---
description: 'System table containing periodic snapshots of histogram metrics, flushed to disk.'
keywords: ['system table', 'histogram_metric_log']
sidebar_label: 'histogram_metric_log'
sidebar_position: 65
slug: /operations/system-tables/histogram_metric_log
title: 'system.histogram_metric_log'
doc_type: 'reference'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

<SystemTableCloud/>

## Description {#description}

History of `system.histogram_metrics`. Snapshot taken every `collect_interval_milliseconds`, flushed to disk.

## Columns {#columns}

- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Hostname of the server.
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — Event date.
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Event time.
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Event time with microseconds resolution.
- `metric` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Metric name.
- `labels` ([Map(String, String)](../../sql-reference/data-types/map.md)) — Metric labels.
- `histogram` ([Map(Float64, UInt64)](../../sql-reference/data-types/map.md)) — Bucket upper bound to cumulative count. `+inf` is the final bucket.
- `count` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Total observations. Equals `histogram[+inf]`.
- `sum` ([Float64](../../sql-reference/data-types/float.md)) — Sum of observed values.

## Example {#example}

```sql
SELECT event_time, metric, labels, histogram
FROM system.histogram_metric_log
WHERE metric = 'keeper_response_time_ms'
ORDER BY event_time DESC
LIMIT 1
FORMAT Vertical;
```

## See Also {#see-also}

- [system.histogram_metrics](/operations/system-tables/histogram_metrics) — Live histogram metrics.
- [system.metric_log](/operations/system-tables/metric_log) — History of `system.metrics` and `system.events`.
