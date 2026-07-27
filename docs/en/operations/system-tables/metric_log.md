---
description: 'System table containing history of metrics values from tables `system.metrics`
  and `system.events`, periodically flushed to disk.'
keywords: ['system table', 'metric_log']
slug: /operations/system-tables/metric_log
title: 'system.metric_log'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.metric_log

<SystemTableCloud/>

Contains history of metrics values from tables `system.metrics` and `system.events`, periodically flushed to disk.

Columns:
- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Hostname of the server executing the query.
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — Event date.
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Event time.
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Event time with microseconds resolution.

**Example**

```sql
SELECT * FROM system.metric_log LIMIT 1 FORMAT Vertical;
```

```text
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

**Schema**
This table can be configured with different schema types using the XML tag `<schema_type>`. The default schema type is `wide`, where each metric or profile event is stored as a separate column. This schema is the most performant and efficient for single-column reads.

The `transposed` schema stores data in a format similar to `system.asynchronous_metric_log`, where metrics and events are stored as rows. This schema is useful for low-resource setups because it reduces resource consumption during merges.

There is also a compatibility schema, `transposed_with_wide_view`, which stores actual data in a table with the transposed schema (`system.transposed_metric_log`) and creates a view on top of it using the wide schema. This view queries the transposed table, making it useful for migrating from the `wide` schema to the `transposed` schema.

**See also**

- [metric_log setting](../../operations/server-configuration-parameters/settings.md#metric_log) — Enabling and disabling the setting.
- [system.asynchronous_metrics](../../operations/system-tables/asynchronous_metrics.md) — Contains periodically calculated metrics.
- [system.events](/operations/system-tables/events) — Contains a number of events that occurred.
- [system.metrics](../../operations/system-tables/metrics.md) — Contains instantly calculated metrics.
- [Monitoring](../../operations/monitoring.md) — Base concepts of ClickHouse monitoring.
