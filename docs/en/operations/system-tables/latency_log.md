---
description: 'Contains the history of all latency buckets, periodically flushed to
  disk.'
slug: /operations/system-tables/latency_log
title: 'system.latency_log'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# latency_log

<SystemTableCloud/>

Contains history of all latency buckets, periodically flushed to disk.

Columns:
- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Hostname of the server executing the query.
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — Event date.
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Event time.
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Event time with microseconds resolution.

**Example**

```sql
SELECT * FROM system.latency_log LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
hostname:                                              clickhouse.eu-central1.internal
event_date:                                            2024-09-19
event_time:                                            2024-09-19 17:09:17
event_time_microseconds:                               2024-09-19 17:09:17.712477
LatencyEvent_S3FirstByteReadAttempt1Microseconds:      [278,278,278,278,278,278,278,278,278,278,278,278,278,278,278,278]
LatencyEvent_S3FirstByteWriteAttempt1Microseconds:     [1774,1776,1776,1776,1776,1776,1776,1776,1776,1776,1776,1776,1776,1776,1776,1776]
LatencyEvent_S3FirstByteReadAttempt2Microseconds:      [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
LatencyEvent_S3FirstByteWriteAttempt2Microseconds:     [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
LatencyEvent_S3FirstByteReadAttemptNMicroseconds:      [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
LatencyEvent_S3FirstByteWriteAttemptNMicroseconds:     [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
LatencyEvent_S3ReadConnectMicroseconds:                [1,1,1,1,1,1,1,1,1,1]
LatencyEvent_S3WriteConnectMicroseconds:               [329,362,362,363,363,363,363,363,363,363]
LatencyEvent_DiskS3FirstByteReadAttempt1Microseconds:  [278,278,278,278,278,278,278,278,278,278,278,278,278,278,278,278]
LatencyEvent_DiskS3FirstByteWriteAttempt1Microseconds: [1774,1776,1776,1776,1776,1776,1776,1776,1776,1776,1776,1776,1776,1776,1776,1776]
LatencyEvent_DiskS3FirstByteReadAttempt2Microseconds:  [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
LatencyEvent_DiskS3FirstByteWriteAttempt2Microseconds: [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
LatencyEvent_DiskS3FirstByteReadAttemptNMicroseconds:  [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
LatencyEvent_DiskS3FirstByteWriteAttemptNMicroseconds: [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
LatencyEvent_DiskS3ReadConnectMicroseconds:            [1,1,1,1,1,1,1,1,1,1]
LatencyEvent_DiskS3WriteConnectMicroseconds:           [329,362,362,363,363,363,363,363,363,363]
```

**See also**

- [latency_log_setting](../../operations/server-configuration-parameters/settings.md#latency_log) - Enabling and disabling the setting.
- [latency_buckets](../../operations/system-tables/latency_buckets.md) - Latency log buckets bounds.
- [Monitoring](../../operations/monitoring.md) - Base concepts of ClickHouse monitoring.
