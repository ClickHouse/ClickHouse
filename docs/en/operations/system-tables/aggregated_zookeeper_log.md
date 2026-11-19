---
description: 'System table containing aggregated statistics of ZooKeeper operations
  grouped by session, path, operation type, component, and subrequest flag.'
keywords: ['system table', 'aggregated_zookeeper_log']
slug: /operations/system-tables/aggregated_zookeeper_log
title: 'system.aggregated_zookeeper_log'
doc_type: 'reference'
---

# system.aggregated_zookeeper_log

This table contains aggregated statistics of ZooKeeper operations (e.g. number of operations, average latency, errors) grouped by `(session_id, parent_path, operation, component, is_subrequest)` and periodically flushed to disk.

Unlike [system.zookeeper_log](zookeeper_log.md) which logs every individual request and response, this table aggregates operations into groups, making it much more lightweight and therefore more suitable for production workloads.

Operations that are part of a `Multi` or `MultiRead` batch are tracked separately via the `is_subrequest` column. Subrequests have zero latency because the total latency is attributed to the enclosing `Multi`/`MultiRead` operation.

Columns:

- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Hostname of the server.
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — Date the group was flushed.
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Time the group was flushed.
- `session_id` ([Int64](../../sql-reference/data-types/int-uint.md)) — Session id.
- `parent_path` ([String](../../sql-reference/data-types/string.md)) — Prefix of the path.
- `operation` ([Enum](../../sql-reference/data-types/enum.md)) — Type of ZooKeeper operation.
- `is_subrequest` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Whether this operation was a subrequest inside a `Multi` or `MultiRead` operation.
- `count` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Number of operations in the group.
- `errors` ([Map(Enum, UInt32)](../../sql-reference/data-types/map.md)) — Errors in the group, mapping error code to count.
- `average_latency` ([Float64](../../sql-reference/data-types/float.md)) — Average latency across all operations in the group, in microseconds. Subrequests have zero latency because the latency is attributed to the enclosing `Multi` or `MultiRead` operation.
- `component` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Component that caused the event.

**See Also**

- [system.zookeeper_log](zookeeper_log.md) — Detailed per-request ZooKeeper log.
- [ZooKeeper](../../operations/tips.md#zookeeper)
