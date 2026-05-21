---
description: 'System table showing currently active ZooKeeper watches registered by
  this ClickHouse server.'
keywords: ['system table', 'zookeeper_watches']
slug: /operations/system-tables/zookeeper_watches
title: 'system.zookeeper_watches'
doc_type: 'reference'
---

## Description {#description}

Shows currently active [watches](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html#ch_zkWatches) registered by this ClickHouse server on ZooKeeper nodes (including auxiliary ZooKeepers). Each row represents one watch.

## Columns {#columns}

- `zookeeper_name` ([String](../../sql-reference/data-types/string.md)) — Name of the ZooKeeper connection (`default` for the main connection, or the auxiliary name).
- `create_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Time when the watch was created.
- `create_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Time when the watch was created with microsecond precision.
- `path` ([String](../../sql-reference/data-types/string.md)) — ZooKeeper path being watched.
- `session_id` ([Int64](../../sql-reference/data-types/int-uint.md)) — Session ID of the connection that registered the watch.
- `request_xid` ([Int64](../../sql-reference/data-types/int-uint.md)) — XID of the request that created the watch.
- `op_num` ([Enum](../../sql-reference/data-types/enum.md)) — The type of the request that created the watch.
- `watch_type` ([Enum8](../../sql-reference/data-types/enum.md)) — Watch type. Possible values:
  - `Children` — watching for changes in the list of child nodes (set by `List` operations).
  - `Exists` — watching for node creation or deletion.
  - `Data` — watching for changes in node data (set by `Get` operations).

Example:

```sql
SELECT * FROM system.zookeeper_watches FORMAT Vertical;
```

```text
Row 1:
──────
zookeeper_name:           default
create_time:              2026-03-16 12:00:00
create_time_microseconds: 2026-03-16 12:00:00.123456
path:                     /clickhouse/task_queue/ddl
session_id:               106662742089334927
request_xid:              10858
op_num:                   List
watch_type:               Children
```

**See Also**

-   [ZooKeeper](../../operations/tips.md#zookeeper)
-   [ZooKeeper guide](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html)
