---
description: 'System table which exists only if ZooKeeper is configured. Shows current
  connections to ZooKeeper (including auxiliary ZooKeepers).'
keywords: ['system table', 'zookeeper_connection']
slug: /operations/system-tables/zookeeper_connection
title: 'system.zookeeper_connection'
doc_type: 'reference'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.zookeeper_connection

<SystemTableCloud/>

This table does not exist if ZooKeeper is not configured. The 'system.zookeeper_connection' table shows current connections to ZooKeeper (including auxiliary ZooKeepers). Each row shows information about one connection.

Columns:

-   `name` ([String](../../sql-reference/data-types/string.md)) — ZooKeeper cluster's name.
-   `host` ([String](../../sql-reference/data-types/string.md)) — The hostname/IP of the ZooKeeper node that ClickHouse connected to.
-   `port` ([UIn16](../../sql-reference/data-types/int-uint.md)) — The port of the ZooKeeper node that ClickHouse connected to.
-   `index` ([Nullable(UInt8)](../../sql-reference/data-types/int-uint.md)) — The index of the ZooKeeper node that ClickHouse connected to. The index is from ZooKeeper config. If not connected, this column is NULL.
-   `connected_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — When the connection was established
-   `session_uptime_elapsed_seconds` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Seconds elapsed since the connection was established.
-   `is_expired` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Is the current connection expired.
-   `keeper_api_version` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Keeper API version.
-   `client_id` ([Int64](../../sql-reference/data-types/int-uint.md)) — Session id of the connection.
-   `xid` ([Int64](../../sql-reference/data-types/int-uint.md)) — XID of the current session.
-   `enabled_feature_flags` ([Array(Enum16)](../../sql-reference/data-types/array.md)) — Feature flags which are enabled. Only applicable to ClickHouse Keeper. Possible values are `FILTERED_LIST`, `MULTI_READ`, `CHECK_NOT_EXISTS`, `CREATE_IF_NOT_EXISTS`, `REMOVE_RECURSIVE`.
-   `availability_zone` ([String](../../sql-reference/data-types/string.md)) — Availability zone.

Example:

```sql
SELECT * FROM system.zookeeper_connection;
```

```text
┌─name────┬─host──────┬─port─┬─index─┬──────connected_time─┬─session_uptime_elapsed_seconds─┬─is_expired─┬─keeper_api_version─┬─client_id─┬─xid─┬─enabled_feature_flags────────────────────────────────────────────────────┬─availability_zone─┐
│ default │ 127.0.0.1 │ 2181 │     0 │ 2025-04-10 14:30:00 │                            943 │          0 │                  0 │       420 │  69 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS'] │ eu-west-1b        │
└─────────┴───────────┴──────┴───────┴─────────────────────┴────────────────────────────────┴────────────┴────────────────────┴───────────┴─────┴──────────────────────────────────────────────────────────────────────────┴───────────────────┘
```
