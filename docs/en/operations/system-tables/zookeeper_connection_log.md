---
description: 'Shows the history of ZooKeeper connections (including auxiliary ZooKeepers).'
keywords: ['system table', 'zookeeper_connection_log']
slug: /operations/system-tables/zookeeper_connection_log
title: 'system.zookeeper_connection_log'
doc_type: 'reference'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.zookeeper_connection_log

<SystemTableCloud/>

The 'system.zookeeper_connection_log' table shows the history of ZooKeeper connections (including auxiliary ZooKeepers). Each row shows information about one event regarding connections.

:::note
The table doesn't contain events for disconnections caused by server shutdown.
:::

Columns:

-   `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Hostname of the server which is connected to or disconnected from ZooKeeper.
-   `type` ([Enum8](../../sql-reference/data-types/enum.md)) - The type of the event. Possible values: `Connected`, `Disconnected`.
-   `event_date` ([Date](../../sql-reference/data-types/date.md)) - Date of the entry.
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) - Time of the entry.
-   `event_time_microseconds` ([Date](../../sql-reference/data-types/datetime64.md)) - Time of the entry with microseconds precision.
-   `name` ([String](../../sql-reference/data-types/string.md)) — ZooKeeper cluster's name.
-   `host` ([String](../../sql-reference/data-types/string.md)) — The hostname/IP of the ZooKeeper node that ClickHouse connected to.
-   `port` ([UIn16](../../sql-reference/data-types/int-uint.md)) — The port of the ZooKeeper node that ClickHouse connected to.
-   `index` ([UInt8](../../sql-reference/data-types/int-uint.md)) — The index of the ZooKeeper node that ClickHouse connected to or disconnected from. The index is from ZooKeeper config.
-   `client_id` ([Int64](../../sql-reference/data-types/int-uint.md)) — Session id of the connection.
-   `keeper_api_version` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Keeper API version.
-   `enabled_feature_flags` ([Array(Enum16)](../../sql-reference/data-types/array.md)) — Feature flags which are enabled. Only applicable to ClickHouse Keeper. Possible values are `FILTERED_LIST`, `MULTI_READ`, `CHECK_NOT_EXISTS`, `CREATE_IF_NOT_EXISTS`, `REMOVE_RECURSIVE`.
-   `availability_zone` ([String](../../sql-reference/data-types/string.md)) — Availability zone.
-   `reason` ([String](../../sql-reference/data-types/string.md)) — Reason for the connection or disconnection.

Example:

```sql
SELECT * FROM system.zookeeper_connection_log;
```

```text
    ┌─hostname─┬─type─────────┬─event_date─┬──────────event_time─┬────event_time_microseconds─┬─name───────────────┬─host─┬─port─┬─index─┬─client_id─┬─keeper_api_version─┬─enabled_feature_flags───────────────────────────────────────────────────────────────────────┬─availability_zone─┬─reason──────────────┐
 1. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:35 │ 2025-05-12 19:49:35.713067 │ zk_conn_log_test_4 │ zoo2 │ 2181 │     0 │        10 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Initialization      │
 2. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:23 │ 2025-05-12 19:49:23.981570 │ default            │ zoo1 │ 2181 │     0 │         4 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Initialization      │
 3. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:28 │ 2025-05-12 19:49:28.104021 │ default            │ zoo1 │ 2181 │     0 │         5 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Initialization      │
 4. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.459251 │ zk_conn_log_test_2 │ zoo2 │ 2181 │     0 │         6 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Initialization      │
 5. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.574312 │ zk_conn_log_test_3 │ zoo3 │ 2181 │     0 │         7 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Initialization      │
 6. │ node     │ Disconnected │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.909890 │ default            │ zoo1 │ 2181 │     0 │         5 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Config changed      │
 7. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.909895 │ default            │ zoo2 │ 2181 │     0 │         8 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Config changed      │
 8. │ node     │ Disconnected │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.912010 │ zk_conn_log_test_2 │ zoo2 │ 2181 │     0 │         6 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Config changed      │
 9. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.912014 │ zk_conn_log_test_2 │ zoo3 │ 2181 │     0 │         9 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Config changed      │
10. │ node     │ Disconnected │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.912061 │ zk_conn_log_test_3 │ zoo3 │ 2181 │     0 │         7 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Removed from config │
    └──────────┴──────────────┴────────────┴─────────────────────┴────────────────────────────┴────────────────────┴──────┴──────┴───────┴───────────┴────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────┴───────────────────┴─────────────────────┘
```
