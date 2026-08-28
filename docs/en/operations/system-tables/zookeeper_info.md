---
description: 'System table which outputs introspection of all available keeper nodes.'
keywords: ['system table', 'zookeeper_info']
slug: /operations/system-tables/zookeeper_info
title: 'system.zookeeper_info'
doc_type: 'reference'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.zookeeper_info

<SystemTableCloud/>

This table outputs combined introspection about zookeeper and the nodes are taken from config.

Columns:
-   `zookeeper_cluster_name` ([String](../../sql-reference/data-types/string.md)) — ZooKeeper cluster's name.
-   `host` ([String](../../sql-reference/data-types/string.md)) — The hostname/IP of the ZooKeeper node that ClickHouse connected to.
-   `port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — The port of the ZooKeeper node that ClickHouse connected to.
-   `index` ([Nullable(UInt8)](../../sql-reference/data-types/int-uint.md)) — The index of the ZooKeeper node that ClickHouse connected to. The index is from ZooKeeper config. If not connected, this column is NULL.
-   `is_connected` ([Nullable(UInt8)](../../sql-reference/data-types/int-uint.md)) — If zookeeper is connected or not.
-   `is_readonly` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Is read only.
-   `version` ([String](../../sql-reference/data-types/string.md)) — The ZooKeeper version.
-   `avg_latency` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The average latency.
-   `max_latency` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The max latency.
-   `min_latency` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The min latency.
-   `packets_received` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The number of packets received.
-   `packets_sent` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The number of packets sent.
-   `outstanding_requests` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The number of outstanding requests.
-   `server_state` ([String](../../sql-reference/data-types/string.md)) — Server state.
-   `is_leader` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Is this zookeeper leader.
-   `znode_count` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The znode count.
-   `watch_count` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The watch count.
-   `ephemerals_count` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The ephemerals count.
-   `approximate_data_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The approximate data size.
-   `followers` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The followers of the leader. This field is only exposed by the leader.
-   `synced_followers` ([UInt64](../../sql-reference/data-types/int-uint.md)) — TThe synced followers of the leader. This field is only exposed by the leader.
-   `pending_syncs` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The pending syncs of the leader. This field is only exposed by the leader.
-   `open_file_descriptor_count` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The open file descriptor count. Only available on Unix platforms.
-   `max_file_descriptor_count` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The max file descriptor count. Only available on Unix platforms.
-   `connections` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The ZooKeeper connections.
-   `outstanding` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The ZooKeeper outstanding.
-   `zxid` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The ZooKeeper zxid.
-   `node_count` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The ZooKeeper node count.
-   `snapshot_dir_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The ZooKeeper snapshot directory size.
-   `log_dir_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The ZooKeeper log directory size.
-   `first_log_idx` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The ZooKeeper first log index.
-   `first_log_term` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The ZooKeeper first log term.
-   `last_log_idx` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The ZooKeeper last log index.
-   `last_log_term` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The ZooKeeper last log term.
-   `last_committed_idx` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The ZooKeeper last committed index.
-   `leader_committed_log_idx` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The ZooKeeper leader committed log index.
-   `target_committed_log_idx` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The ZooKeeper target committed log index.
-   `last_snapshot_idx` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The ZooKeeper last snapshot index.
g