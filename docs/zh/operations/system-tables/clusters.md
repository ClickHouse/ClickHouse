---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# 系统。集群 {#system-clusters}

包含有关配置文件中可用的集群及其中的服务器的信息。

列:

-   `cluster` (String) — The cluster name.
-   `shard_num` (UInt32) — The shard number in the cluster, starting from 1.
-   `shard_weight` (UInt32) — The relative weight of the shard when writing data.
-   `replica_num` (UInt32) — The replica number in the shard, starting from 1.
-   `host_name` (String) — The host name, as specified in the config.
-   `host_address` (String) — The host IP address obtained from DNS.
-   `port` (UInt16) — The port to use for connecting to the server.
-   `user` (String) — The name of the user for connecting to the server.
-   `errors_count` (UInt32)-此主机无法到达副本的次数。
-   `estimated_recovery_time` (UInt32)-剩下的秒数，直到副本错误计数归零，它被认为是恢复正常。

请注意 `errors_count` 每个查询集群更新一次，但 `estimated_recovery_time` 按需重新计算。 所以有可能是非零的情况 `errors_count` 和零 `estimated_recovery_time`，下一个查询将为零 `errors_count` 并尝试使用副本，就好像它没有错误。

**另请参阅**

-   [表引擎分布式](../../engines/table-engines/special/distributed.md)
-   [distributed\_replica\_error\_cap设置](../../operations/settings/settings.md#settings-distributed_replica_error_cap)
-   [distributed\_replica\_error\_half\_life设置](../../operations/settings/settings.md#settings-distributed_replica_error_half_life)
