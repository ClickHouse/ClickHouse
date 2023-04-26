# system.clusters{#system-clusters}

包含有关配置文件中可用的集群及其中的服务器的信息。

列:

-   `cluster` (String) — 集群名。
-   `shard_num` (UInt32) — 集群中的分片数，从1开始。
-   `shard_weight` (UInt32) — 写数据时该分片的相对权重。
-   `replica_num` (UInt32) — 分片的副本数量，从1开始。
-   `host_name` (String) — 配置中指定的主机名。
-   `host_address` (String) — 从DNS获取的主机IP地址。
-   `port` (UInt16) — 连接到服务器的端口。
-   `user` (String) — 连接到服务器的用户名。
-   `errors_count` (UInt32) - 此主机无法访问副本的次数。
-   `slowdowns_count` (UInt32) - 与对冲请求建立连接时导致更改副本的减速次数。
-   `estimated_recovery_time` (UInt32) - 剩下的秒数，直到副本错误计数归零并被视为恢复正常。

请注意 `errors_count` 每个查询集群更新一次，但 `estimated_recovery_time` 按需重新计算。 所以有可能是非零的情况 `errors_count` 和零 `estimated_recovery_time`，下一个查询将为零 `errors_count` 并尝试使用副本，就好像它没有错误。

**另请参阅**

-   [表引擎分布式](../../engines/table-engines/special/distributed.md)
-   [distributed_replica_error_cap设置](../../operations/settings/settings.md#settings-distributed_replica_error_cap)
-   [distributed_replica_error_half_life设置](../../operations/settings/settings.md#settings-distributed_replica_error_half_life)

[原文](https://clickhouse.com/docs/zh/operations/system-tables/clusters) <!--hide-->
