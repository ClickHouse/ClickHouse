# system.cluster {#system-clusters}

包含有关配置文件中可用的集群及其中的服务器的信息。

列:

-   `cluster` (String) — 集群名称。
-   `shard_num` (UInt32) — 集群中的分片号，从1开始。
-   `shard_weight` (UInt32) — 写入数据时分片的相对权重。
-   `replica_num` (UInt32) — 分片中的副本号，从1开始。
-   `host_name` (String) — 主机名，如在配置中指定的那样。
-   `host_address` (String) — 从DNS获得的主机IP地址。
-   `port` (UInt16) — 用于连接到服务器的端口。
-   `is_local` (UInt8) — 指示主机是否在本地的标志。
-   `user` (String) — 用于连接到服务器的用户名。
-   `errors_count` (UInt32)-此主机无法访问副本的次数。
-   `slowdowns_count` (UInt32) — 在与被对冲的请求建立连接时导致更改副本的速度下降的次数。
-   `estimated_recovery_time` (UInt32)-剩余秒数，直到副本错误计数归零且被视为恢复正常为止。

**示例**

查询:

```sql
SELECT * FROM system.clusters LIMIT 2 FORMAT Vertical;
```

结果:

```text
Row 1:
──────
cluster:                 test_cluster_two_shards
shard_num:               1
shard_weight:            1
replica_num:             1
host_name:               127.0.0.1
host_address:            127.0.0.1
port:                    9000
is_local:                1
user:                    default
default_database:
errors_count:            0
slowdowns_count:         0
estimated_recovery_time: 0

Row 2:
──────
cluster:                 test_cluster_two_shards
shard_num:               2
shard_weight:            1
replica_num:             1
host_name:               127.0.0.2
host_address:            127.0.0.2
port:                    9000
is_local:                0
user:                    default
default_database:
errors_count:            0
slowdowns_count:         0
estimated_recovery_time: 0
```

**另请参阅**

-   [分布式表引擎](../../engines/table-engines/special/distributed.md)
-   [distributed_replica_error_cap设置](../../operations/settings/settings.md#settings-distributed_replica_error_cap)
-   [distributed_replica_error_half_life设置](../../operations/settings/settings.md#settings-distributed_replica_error_half_life)

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/clusters) <!--hide-->
