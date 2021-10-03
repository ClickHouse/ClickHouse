# system.clusters {#system-clusters}

Contains information about clusters available in the config file and the servers in them.

Columns:

-   `cluster` ([String](../../sql-reference/data-types/string.md)) — The cluster name.
-   `shard_num` ([UInt32](../../sql-reference/data-types/int-uint.md)) — The shard number in the cluster, starting from 1.
-   `shard_weight` ([UInt32](../../sql-reference/data-types/int-uint.md)) — The relative weight of the shard when writing data.
-   `replica_num` ([UInt32](../../sql-reference/data-types/int-uint.md)) — The replica number in the shard, starting from 1.
-   `host_name` ([String](../../sql-reference/data-types/string.md)) — The host name, as specified in the config.
-   `host_address` ([String](../../sql-reference/data-types/string.md)) — The host IP address obtained from DNS.
-   `port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — The port to use for connecting to the server.
-   `is_local` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Flag that indicates whether the host is local.
-   `user` ([String](../../sql-reference/data-types/string.md)) — The name of the user for connecting to the server.
-   `default_database` ([String](../../sql-reference/data-types/string.md)) — The default database name.
-   `errors_count` ([UInt32](../../sql-reference/data-types/int-uint.md)) — The number of times this host failed to reach replica.
-   `slowdowns_count` ([UInt32](../../sql-reference/data-types/int-uint.md)) — The number of slowdowns that led to changing replica when establishing a connection with hedged requests.
-   `estimated_recovery_time` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Seconds remaining until the replica error count is zeroed and it is considered to be back to normal.

**Example**

Query:

```sql
SELECT * FROM system.clusters LIMIT 2 FORMAT Vertical;
```

Result:

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

**See Also**

-   [Table engine Distributed](../../engines/table-engines/special/distributed.md)
-   [distributed_replica_error_cap setting](../../operations/settings/settings.md#settings-distributed_replica_error_cap)
-   [distributed_replica_error_half_life setting](../../operations/settings/settings.md#settings-distributed_replica_error_half_life)

[Original article](https://clickhouse.com/docs/en/operations/system-tables/clusters) <!--hide-->
