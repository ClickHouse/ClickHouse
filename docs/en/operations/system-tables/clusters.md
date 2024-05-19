---
slug: /en/operations/system-tables/clusters
---
# clusters

Contains information about clusters available in the config file and the servers in them.

Columns:

- `cluster` ([String](../../sql-reference/data-types/string.md)) — The cluster name.
- `shard_num` ([UInt32](../../sql-reference/data-types/int-uint.md)) — The shard number in the cluster, starting from 1.
- `shard_weight` ([UInt32](../../sql-reference/data-types/int-uint.md)) — The relative weight of the shard when writing data.
- `replica_num` ([UInt32](../../sql-reference/data-types/int-uint.md)) — The replica number in the shard, starting from 1.
- `host_name` ([String](../../sql-reference/data-types/string.md)) — The host name, as specified in the config.
- `host_address` ([String](../../sql-reference/data-types/string.md)) — The host IP address obtained from DNS.
- `port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — The port to use for connecting to the server.
- `is_local` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Flag that indicates whether the host is local.
- `user` ([String](../../sql-reference/data-types/string.md)) — The name of the user for connecting to the server.
- `default_database` ([String](../../sql-reference/data-types/string.md)) — The default database name.
- `errors_count` ([UInt32](../../sql-reference/data-types/int-uint.md)) — The number of times this host failed to reach replica.
- `slowdowns_count` ([UInt32](../../sql-reference/data-types/int-uint.md)) — The number of slowdowns that led to changing replica when establishing a connection with hedged requests.
- `estimated_recovery_time` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Seconds remaining until the replica error count is zeroed, and it is considered to be back to normal.
- `database_shard_name` ([String](../../sql-reference/data-types/string.md)) — The name of the `Replicated` database shard (for clusters that belong to a `Replicated` database).
- `database_replica_name` ([String](../../sql-reference/data-types/string.md)) — The name of the `Replicated` database replica (for clusters that belong to a `Replicated` database).
- `is_active` ([Nullable(UInt8)](../../sql-reference/data-types/int-uint.md)) — The status of the `Replicated` database replica (for clusters that belong to a `Replicated` database): 1 means "replica is online", 0 means "replica is offline", `NULL` means "unknown".
- `name` ([String](../../sql-reference/data-types/string.md)) - An alias to cluster.

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
database_shard_name:
database_replica_name:
is_active:               NULL

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
database_shard_name:
database_replica_name:
is_active:               NULL
```

**See Also**

- [Table engine Distributed](../../engines/table-engines/special/distributed.md)
- [distributed_replica_error_cap setting](../../operations/settings/settings.md#distributed_replica_error_cap)
- [distributed_replica_error_half_life setting](../../operations/settings/settings.md#distributed_replica_error_half_life)
