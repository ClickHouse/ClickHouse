# system.clusters {#system-clusters}

Contains information about clusters available in the config file and the servers in them.

Columns:

-   `cluster` (String) — The cluster name.
-   `shard_num` (UInt32) — The shard number in the cluster, starting from 1.
-   `shard_weight` (UInt32) — The relative weight of the shard when writing data.
-   `replica_num` (UInt32) — The replica number in the shard, starting from 1.
-   `host_name` (String) — The host name, as specified in the config.
-   `host_address` (String) — The host IP address obtained from DNS.
-   `port` (UInt16) — The port to use for connecting to the server.
-   `user` (String) — The name of the user for connecting to the server.
-   `errors_count` (UInt32) - number of times this host failed to reach replica.
-   `estimated_recovery_time` (UInt32) - seconds left until replica error count is zeroed and it is considered to be back to normal.

Please note that `errors_count` is updated once per query to the cluster, but `estimated_recovery_time` is recalculated on-demand. So there could be a case of non-zero `errors_count` and zero `estimated_recovery_time`, that next query will zero `errors_count` and try to use replica as if it has no errors.

**See also**

-   [Table engine Distributed](../../engines/table-engines/special/distributed.md)
-   [distributed_replica_error_cap setting](../../operations/settings/settings.md#settings-distributed_replica_error_cap)
-   [distributed_replica_error_half_life setting](../../operations/settings/settings.md#settings-distributed_replica_error_half_life)

**Example**

```sql
:) SELECT * FROM system.clusters LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
cluster:                 test_cluster
shard_num:               1
shard_weight:            1
replica_num:             1
host_name:               clickhouse01
host_address:            172.23.0.11
port:                    9000
is_local:                1
user:                    default
default_database:        
errors_count:            0
estimated_recovery_time: 0

Row 2:
──────
cluster:                 test_cluster
shard_num:               1
shard_weight:            1
replica_num:             2
host_name:               clickhouse02
host_address:            172.23.0.12
port:                    9000
is_local:                0
user:                    default
default_database:        
errors_count:            0
estimated_recovery_time: 0

2 rows in set. Elapsed: 0.002 sec. 
```

[Original article](https://clickhouse.tech/docs/en/operations/system_tables/clusters) <!--hide-->
