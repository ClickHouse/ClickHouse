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
-   [distributed\_replica\_error\_cap setting](../../operations/settings/settings.md#settings-distributed_replica_error_cap)
-   [distributed\_replica\_error\_half\_life setting](../../operations/settings/settings.md#settings-distributed_replica_error_half_life)
