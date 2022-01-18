# system.metrics {#system_tables-metrics}

Contains metrics which can be calculated instantly, or have a current value. For example, the number of simultaneously processed queries or the current replica delay. This table is always up to date.

Columns:

-   `metric` ([String](../../sql-reference/data-types/string.md)) — Metric name.
-   `value` ([Int64](../../sql-reference/data-types/int-uint.md)) — Metric value.
-   `description` ([String](../../sql-reference/data-types/string.md)) — Metric description.

The list of supported metrics you can find in the [src/Common/CurrentMetrics.cpp](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/CurrentMetrics.cpp) source file of ClickHouse.

**Example**

``` sql
SELECT * FROM system.metrics LIMIT 10
```

``` text
┌─metric─────────────────────┬─value─┬─description──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Query                      │     1 │ Number of executing queries                                                                                                                                                                      │
│ Merge                      │     0 │ Number of executing background merges                                                                                                                                                            │
│ PartMutation               │     0 │ Number of mutations (ALTER DELETE/UPDATE)                                                                                                                                                        │
│ ReplicatedFetch            │     0 │ Number of data parts being fetched from replicas                                                                                                                                                │
│ ReplicatedSend             │     0 │ Number of data parts being sent to replicas                                                                                                                                                      │
│ ReplicatedChecks           │     0 │ Number of data parts checking for consistency                                                                                                                                                    │
│ BackgroundPoolTask         │     0 │ Number of active tasks in BackgroundProcessingPool (merges, mutations, fetches, or replication queue bookkeeping)                                                                                │
│ BackgroundSchedulePoolTask │     0 │ Number of active tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old data parts, altering data parts, replica re-initialization, etc.   │
│ DiskSpaceReservedForMerge  │     0 │ Disk space reserved for currently running background merges. It is slightly more than the total size of currently merging parts.                                                                     │
│ DistributedSend            │     0 │ Number of connections to remote servers sending data that was INSERTed into Distributed tables. Both synchronous and asynchronous mode.                                                          │
└────────────────────────────┴───────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**See Also**

-   [system.asynchronous_metrics](../../operations/system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [system.events](../../operations/system-tables/events.md#system_tables-events) — Contains a number of events that occurred.
-   [system.metric_log](../../operations/system-tables/metric_log.md#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.
-   [Monitoring](../../operations/monitoring.md) — Base concepts of ClickHouse monitoring.

[Original article](https://clickhouse.tech/docs/en/operations/system-tables/metrics) <!--hide-->
