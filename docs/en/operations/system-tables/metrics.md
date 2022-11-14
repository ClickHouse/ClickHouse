# metrics

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
┌─metric───────────────────────────────┬─value─┬─description────────────────────────────────────────────────────────────┐
│ Query                                │     1 │ Number of executing queries                                            │
│ Merge                                │     0 │ Number of executing background merges                                  │
│ PartMutation                         │     0 │ Number of mutations (ALTER DELETE/UPDATE)                              │
│ ReplicatedFetch                      │     0 │ Number of data parts being fetched from replicas                       │
│ ReplicatedSend                       │     0 │ Number of data parts being sent to replicas                            │
│ ReplicatedChecks                     │     0 │ Number of data parts checking for consistency                          │
│ BackgroundMergesAndMutationsPoolTask │     0 │ Number of active merges and mutations in an associated background pool │
│ BackgroundFetchesPoolTask            │     0 │ Number of active fetches in an associated background pool              │
│ BackgroundCommonPoolTask             │     0 │ Number of active tasks in an associated background pool                │
│ BackgroundMovePoolTask               │     0 │ Number of active tasks in BackgroundProcessingPool for moves           │
└──────────────────────────────────────┴───────┴────────────────────────────────────────────────────────────────────────┘
```

**See Also**

-   [system.asynchronous_metrics](../../operations/system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [system.events](../../operations/system-tables/events.md#system_tables-events) — Contains a number of events that occurred.
-   [system.metric_log](../../operations/system-tables/metric_log.md#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` and `system.events`.
-   [Monitoring](../../operations/monitoring.md) — Base concepts of ClickHouse monitoring.

[Original article](https://clickhouse.com/docs/en/operations/system-tables/metrics) <!--hide-->
