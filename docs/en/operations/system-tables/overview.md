---
description: 'Overview of what system tables are and why they are useful.'
keywords: ['system tables', 'overview']
sidebar_label: 'Overview'
sidebar_position: 52
slug: /operations/system-tables/overview
title: 'System Tables Overview'
doc_type: 'reference'
---

## System tables overview {#system-tables-introduction}

System tables provide information about:

- Server states, processes, and environment.
- Server's internal processes.
- Options used when the ClickHouse binary was built.

System tables:

- Located in the `system` database.
- Available only for reading data.
- Can't be dropped or altered, but can be detached.

Most of the system tables store their data in RAM. A ClickHouse server creates such system tables at the start.

Unlike other system tables, the system log tables [metric_log](../../operations/system-tables/metric_log.md), [query_log](../../operations/system-tables/query_log.md), [query_thread_log](../../operations/system-tables/query_thread_log.md), [trace_log](../../operations/system-tables/trace_log.md), [part_log](../../operations/system-tables/part_log.md), [crash_log](../../operations/system-tables/crash_log.md), [text_log](../../operations/system-tables/text_log.md) and [backup_log](../../operations/system-tables/backup_log.md) are served by [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table engine and store their data in a filesystem by default. If you remove a table from a filesystem, the ClickHouse server creates the empty one again at the time of the next data writing. If system table schema changed in a new release, then ClickHouse renames the current table and creates a new one.

System log tables can be customized by creating a config file with the same name as the table under `/etc/clickhouse-server/config.d/`, or setting corresponding elements in `/etc/clickhouse-server/config.xml`. Elements can be customized are:

- `database`: database the system log table belongs to. This option is deprecated now. All system log tables are under database `system`.
- `table`: table to insert data.
- `partition_by`: specify [PARTITION BY](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) expression.
- `ttl`: specify table [TTL](../../sql-reference/statements/alter/ttl.md) expression.
- `flush_interval_milliseconds`: interval of flushing data to disk.
- `engine`: provide full engine expression (starting with `ENGINE =` ) with parameters. This option conflicts with `partition_by` and `ttl`. If set together, the server will raise an exception and exit.

An example:

```xml
<clickhouse>
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <ttl>event_date + INTERVAL 30 DAY DELETE</ttl>
        <!--
        <engine>ENGINE = MergeTree PARTITION BY toYYYYMM(event_date) ORDER BY (event_date, event_time) SETTINGS index_granularity = 1024</engine>
        -->
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </query_log>
</clickhouse>
```

By default, table growth is unlimited. To control a size of a table, you can use [TTL](/sql-reference/statements/alter/ttl) settings for removing outdated log records. Also you can use the partitioning feature of `MergeTree`-engine tables.

## Sources of System Metrics {#system-tables-sources-of-system-metrics}

For collecting system metrics ClickHouse server uses:

- `CAP_NET_ADMIN` capability.
- [procfs](https://en.wikipedia.org/wiki/Procfs) (only in Linux).

**procfs**

If ClickHouse server does not have `CAP_NET_ADMIN` capability, it tries to fall back to `ProcfsMetricsProvider`. `ProcfsMetricsProvider` allows collecting per-query system metrics (for CPU and I/O).

If procfs is supported and enabled on the system, ClickHouse server collects these metrics:

- `OSCPUVirtualTimeMicroseconds`
- `OSCPUWaitMicroseconds`
- `OSIOWaitMicroseconds`
- `OSReadChars`
- `OSWriteChars`
- `OSReadBytes`
- `OSWriteBytes`

:::note
`OSIOWaitMicroseconds` is disabled by default in Linux kernels starting from 5.14.x.
You can enable it using `sudo sysctl kernel.task_delayacct=1` or by creating a `.conf` file in `/etc/sysctl.d/` with `kernel.task_delayacct = 1`
:::

## System tables in ClickHouse Cloud {#system-tables-in-clickhouse-cloud}

In ClickHouse Cloud, system tables provide critical insights into the state and performance of the service, just as they do in self-managed deployments. Some system tables operate at the cluster-wide level, especially those that derive their data from Keeper nodes, which manage distributed metadata. These tables reflect the collective state of the cluster and should be consistent when queried on individual nodes. For example, the [`parts`](/operations/system-tables/parts) should be consistent irrespective of the node it is queried from:

```sql
SELECT hostname(), count()
FROM system.parts
WHERE `table` = 'pypi'

┌─hostname()────────────────────┬─count()─┐
│ c-ecru-qn-34-server-vccsrty-0 │      26 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.005 sec.

SELECT
 hostname(),
    count()
FROM system.parts
WHERE `table` = 'pypi'

┌─hostname()────────────────────┬─count()─┐
│ c-ecru-qn-34-server-w59bfco-0 │      26 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.004 sec.
```

Conversely, other system tables are node-specific e.g. in-memory or persisting their data using the MergeTree table engine. This is typical for data such as logs and metrics. This persistence ensures that historical data remains available for analysis. However, these node-specific tables are inherently unique to each node.

In general, the following rules can be applied when determining if a system table is node-specific:

- System tables with a `_log` suffix.
- System tables that expose metrics e.g. `metrics`, `asynchronous_metrics`, `events`.
- System tables that expose ongoing processes e.g. `processes`, `merges`.

Additionally, new versions of system tables may be created as a result of upgrades or changes to their schema. These versions are named using a numerical suffix.

For example, consider the `system.query_log` tables, which contain a row for each query executed by the node:

```sql
SHOW TABLES FROM system LIKE 'query_log%'

┌─name─────────┐
│ query_log    │
│ query_log_1  │
│ query_log_10 │
│ query_log_2  │
│ query_log_3  │
│ query_log_4  │
│ query_log_5  │
│ query_log_6  │
│ query_log_7  │
│ query_log_8  │
│ query_log_9  │
└──────────────┘

11 rows in set. Elapsed: 0.004 sec.
```

### Querying multiple versions {#querying-multiple-versions}

We can query across these tables using the [`merge`](/sql-reference/table-functions/merge) function. For example, the query below identifies the latest query issued to the target node in each `query_log` table:

```sql
SELECT
    _table,
    max(event_time) AS most_recent
FROM merge('system', '^query_log')
GROUP BY _table
ORDER BY most_recent DESC

┌─_table───────┬─────────most_recent─┐
│ query_log    │ 2025-04-13 10:59:29 │
│ query_log_1  │ 2025-04-09 12:34:46 │
│ query_log_2  │ 2025-04-09 12:33:45 │
│ query_log_3  │ 2025-04-07 17:10:34 │
│ query_log_5  │ 2025-03-24 09:39:39 │
│ query_log_4  │ 2025-03-24 09:38:58 │
│ query_log_6  │ 2025-03-19 16:07:41 │
│ query_log_7  │ 2025-03-18 17:01:07 │
│ query_log_8  │ 2025-03-18 14:36:07 │
│ query_log_10 │ 2025-03-18 14:01:33 │
│ query_log_9  │ 2025-03-18 14:01:32 │
└──────────────┴─────────────────────┘

11 rows in set. Elapsed: 0.373 sec. Processed 6.44 million rows, 25.77 MB (17.29 million rows/s., 69.17 MB/s.)
Peak memory usage: 28.45 MiB.
```

:::note Don't rely on the numerical suffix for ordering
While the numeric suffix on tables can suggest the order of data, it should never be relied upon. For this reason, always use the merge table function combined with a date filter when targeting specific date ranges.
:::

Importantly, these tables are still **local to each node**.

### Querying across nodes {#querying-across-nodes}

To comprehensively view the entire cluster, users can leverage the [`clusterAllReplicas`](/sql-reference/table-functions/cluster) function in combination with the `merge` function. The `clusterAllReplicas` function allows querying system tables across all replicas within the "default" cluster, consolidating node-specific data into a unified result. When combined with the `merge` function this can be used to target all system data for a specific table in a cluster. 

This approach is particularly valuable for monitoring and debugging cluster-wide operations, ensuring users can effectively analyze the health and performance of their ClickHouse Cloud deployment.

:::note
ClickHouse Cloud provides clusters of multiple replicas for redundancy and failover. This enables its features, such as dynamic autoscaling and zero-downtime upgrades. At a certain moment in time, new nodes could be in the process of being added to the cluster or removed from the cluster. To skip these nodes, add `SETTINGS skip_unavailable_shards = 1` to queries using `clusterAllReplicas` as shown below.
:::

For example, consider the difference when querying the `query_log` table - often essential to analysis.

```sql
SELECT
    hostname() AS host,
    count()
FROM system.query_log
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │  650543 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.010 sec. Processed 17.87 thousand rows, 71.51 KB (1.75 million rows/s., 7.01 MB/s.)

SELECT
    hostname() AS host,
    count()
FROM clusterAllReplicas('default', system.query_log)
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host SETTINGS skip_unavailable_shards = 1

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │  650543 │
│ c-ecru-qn-34-server-6em4y4t-0 │  656029 │
│ c-ecru-qn-34-server-iejrkg0-0 │  641155 │
└───────────────────────────────┴─────────┘

3 rows in set. Elapsed: 0.026 sec. Processed 1.97 million rows, 7.88 MB (75.51 million rows/s., 302.05 MB/s.)
```

### Querying across nodes and versions {#querying-across-nodes-and-versions}

Due to system table versioning this still does not represent the full data in the cluster. When combining the above with the `merge` function we get an accurate result for our date range:

```sql
SELECT
    hostname() AS host,
    count()
FROM clusterAllReplicas('default', merge('system', '^query_log'))
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host SETTINGS skip_unavailable_shards = 1

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │ 3008000 │
│ c-ecru-qn-34-server-6em4y4t-0 │ 3659443 │
│ c-ecru-qn-34-server-iejrkg0-0 │ 1078287 │
└───────────────────────────────┴─────────┘

3 rows in set. Elapsed: 0.462 sec. Processed 7.94 million rows, 31.75 MB (17.17 million rows/s., 68.67 MB/s.)
```

## Related content {#related-content}

- Blog: [System Tables and a window into the internals of ClickHouse](https://clickhouse.com/blog/clickhouse-debugging-issues-with-system-tables)
- Blog: [Essential monitoring queries - part 1 - INSERT queries](https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse)
- Blog: [Essential monitoring queries - part 2 - SELECT queries](https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse)
