---
slug: /en/operations/system-tables/
sidebar_position: 52
sidebar_label: Overview
pagination_next: 'en/operations/system-tables/asynchronous_metric_log'
---

# System Tables

## Introduction {#system-tables-introduction}

System tables provide information about:

- Server states, processes, and environment.
- Server’s internal processes.
- Options used when the ClickHouse binary was built.

System tables:

- Located in the `system` database.
- Available only for reading data.
- Can’t be dropped or altered, but can be detached.

Most of system tables store their data in RAM. A ClickHouse server creates such system tables at the start.

Unlike other system tables, the system log tables [metric_log](../../operations/system-tables/metric_log.md), [query_log](../../operations/system-tables/query_log.md), [query_thread_log](../../operations/system-tables/query_thread_log.md), [trace_log](../../operations/system-tables/trace_log.md), [part_log](../../operations/system-tables/part_log.md), [crash_log](../../operations/system-tables/crash-log.md), [text_log](../../operations/system-tables/text_log.md) and [backup_log](../../operations/system-tables/backup_log.md) are served by [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table engine and store their data in a filesystem by default. If you remove a table from a filesystem, the ClickHouse server creates the empty one again at the time of the next data writing. If system table schema changed in a new release, then ClickHouse renames the current table and creates a new one.

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

By default, table growth is unlimited. To control a size of a table, you can use [TTL](../../sql-reference/statements/alter/ttl.md#manipulations-with-table-ttl) settings for removing outdated log records. Also you can use the partitioning feature of `MergeTree`-engine tables.

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

## Related content

- Blog: [System Tables and a window into the internals of ClickHouse](https://clickhouse.com/blog/clickhouse-debugging-issues-with-system-tables)
- Blog: [Essential monitoring queries - part 1 - INSERT queries](https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse)
- Blog: [Essential monitoring queries - part 2 - SELECT queries](https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse)

