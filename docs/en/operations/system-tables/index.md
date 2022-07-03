---
sidebar_position: 52
sidebar_label: System Tables
---

# System Tables {#system-tables}

## Introduction {#system-tables-introduction}

System tables provide information about:

-   Server states, processes, and environment.
-   Server’s internal processes.

System tables:

-   Located in the `system` database.
-   Available only for reading data.
-   Can’t be dropped or altered, but can be detached.

Most of system tables store their data in RAM. A ClickHouse server creates such system tables at the start.

Unlike other system tables, the system log tables [metric_log](../../operations/system-tables/metric_log.md), [query_log](../../operations/system-tables/query_log.md), [query_thread_log](../../operations/system-tables/query_thread_log.md), [trace_log](../../operations/system-tables/trace_log.md), [part_log](../../operations/system-tables/part_log.md), [crash_log](../../operations/system-tables/crash-log.md) and [text_log](../../operations/system-tables/text_log.md) are served by [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table engine and store their data in a filesystem by default. If you remove a table from a filesystem, the ClickHouse server creates the empty one again at the time of the next data writing. If system table schema changed in a new release, then ClickHouse renames the current table and creates a new one.

System log tables can be customized by creating a config file with the same name as the table under `/etc/clickhouse-server/config.d/`, or setting corresponding elements in `/etc/clickhouse-server/config.xml`. Elements can be customized are:

-   `database`: database the system log table belongs to. This option is deprecated now. All system log tables are under database `system`.
-   `table`: table to insert data.
-   `partition_by`: specify [PARTITION BY](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) expression.
-   `ttl`: specify table [TTL](../../sql-reference/statements/alter/ttl.md) expression.
-   `flush_interval_milliseconds`: interval of flushing data to disk.
-   `engine`: provide full engine expression (starting with `ENGINE =` ) with parameters. This option is contradict with `partition_by` and `ttl`. If set together, the server would raise an exception and exit.

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
    </query_log>
</clickhouse>
```

By default, table growth is unlimited. To control a size of a table, you can use [TTL](../../sql-reference/statements/alter/ttl.md#manipulations-with-table-ttl) settings for removing outdated log records. Also you can use the partitioning feature of `MergeTree`-engine tables.

## Sources of System Metrics {#system-tables-sources-of-system-metrics}

For collecting system metrics ClickHouse server uses:

-   `CAP_NET_ADMIN` capability.
-   [procfs](https://en.wikipedia.org/wiki/Procfs) (only in Linux).

**procfs**

If ClickHouse server does not have `CAP_NET_ADMIN` capability, it tries to fall back to `ProcfsMetricsProvider`. `ProcfsMetricsProvider` allows collecting per-query system metrics (for CPU and I/O).

If procfs is supported and enabled on the system, ClickHouse server collects these metrics:

-   `OSCPUVirtualTimeMicroseconds`
-   `OSCPUWaitMicroseconds`
-   `OSIOWaitMicroseconds`
-   `OSReadChars`
-   `OSWriteChars`
-   `OSReadBytes`
-   `OSWriteBytes`

[Original article](https://clickhouse.com/docs/en/operations/system-tables/) <!--hide-->
