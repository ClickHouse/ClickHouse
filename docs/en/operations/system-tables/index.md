---
toc_priority: 52
toc_title: System Tables
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

Unlike other system tables, the system tables [metric_log](../../operations/system-tables/metric_log.md#system_tables-metric_log), [query_log](../../operations/system-tables/query_log.md#system_tables-query_log), [query_thread_log](../../operations/system-tables/query_thread_log.md#system_tables-query_thread_log), [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log) are served by [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table engine and store their data in a storage filesystem. If you remove a table from a filesystem, the ClickHouse server creates the empty one again at the time of the next data writing. If system table schema changed in a new release, then ClickHouse renames the current table and creates a new one.

By default, table growth is unlimited. To control a size of a table, you can use [TTL](../../sql-reference/statements/alter/ttl.md#manipulations-with-table-ttl) settings for removing outdated log records. Also you can use the partitioning feature of `MergeTree`-engine tables.

## Sources of System Metrics {#system-tables-sources-of-system-metrics}

For collecting system metrics ClickHouse server uses:

-   `CAP_NET_ADMIN` capability.
-   [procfs](https://en.wikipedia.org/wiki/Procfs) (only in Linux).

**procfs**

If ClickHouse server doesn’t have `CAP_NET_ADMIN` capability, it tries to fall back to `ProcfsMetricsProvider`. `ProcfsMetricsProvider` allows collecting per-query system metrics (for CPU and I/O).

If procfs is supported and enabled on the system, ClickHouse server collects these metrics:

-   `OSCPUVirtualTimeMicroseconds`
-   `OSCPUWaitMicroseconds`
-   `OSIOWaitMicroseconds`
-   `OSReadChars`
-   `OSWriteChars`
-   `OSReadBytes`
-   `OSWriteBytes`

[Original article](https://clickhouse.tech/docs/en/operations/system-tables/) <!--hide-->
