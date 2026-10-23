---
description: 'Documentation for the sampling query profiler tool in ClickHouse'
sidebar_label: 'Query Profiling'
sidebar_position: 54
slug: /operations/optimizing-performance/sampling-query-profiler
title: 'Sampling query profiler'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Sampling query profiler

ClickHouse runs a sampling profiler that allows analyzing query execution.
Using the profiler, you can find the source code routines that are used the most frequently during query execution.
You can trace CPU time and wall-clock time spent including idle time.

The query profiler is automatically enabled in ClickHouse Cloud.
The following example query finds the most frequent stack traces for a profiled query, with resolved function names and source locations:

:::tip
Replace the `query_id` value with the ID of the query you want to profile.
:::

<Tabs groupId="deployment">
<TabItem value="cloud" label="ClickHouse Cloud">

In ClickHouse Cloud, you can obtain the query ID by clicking **"..."** on the far right of the bar above the query result table (next to the table/chart toggle). This opens a context menu where you can click **"Copy query ID"**.

Use `clusterAllReplicas(default, system.trace_log)` to select from all nodes of the cluster:

```sql
SELECT
    count(),
    arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
FROM clusterAllReplicas(default, system.trace_log)
WHERE query_id = '<query_id>' AND trace_type = 'CPU' AND event_date = today()
GROUP BY trace
ORDER BY count() DESC
LIMIT 10
SETTINGS allow_introspection_functions = 1
```

</TabItem>
<TabItem value="self-managed" label="Self-managed">

```sql
SELECT
    count(),
    arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
FROM system.trace_log
WHERE query_id = '<query_id>' AND trace_type = 'CPU' AND event_date = today()
GROUP BY trace
ORDER BY count() DESC
LIMIT 10
SETTINGS allow_introspection_functions = 1
```

</TabItem>
</Tabs>

## Using the query profiler in self-managed deployments {#self-managed-query-profiler}

In self-managed deployments, to use the query profiler follow the steps below:

<VerticalStepper headerLevel="h3">

### Install ClickHouse with debug info {#debug-info}

Install the `clickhouse-common-static-dbg` package:
1. Follow the instructions in step ["Set up the Debian repository"](/install/debian_ubuntu#setup-the-debian-repository)
2. Run `sudo apt-get install clickhouse-server clickhouse-client clickhouse-common-static-dbg` to install ClickHouse compiled binary files with debug info
3. Run `sudo service clickhouse-server start` to start the server
4. Run `clickhouse-client`. The debug symbols from clickhouse-common-static-dbg will automatically be picked up by the server - you don't need to do anything special to enable them

### Check server config {#server-config}

Ensure that the [`trace_log`](../../operations/server-configuration-parameters/settings.md#trace_log) section of your [server configuration file](/operations/configuration-files) is set up. It is enabled by default:

```xml
<!-- Trace log. Stores stack traces collected by query profilers.
     See query_profiler_real_time_period_ns and query_profiler_cpu_time_period_ns settings. -->
<trace_log>
    <database>system</database>
    <table>trace_log</table>

    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <!-- Indication whether logs should be dumped to the disk in case of a crash -->
    <flush_on_crash>false</flush_on_crash>
    <symbolize>true</symbolize>
</trace_log>
```

This section configures the [trace_log](/operations/system-tables/trace_log) system table containing the results of the profiler functioning.
Remember that data in this table is valid only for a running server.
After the server restart, ClickHouse does not clean up the table and all the stored virtual memory address may become invalid.

### Configure profile timers {#configure-profile-timers}

Set up the [`query_profiler_cpu_time_period_ns`](../../operations/settings/settings.md#query_profiler_cpu_time_period_ns) or [`query_profiler_real_time_period_ns`](../../operations/settings/settings.md#query_profiler_real_time_period_ns) settings.
Both settings can be used simultaneously.

These settings allow you to configure profiler timers.
As these are the session settings, you can get different sampling frequency for the whole server, individual users or user profiles, for your interactive session, and for each individual query.

The default sampling frequency is one sample per second, and both CPU and real timers are enabled.
This frequency allows you to collect sufficient information about your ClickHouse cluster whilst not affecting your server's performance.
If you need to profile each individual query, use a higher sampling frequency.

### Analyze the `trace_log` system table {#analyze-trace-log-system-table}

To analyze the `trace_log` system table allow introspection functions with the [`allow_introspection_functions`](../../operations/settings/settings.md#allow_introspection_functions) setting:

```sql
SET allow_introspection_functions=1
```

:::note
For security reasons, introspection functions are disabled by default
:::

Use the `addressToLine`, `addressToLineWithInlines`, `addressToSymbol` and `demangle` [introspection functions](../../sql-reference/functions/introspection.md) to get function names and their positions in ClickHouse code.
To get a profile for some query, you need to aggregate data from the `trace_log` table.
You can aggregate data by individual functions or by the whole stack traces.

:::tip
If you need to visualize `trace_log` info, try [flamegraph](/interfaces/third-party/gui#clickhouse-flamegraph) and [speedscope](https://www.speedscope.app).
:::

</VerticalStepper>

## Building flame graphs with the `flameGraph` function {#flamegraph}

ClickHouse provides the [`flameGraph`](/sql-reference/aggregate-functions/reference/flame_graph) aggregate function which builds a flame graph directly from stack traces stored in `trace_log`.
The output is an array of strings in a format compatible with [flamegraph.pl](https://github.com/brendangregg/FlameGraph).

**Syntax:**

```sql
flameGraph(traces, [size = 1], [ptr = 0])
```

**Arguments:**

- `traces` — a stacktrace. [`Array(UInt64)`](/sql-reference/data-types/array).
- `size` — an allocation size for memory profiling. [`Int64`](/sql-reference/data-types/int-uint).
- `ptr` — an allocation address. [`UInt64`](/sql-reference/data-types/int-uint).

When `ptr` is non-zero, `flameGraph` maps allocations (`size > 0`) and deallocations (`size < 0`) with the same size and pointer.
Only allocations that were not freed are shown.
Unmatched deallocations are ignored.

### CPU flame graph {#cpu-flame-graph}

:::note
The queries below require you to have [flamegraph.pl](https://github.com/brendangregg/FlameGraph) installed.

You can do so by running:

```bash
git clone https://github.com/brendangregg/FlameGraph
# Then use it as:
# ~/FlameGraph/flamegraph.pl
```

Replace `flamegraph.pl` in the following queries with the path where `flamegraph.pl` is located on your machine
:::

```sql
SET query_profiler_cpu_time_period_ns = 10000000;
```

Run your query, then build the flame graph:

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(arrayReverse(trace)))
        FROM system.trace_log
        WHERE trace_type = 'CPU' AND query_id = '<query_id>'" \
    | flamegraph.pl > flame_cpu.svg
```

### Memory flame graph — all allocations {#memory-flame-graph-all}

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1;
```

Run your query, then build the flame graph:

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size))
        FROM system.trace_log
        WHERE trace_type = 'MemorySample' AND query_id = '<query_id>'" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem.svg
```

### Memory flame graph — unfreed allocations {#memory-flame-graph-unfreed}

This variant matches allocations against deallocations by pointer and shows only memory that was not freed during the query.

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1,
    use_uncompressed_cache = 1,
    merge_tree_max_rows_to_use_cache = 100000000000,
    merge_tree_max_bytes_to_use_cache = 1000000000000;
```

Run the following query to build the flame graph:

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size, ptr))
        FROM system.trace_log
        WHERE trace_type = 'MemorySample' AND query_id = '<query_id>'" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_unfreed.svg
```

### Memory flame graph — active allocations at a point in time {#memory-flame-graph-time-point}

This approach lets you find peak memory usage and visualize what was allocated at that moment.

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1;
```

#### Find memory usage over time {#find-memory-usage-over-time}

```sql
SELECT
    event_time,
    formatReadableSize(max(s)) AS m
FROM (
    SELECT
        event_time,
        sum(size) OVER (ORDER BY event_time) AS s
    FROM system.trace_log
    WHERE query_id = '<query_id>' AND trace_type = 'MemorySample'
)
GROUP BY event_time
ORDER BY event_time;
```

#### Find the time point with maximum memory usage {#find-time-point-maximum-memory-usage}

```sql
SELECT
    argMax(event_time, s),
    max(s)
FROM (
    SELECT
        event_time,
        sum(size) OVER (ORDER BY event_time) AS s
    FROM system.trace_log
    WHERE query_id = '<query_id>' AND trace_type = 'MemorySample'
);
```

#### Build a flame graph of active allocations at that time point {#build-flame-graph}

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size, ptr))
        FROM (
            SELECT * FROM system.trace_log
            WHERE trace_type = 'MemorySample'
              AND query_id = '<query_id>'
              AND event_time <= '<time_point>'
            ORDER BY event_time
        )" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_time_point_pos.svg
```

#### Build a flame graph of deallocations after that time point (to understand what was freed later) {#build-flame-graph-deallocations}

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, -size, ptr))
        FROM (
            SELECT * FROM system.trace_log
            WHERE trace_type = 'MemorySample'
              AND query_id = '<query_id>'
              AND event_time > '<time_point>'
            ORDER BY event_time DESC
        )" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_time_point_neg.svg
```

## Example {#example}

The code snippet below:
- Filters `trace_log` data by a query identifier and the current date.
- Aggregates by stack trace.
- Uses introspection functions to get a report of:
  - The names of symbols and corresponding source code functions.
  - The source code locations of these functions.

```sql
SELECT
    count(),
    arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
FROM system.trace_log
WHERE (query_id = '<query_id>') AND (event_date = today())
GROUP BY trace
ORDER BY count() DESC
LIMIT 10
```
