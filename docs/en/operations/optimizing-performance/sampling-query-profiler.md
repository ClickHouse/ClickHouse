---
toc_priority: 54
toc_title: Query Profiling
---

# Sampling Query Profiler {#sampling-query-profiler}

ClickHouse runs sampling profiler that allows analyzing query execution. Using profiler you can find source code routines that used the most frequently during query execution. You can trace CPU time and wall-clock time spent including idle time.

To use profiler:

-   Setup the [trace\_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) section of the server configuration.

    This section configures the [trace\_log](../../operations/system-tables/trace_log.md#system_tables-trace_log) system table containing the results of the profiler functioning. It is configured by default. Remember that data in this table is valid only for a running server. After the server restart, ClickHouse doesn’t clean up the table and all the stored virtual memory address may become invalid.

-   Setup the [query\_profiler\_cpu\_time\_period\_ns](../../operations/settings/settings.md#query_profiler_cpu_time_period_ns) or [query\_profiler\_real\_time\_period\_ns](../../operations/settings/settings.md#query_profiler_real_time_period_ns) settings. Both settings can be used simultaneously.

    These settings allow you to configure profiler timers. As these are the session settings, you can get different sampling frequency for the whole server, individual users or user profiles, for your interactive session, and for each individual query.

The default sampling frequency is one sample per second and both CPU and real timers are enabled. This frequency allows collecting enough information about ClickHouse cluster. At the same time, working with this frequency, profiler doesn’t affect ClickHouse server’s performance. If you need to profile each individual query try to use higher sampling frequency.

To analyze the `trace_log` system table:

-   Install the `clickhouse-common-static-dbg` package. See [Install from DEB Packages](../../getting-started/install.md#install-from-deb-packages).

-   Allow introspection functions by the [allow\_introspection\_functions](../../operations/settings/settings.md#settings-allow_introspection_functions) setting.

    For security reasons, introspection functions are disabled by default.

-   Use the `addressToLine`, `addressToSymbol` and `demangle` [introspection functions](../../sql-reference/functions/introspection.md) to get function names and their positions in ClickHouse code. To get a profile for some query, you need to aggregate data from the `trace_log` table. You can aggregate data by individual functions or by the whole stack traces.

If you need to visualize `trace_log` info, try [flamegraph](../../interfaces/third-party/gui/#clickhouse-flamegraph) and [speedscope](https://github.com/laplab/clickhouse-speedscope).

## Example {#example}

In this example we:

-   Filtering `trace_log` data by a query identifier and the current date.

-   Aggregating by stack trace.

-   Using introspection functions, we will get a report of:

    -   Names of symbols and corresponding source code functions.
    -   Source code locations of these functions.

<!-- -->

``` sql
SELECT
    count(),
    arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
FROM system.trace_log
WHERE (query_id = 'ebca3574-ad0a-400a-9cbc-dca382f5998c') AND (event_date = today())
GROUP BY trace
ORDER BY count() DESC
LIMIT 10
```

``` text
{% include "examples/sampling_query_profiler_result.txt" %}
```
