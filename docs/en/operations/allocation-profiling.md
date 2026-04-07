---
description: 'Page detailing allocation profiling in ClickHouse'
sidebar_label: 'Allocation profiling'
slug: /operations/allocation-profiling
title: 'Allocation profiling'
doc_type: 'guide'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Allocation profiling

ClickHouse uses [jemalloc](https://github.com/jemalloc/jemalloc) as its global allocator. Jemalloc comes with tools for allocation sampling and profiling.

ClickHouse and Keeper allow you to control sampling using configs, query settings, `SYSTEM` commands and four letter word (4LW) commands in Keeper. There are several ways to inspect the results:

- Collect samples into `system.trace_log` under the `JemallocSample` type for per-query analysis.
- View live memory statistics and fetch heap profiles through the built-in [jemalloc web UI](#jemalloc-web-ui) (26.2+).
- Query the current heap profile directly from SQL using [`system.jemalloc_profile_text`](#fetching-heap-profiles-from-sql) (26.2+).
- Flush heap profiles to disk and analyze them with [`jeprof`](#analyzing-heap-profile-files-with-jeprof).

:::note

This guide is applicable for versions 25.9+.
For older versions, please check [allocation profiling for versions before 25.9](/operations/allocation-profiling-old.md).

:::

## Sampling allocations {#sampling-allocations}

To sample and profile allocations, start ClickHouse/Keeper with the `jemalloc_enable_global_profiler` config enabled:

```xml
<clickhouse>
    <jemalloc_enable_global_profiler>1</jemalloc_enable_global_profiler>
</clickhouse>
```

`jemalloc` will sample allocations and store the information internally.

You can also enable sampling per query using the `jemalloc_enable_profiler` setting.

:::warning Warning
Because ClickHouse is an allocation-heavy application, jemalloc sampling may incur performance overhead.
:::

## Storing jemalloc samples in `system.trace_log` {#storing-jemalloc-samples-in-system-trace-log}

You can store jemalloc samples in `system.trace_log` under the `JemallocSample` type.
To enable it globally, use the `jemalloc_collect_global_profile_samples_in_trace_log` config:

```xml
<clickhouse>
    <jemalloc_collect_global_profile_samples_in_trace_log>1</jemalloc_collect_global_profile_samples_in_trace_log>
</clickhouse>
```

:::warning Warning
Because ClickHouse is an allocation-heavy application, collecting all samples in system.trace_log may incur high load.
:::

You can also enable it per query using the `jemalloc_collect_profile_samples_in_trace_log` setting.

### Example: analyzing memory usage of a query {#example-analyzing-memory-usage-trace-log}

First, run a query with the jemalloc profiler enabled and collect the samples into `system.trace_log`:

```sql
SELECT *
FROM numbers(1000000)
ORDER BY number DESC
SETTINGS max_bytes_ratio_before_external_sort = 0
FORMAT `Null`
SETTINGS jemalloc_enable_profiler = 1, jemalloc_collect_profile_samples_in_trace_log = 1

Query id: 8678d8fe-62c5-48b8-b0cd-26851c62dd75

Ok.

0 rows in set. Elapsed: 0.009 sec. Processed 1.00 million rows, 8.00 MB (108.58 million rows/s., 868.61 MB/s.)
Peak memory usage: 12.65 MiB.
```

:::note
If ClickHouse was started with `jemalloc_enable_global_profiler`, you don't have to enable `jemalloc_enable_profiler`.
Same is true for `jemalloc_collect_global_profile_samples_in_trace_log` and `jemalloc_collect_profile_samples_in_trace_log`.
:::

Flush the `system.trace_log`:

```sql
SYSTEM FLUSH LOGS trace_log
```

Then query it to get cumulative memory usage over time:

```sql
WITH per_bucket AS
(
    SELECT
        event_time_microseconds AS bucket_time,
        sum(size) AS bucket_sum
    FROM system.trace_log
    WHERE trace_type = 'JemallocSample'
      AND query_id = '8678d8fe-62c5-48b8-b0cd-26851c62dd75'
    GROUP BY bucket_time
)
SELECT
    bucket_time,
    sum(bucket_sum) OVER (
        ORDER BY bucket_time ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_size,
    formatReadableSize(cumulative_size) AS cumulative_size_readable
FROM per_bucket
ORDER BY bucket_time
```

Find the time where memory usage was the highest:

```sql
SELECT
    argMax(bucket_time, cumulative_size),
    max(cumulative_size)
FROM
(
    WITH per_bucket AS
    (
        SELECT
            event_time_microseconds AS bucket_time,
            sum(size) AS bucket_sum
        FROM system.trace_log
        WHERE trace_type = 'JemallocSample'
          AND query_id = '8678d8fe-62c5-48b8-b0cd-26851c62dd75'
        GROUP BY bucket_time
    )
    SELECT
        bucket_time,
        sum(bucket_sum) OVER (
            ORDER BY bucket_time ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_size,
        formatReadableSize(cumulative_size) AS cumulative_size_readable
    FROM per_bucket
    ORDER BY bucket_time
)
```

Using that result, see which allocation stacks were most active at the peak:

```sql
SELECT
    concat(
        '\n',
        arrayStringConcat(
            arrayMap(
                (x, y) -> concat(x, ': ', y),
                arrayMap(x -> addressToLine(x), allocation_trace),
                arrayMap(x -> demangle(addressToSymbol(x)), allocation_trace)
            ),
            '\n'
        )
    ) AS symbolized_trace,
    sum(s) AS per_trace_sum
FROM
(
    SELECT
        ptr,
        sum(size) AS s,
        argMax(trace, event_time_microseconds) AS allocation_trace
    FROM system.trace_log
    WHERE trace_type = 'JemallocSample'
      AND query_id = '8678d8fe-62c5-48b8-b0cd-26851c62dd75'
      AND event_time_microseconds <= '2025-09-04 11:56:21.737139'
    GROUP BY ptr
    HAVING s > 0
)
GROUP BY ALL
ORDER BY per_trace_sum ASC
```

## Jemalloc web UI {#jemalloc-web-ui}

:::note
This section is applicable for versions 26.2+.
:::

ClickHouse provides a built-in web UI for viewing jemalloc memory statistics at the `/jemalloc` HTTP endpoint.
It displays live memory metrics with charts, including allocated, active, resident, and mapped memory, as well as per-arena and per-bin statistics.
You can also fetch global and per-query heap profiles directly from the UI.

To access it, open in your browser:

```text
http://localhost:8123/jemalloc
```

## Fetching heap profiles from SQL {#fetching-heap-profiles-from-sql}

:::note
This section is applicable for versions 26.2+.
:::

The `system.jemalloc_profile_text` system table lets you fetch and view the current jemalloc heap profile directly from SQL, without needing external tools or flushing to disk first.

The table has a single column:

| Column | Type   | Description                                      |
|--------|--------|--------------------------------------------------|
| `line` | String | Line from the symbolized jemalloc heap profile.  |

You can query the table directly — there is no need to flush a heap profile beforehand:

```sql
SELECT * FROM system.jemalloc_profile_text
```

### Output format {#output-format}

The output format is controlled by the `jemalloc_profile_text_output_format` setting, which supports three values:

- `raw` — raw heap profile as produced by jemalloc.
- `symbolized` — jeprof-compatible format with embedded function symbols. Since symbols are already embedded, `jeprof` can analyze the output without requiring the ClickHouse binary.
- `collapsed` (default) — FlameGraph-compatible collapsed stacks, one stack per line with the byte count.

For example, to get the raw profile:

```sql
SELECT * FROM system.jemalloc_profile_text
SETTINGS jemalloc_profile_text_output_format = 'raw'
```

To get symbolized output:

```sql
SELECT * FROM system.jemalloc_profile_text
SETTINGS jemalloc_profile_text_output_format = 'symbolized'
```

### Additional settings {#fetching-heap-profiles-settings}

- `jemalloc_profile_text_symbolize_with_inline` (Bool, default: `true`) — Whether to include inline frames when symbolizing. Disabling this speeds up symbolization significantly but loses precision as inlined function calls will not appear in the stacks. Only affects `symbolized` and `collapsed` formats.
- `jemalloc_profile_text_collapsed_use_count` (Bool, default: `false`) — When using the `collapsed` format, aggregate by allocation count instead of bytes.

### Example: generating a flame graph from SQL {#example-flamegraph-from-sql}

Since the default output format is `collapsed`, you can pipe the output directly to FlameGraph:

```sh
clickhouse-client -q "SELECT * FROM system.jemalloc_profile_text" | flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

To generate a flame graph by allocation count instead of bytes:

```sh
clickhouse-client -q "SELECT * FROM system.jemalloc_profile_text SETTINGS jemalloc_profile_text_collapsed_use_count = 1" | flamegraph.pl --color=mem --title="Allocation Count Flame Graph" --width 2400 > result.svg
```

## Flushing heap profiles to disk {#flushing-heap-profiles}

If you need to save heap profiles as files for offline analysis with `jeprof`, you can flush them to disk.

By default, the heap profile file will be generated in `/tmp/jemalloc_clickhouse._pid_._seqnum_.heap` where `_pid_` is the PID of ClickHouse and `_seqnum_` is the global sequence number for the current heap profile.
For Keeper, the default file is `/tmp/jemalloc_keeper._pid_._seqnum_.heap`, and follows the same rules.

To flush the current profile:

<Tabs groupId="binary">
<TabItem value="clickhouse" label="ClickHouse">

```sql
SYSTEM JEMALLOC FLUSH PROFILE
```

It will return the location of the flushed profile.

</TabItem>
<TabItem value="keeper" label="Keeper">

```sh
echo jmfp | nc localhost 9181
```

</TabItem>
</Tabs>

A different location can be defined by appending the `MALLOC_CONF` environment variable with the `prof_prefix` option.
For example, if you want to generate profiles in the `/data` folder where the filename prefix will be `my_current_profile`, you can run ClickHouse/Keeper with the following environment variable:

```sh
MALLOC_CONF=prof_prefix:/data/my_current_profile
```

The generated file will be appended to the prefix PID and sequence number.

## Analyzing heap profile files with `jeprof` {#analyzing-heap-profile-files-with-jeprof}

After flushing heap profiles to disk, they can be analyzed using `jemalloc`'s tool called [jeprof](https://github.com/jemalloc/jemalloc/blob/dev/bin/jeprof.in). It can be installed in multiple ways:
- Using the system's package manager
- Cloning the [jemalloc repo](https://github.com/jemalloc/jemalloc) and running `autogen.sh` from the root folder. This will provide you with the `jeprof` script inside the `bin` folder

There are many different output formats available. Run `jeprof --help` for the full list of options.

### Symbolized heap profiles {#symbolized-heap-profiles}

Starting from version 26.1+, ClickHouse automatically generates symbolized heap profiles when you flush using `SYSTEM JEMALLOC FLUSH PROFILE`.
The symbolized profile (with `.symbolized` extension) contains embedded function symbols and can be analyzed by `jeprof` without requiring the ClickHouse binary.

For example, when you run:

```sql
SYSTEM JEMALLOC FLUSH PROFILE
```

ClickHouse will return the path to the symbolized profile (e.g., `/tmp/jemalloc_clickhouse.12345.0.heap.symbolized`).

You can then analyze it directly with `jeprof`:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --output_format [ > output_file]
```

:::note

**No binary required**: When using symbolized profiles (`.symbolized` files), you don't need to provide the ClickHouse binary path to `jeprof`. This makes it much easier to analyze profiles on different machines or after the binary has been updated.

:::

If you have an older non-symbolized heap profile and still have access to the ClickHouse binary, you can use the traditional approach:

```sh
jeprof path/to/clickhouse path/to/heap/profile --output_format [ > output_file]
```

:::note

For non-symbolized profiles, `jeprof` uses `addr2line` to generate stacktraces which can be really slow.
If that's the case, it is recommended to install an [alternative implementation](https://github.com/gimli-rs/addr2line) of the tool.

```bash
git clone https://github.com/gimli-rs/addr2line.git --depth=1 --branch=0.23.0
cd addr2line
cargo build --features bin --release
cp ./target/release/addr2line path/to/current/addr2line
```

Alternatively, `llvm-addr2line` works equally well (But note, that `llvm-objdump` is not compatible with `jeprof`)

And later use it like this `jeprof --tools addr2line:/usr/bin/llvm-addr2line,nm:/usr/bin/llvm-nm,objdump:/usr/bin/objdump,c++filt:/usr/bin/llvm-cxxfilt`

:::

When comparing two profiles, you can use the `--base` argument:

```sh
jeprof --base /path/to/first.heap.symbolized /path/to/second.heap.symbolized --output_format [ > output_file]
```

### Examples {#examples}

Using symbolized profiles (recommended):

- Generate a text file with each procedure written per line:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --text > result.txt
```

- Generate a PDF file with a call-graph:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --pdf > result.pdf
```

Using non-symbolized profiles (requires binary):

- Generate a text file with each procedure written per line:

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --text > result.txt
```

- Generate a PDF file with a call-graph:

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --pdf > result.pdf
```

### Generating a flame graph {#generating-flame-graph}

`jeprof` allows you to generate collapsed stacks for building flame graphs.

You need to use the `--collapsed` argument:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --collapsed > result.collapsed
```

Or with a non-symbolized profile:

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --collapsed > result.collapsed
```

After that, you can use many different tools to visualize collapsed stacks.

The most popular is [FlameGraph](https://github.com/brendangregg/FlameGraph) which contains a script called `flamegraph.pl`:

```sh
cat result.collapsed | /path/to/FlameGraph/flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

Another interesting tool is [speedscope](https://www.speedscope.app/) that allows you to analyze collected stacks in a more interactive way.

## Additional options for the profiler {#additional-options-for-profiler}

`jemalloc` has many different options available, which are related to the profiler. They can be controlled by modifying the `MALLOC_CONF` environment variable.
For example, the interval between allocation samples can be controlled with `lg_prof_sample`.
If you want to dump the heap profile every N bytes you can enable it using `lg_prof_interval`.

It is recommended to check `jemalloc`s [reference page](https://jemalloc.net/jemalloc.3.html) for a complete list of options.

## Other resources {#other-resources}

ClickHouse/Keeper expose `jemalloc` related metrics in many different ways.

:::warning Warning
It's important to be aware that none of these metrics are synchronized with each other and values may drift.
:::

### System table `asynchronous_metrics` {#system-table-asynchronous_metrics}

```sql
SELECT *
FROM system.asynchronous_metrics
WHERE metric LIKE '%jemalloc%'
FORMAT Vertical
```

[Reference](/operations/system-tables/asynchronous_metrics)

### System table `jemalloc_bins` {#system-table-jemalloc_bins}

Contains information about memory allocations done via the jemalloc allocator in different size classes (bins) aggregated from all arenas.

[Reference](/operations/system-tables/jemalloc_bins)

### System table `jemalloc_stats` (26.2+) {#system-table-jemalloc_stats}

Returns the full output of `malloc_stats_print()` as a single string. Equivalent to the `SYSTEM JEMALLOC STATS` command.

```sql
SELECT * FROM system.jemalloc_stats
```

### Prometheus {#prometheus}

All `jemalloc` related metrics from `asynchronous_metrics` are also exposed using the Prometheus endpoint in both ClickHouse and Keeper.

[Reference](/operations/server-configuration-parameters/settings#prometheus)

### `jmst` 4LW command in Keeper {#jmst-4lw-command-in-keeper}

Keeper supports the `jmst` 4LW command which returns [basic allocator statistics](https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Basic-Allocator-Statistics):

```sh
echo jmst | nc localhost 9181
```
