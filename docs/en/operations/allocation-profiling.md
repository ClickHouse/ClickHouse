---
description: 'Page detailing allocation profiling in ClickHouse'
sidebar_label: 'Allocation profiling'
slug: /operations/allocation-profiling
title: 'Allocation profiling'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Allocation profiling

ClickHouse uses [jemalloc](https://github.com/jemalloc/jemalloc) as its global allocator. Jemalloc comes with some tools for allocation sampling and profiling.  
To make allocation profiling more convenient, ClickHouse and Keeper allow you to control sampling using configs, query settings,`SYSTEM` commands and four letter word (4LW) commands in Keeper.   
Additionally, samples can be collected into `system.trace_log` table under `JemallocSample` type.

:::note

This guide is applicable for versions 25.9+.
For older versions, please check [allocation profiling for versions before 25.9](/operations/allocation-profiling-old.md).

:::

## Sampling allocations {#sampling-allocations}

If you want to sample and profile allocations in `jemalloc`, you need to start ClickHouse/Keeper with config `jemalloc_enable_global_profiler` enabled.

```xml
<clickhouse>
    <jemalloc_enable_global_profiler>1</jemalloc_enable_global_profiler>
</clickhouse>
```

`jemalloc` will sample allocations and store the information internally.

You can also enable allocations per query by using `jemalloc_enable_profiler` setting.

:::warning Warning
Because ClickHouse is an allocation-heavy application, jemalloc sampling may incur performance overhead.
:::

## Storing jemalloc samples in `system.trace_log` {#storing-jemalloc-samples-in-system-trace-log}

You can store all the jemalloc samples in `system.trace_log` under `JemallocSample` type.
To enable it globally you can use config `jemalloc_collect_global_profile_samples_in_trace_log`.

```xml
<clickhouse>
    <jemalloc_collect_global_profile_samples_in_trace_log>1</jemalloc_collect_global_profile_samples_in_trace_log>
</clickhouse>
```

:::warning Warning
Because ClickHouse is an allocation-heavy application, collecting all samples in system.trace_log may incur high load.
:::

You can also enable it per query by using `jemalloc_collect_profile_samples_in_trace_log` setting.

### Example of analyzing memory usage of a query using `system.trace_log` {#example-analyzing-memory-usage-trace-log}

First, we need to run the query with enabled jemalloc profiler and collect the samples for it into `system.trace_log`:

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

We will flush the `system.trace_log`:

```sql
SYSTEM FLUSH LOGS trace_log
```
and query it to get memory usage of the query we run for each time point:
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

We can also find the time where the memory usage was the highest:

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

We can use that result to see from where did we have the most active allocations at that time point:

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

## Flushing heap profiles {#flushing-heap-profiles}

By default, the heap profile file will be generated in `/tmp/jemalloc_clickhouse._pid_._seqnum_.heap` where `_pid_` is the PID of ClickHouse and `_seqnum_` is the global sequence number for the current heap profile.  
For Keeper, the default file is `/tmp/jemalloc_keeper._pid_._seqnum_.heap`, and follows the same rules.

You can tell `jemalloc` to flush the current profile by running:

<Tabs groupId="binary">
<TabItem value="clickhouse" label="ClickHouse">
    
```sql
SYSTEM JEMALLOC FLUSH PROFILE
```

It will return the location of the flushed profile.

You can also specify a different prefix for the file in the query:
```sql
SYSTEM JEMALLOC FLUSH PROFILE TO '/tmp/my_own_prefix'
```

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

## Analyzing heap profiles {#analyzing-heap-profiles}

After heap profiles have been generated, they need to be analyzed.  
For that, `jemalloc`'s tool called [jeprof](https://github.com/jemalloc/jemalloc/blob/dev/bin/jeprof.in) can be used. It can be installed in multiple ways:
- Using the system's package manager
- Cloning the [jemalloc repo](https://github.com/jemalloc/jemalloc) and running `autogen.sh` from the root folder. This will provide you with the `jeprof` script inside the `bin` folder

:::note
`jeprof` uses `addr2line` to generate stacktraces which can be really slow.  
If that's the case, it is recommended to install an [alternative implementation](https://github.com/gimli-rs/addr2line) of the tool.   

```bash
git clone https://github.com/gimli-rs/addr2line.git --depth=1 --branch=0.23.0
cd addr2line
cargo build --features bin --release
cp ./target/release/addr2line path/to/current/addr2line
```

Alternatively, `llvm-addr2line` works equally well.

:::

There are many different formats to generate from the heap profile using `jeprof`.
It is recommended to run `jeprof --help` for information on the usage and the various options the tool provides. 

In general, the `jeprof` command is used as:

```sh
jeprof path/to/binary path/to/heap/profile --output_format [ > output_file]
```

If you want to compare which allocations happened between two profiles you can set the `base` argument:

```sh
jeprof path/to/binary --base path/to/first/heap/profile path/to/second/heap/profile --output_format [ > output_file]
```

### Examples {#examples}

- if you want to generate a text file with each procedure written per line:

```sh
jeprof path/to/binary path/to/heap/profile --text > result.txt
```

- if you want to generate a PDF file with a call-graph:

```sh
jeprof path/to/binary path/to/heap/profile --pdf > result.pdf
```

### Generating a flame graph {#generating-flame-graph}

`jeprof` allows you to generate collapsed stacks for building flame graphs.

You need to use the `--collapsed` argument:

```sh
jeprof path/to/binary path/to/heap/profile --collapsed > result.collapsed
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

### Prometheus {#prometheus}

All `jemalloc` related metrics from `asynchronous_metrics` are also exposed using the Prometheus endpoint in both ClickHouse and Keeper.

[Reference](/operations/server-configuration-parameters/settings#prometheus)

### `jmst` 4LW command in Keeper {#jmst-4lw-command-in-keeper}

Keeper supports the `jmst` 4LW command which returns [basic allocator statistics](https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Basic-Allocator-Statistics):

```sh
echo jmst | nc localhost 9181
```
