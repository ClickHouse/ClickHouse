---
description: 'Page detailing allocation profiling in ClickHouse'
sidebar_label: 'Allocation profiling for versions before 25.9'
slug: /operations/allocation-profiling-old
title: 'Allocation profiling for versions before 25.9'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Allocation profiling for versions before 25.9

ClickHouse uses [jemalloc](https://github.com/jemalloc/jemalloc) as its global allocator. Jemalloc comes with some tools for allocation sampling and profiling.  
To make allocation profiling more convenient, `SYSTEM` commands are provided along with four letter word (4LW) commands in Keeper.

## Sampling allocations and flushing heap profiles {#sampling-allocations-and-flushing-heap-profiles}

If you want to sample and profile allocations in `jemalloc`, you need to start ClickHouse/Keeper with profiling enabled using the environment variable `MALLOC_CONF`:

```sh
MALLOC_CONF=background_thread:true,prof:true
```

`jemalloc` will sample allocations and store the information internally.

You can tell `jemalloc` to flush the current profile by running:

<Tabs groupId="binary">
<TabItem value="clickhouse" label="ClickHouse">
    
```sql
SYSTEM JEMALLOC FLUSH PROFILE
```

</TabItem>
<TabItem value="keeper" label="Keeper">
    
```sh
echo jmfp | nc localhost 9181
```

</TabItem>
</Tabs>

By default, the heap profile file will be generated in `/tmp/jemalloc_clickhouse._pid_._seqnum_.heap` where `_pid_` is the PID of ClickHouse and `_seqnum_` is the global sequence number for the current heap profile.  
For Keeper, the default file is `/tmp/jemalloc_keeper._pid_._seqnum_.heap`, and follows the same rules.

A different location can be defined by appending the `MALLOC_CONF` environment variable with the `prof_prefix` option.  
For example, if you want to generate profiles in the `/data` folder where the filename prefix will be `my_current_profile`, you can run ClickHouse/Keeper with the following environment variable:

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_prefix:/data/my_current_profile
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

## Controlling allocation profiler during runtime {#controlling-allocation-profiler-during-runtime}

If ClickHouse/Keeper is started with the profiler enabled, additional commands for disabling/enabling allocation profiling during runtime are supported.
Using those commands, it's easier to profile only specific intervals.

To disable the profiler:

<Tabs groupId="binary">
<TabItem value="clickhouse" label="ClickHouse">

```sql
SYSTEM JEMALLOC DISABLE PROFILE
```

</TabItem>
<TabItem value="keeper" label="Keeper">

```sh
echo jmdp | nc localhost 9181
```

</TabItem>
</Tabs>

To enable the profiler:

<Tabs groupId="binary">
<TabItem value="clickhouse" label="ClickHouse">

```sql
SYSTEM JEMALLOC ENABLE PROFILE
```

</TabItem>
<TabItem value="keeper" label="Keeper">

```sh
echo jmep | nc localhost 9181
```

</TabItem>
</Tabs>

It's also possible to control the initial state of the profiler by setting the `prof_active` option which is enabled by default.  
For example, if you don't want to sample allocations during startup but only after, you can enable the profiler. You can start ClickHouse/Keeper with the following environment variable:

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_active:false
```

The profiler can be enabled later.

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
