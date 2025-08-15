---
slug: /en/operations/allocation-profiling
sidebar_label: "Allocation profiling"
title: "Allocation profiling"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Allocation profiling

ClickHouse uses [jemalloc](https://github.com/jemalloc/jemalloc) as its global allocator that comes with some tools for allocation sampling and profiling.  
To make allocation profiling more convenient, `SYSTEM` commands are provided along 4LW commands in Keeper.

## Sampling allocations and flushing heap profiles

If we want to sample and profile allocations in `jemalloc`, we need to start ClickHouse/Keeper with profiling enabled using environment variable `MALLOC_CONF`.

```sh
MALLOC_CONF=background_thread:true,prof:true
```

`jemalloc` will sample allocation and store the information internally.

We can tell `jemalloc` to flush current profile by running:

<Tabs groupId="binary">
<TabItem value="clickhouse" label="ClickHouse">

    SYSTEM JEMALLOC FLUSH PROFILE

</TabItem>
<TabItem value="keeper" label="Keeper">

    echo jmfp | nc localhost 9181

</TabItem>
</Tabs>

By default, heap profile file will be generated in `/tmp/jemalloc_clickhouse._pid_._seqnum_.heap` where `_pid_` is the PID of ClickHouse and `_seqnum_` is the global sequence number for the current heap profile.  
For Keeper, the default file is `/tmp/jemalloc_keeper._pid_._seqnum_.heap` following the same rules.

A different location can be defined by appending the `MALLOC_CONF` environment variable with `prof_prefix` option.  
For example, if we want to generate profiles in `/data` folder where the prefix for filename will be `my_current_profile` we can run ClickHouse/Keeper with following environment variable:
```sh
MALLOC_CONF=background_thread:true,prof:true,prof_prefix:/data/my_current_profile
```
Generated file will append to prefix PID and sequence number.

## Analyzing heap profiles

After we generated heap profiles, we need to analyze them.  
For that, we need to use `jemalloc`'s tool called [jeprof](https://github.com/jemalloc/jemalloc/blob/dev/bin/jeprof.in) which can be installed in multiple ways:
- installing `jemalloc` using system's package manager
- cloning [jemalloc repo](https://github.com/jemalloc/jemalloc) and running autogen.sh from the root folder that will provide you with `jeprof` script inside the `bin` folder

:::note
`jeprof` uses `addr2line` to generate stacktraces which can be really slow.  
If that’s the case, we recommend installing an [alternative implementation](https://github.com/gimli-rs/addr2line) of the tool.

```
git clone https://github.com/gimli-rs/addr2line.git --depth=1 --branch=0.23.0
cd addr2line
cargo build --features bin --release
cp ./target/release/addr2line path/to/current/addr2line
```
:::

There are many different formats to generate from the heap profile using `jeprof`.
We recommend to run `jeprof --help` to check usage and many different options the tool provides. 

In general, `jeprof` command will look like this:

```sh
jeprof path/to/binary path/to/heap/profile --output_format [ > output_file]
```

If we want to compare which allocations happened between 2 profiles we can set the base argument:

```sh
jeprof path/to/binary --base path/to/first/heap/profile path/to/second/heap/profile --output_format [ > output_file]
```

For example:

- if we want to generate a text file with each procedure written per line:

```sh
jeprof path/to/binary path/to/heap/profile --text > result.txt
```

- if we want to generate a PDF file with call-graph:

```sh
jeprof path/to/binary path/to/heap/profile --pdf > result.pdf
```

### Generating flame graph

`jeprof` allows us to generate collapsed stacks for building flame graphs.

We need to use `--collapsed` argument:

```sh
jeprof path/to/binary path/to/heap/profile --collapsed > result.collapsed
```

After that, we can use many different tools to visualize collapsed stacks.

Most popular would be [FlameGraph](https://github.com/brendangregg/FlameGraph) which contains a script called `flamegraph.pl`:

```sh
cat result.collapsed | /path/to/FlameGraph/flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

Another interesting tool is [speedscope](https://www.speedscope.app/) that allows you to analyze collected stacks in a more interactive way.

## Controlling allocation profiler during runtime

If ClickHouse/Keeper were started with enabled profiler, they support additional commands for disabling/enabling allocation profiling during runtime.
Using those commands, it's easier to profile only specific intervals.

Disable profiler:

<Tabs groupId="binary">
<TabItem value="clickhouse" label="ClickHouse">

    SYSTEM JEMALLOC DISABLE PROFILE

</TabItem>
<TabItem value="keeper" label="Keeper">

    echo jmdp | nc localhost 9181

</TabItem>
</Tabs>

Enable profiler:

<Tabs groupId="binary">
<TabItem value="clickhouse" label="ClickHouse">

    SYSTEM JEMALLOC ENABLE PROFILE

</TabItem>
<TabItem value="keeper" label="Keeper">

    echo jmep | nc localhost 9181

</TabItem>
</Tabs>

It's also possible to control the initial state of the profiler by setting `prof_active` option which is enabled by default.  
For example, if we don't want to sample allocations during startup but only after we enable the profiler, we can start ClickHouse/Keeper with following environment variable:
```sh
MALLOC_CONF=background_thread:true,prof:true,prof_active:false
```

and enable profiler at a later point.

## Additional options for profiler

`jemalloc` has many different options available related to profiler which can be controlled by modifying `MALLOC_CONF` environment variable.
For example, interval between allocation samples can be controlled with `lg_prof_sample`.  
If you want to dump heap profile every N bytes you can enable it using `lg_prof_interval`.  

We recommend to check `jemalloc`s [reference page](https://jemalloc.net/jemalloc.3.html) for such options.

## Other resources

ClickHouse/Keeper expose `jemalloc` related metrics in many different ways.

:::warning Warning
It's important to be aware that none of these metrics are synchronized with each other and values may drift.
:::

### System table `asynchronous_metrics`

```sql
SELECT *
FROM system.asynchronous_metrics
WHERE metric ILIKE '%jemalloc%'
FORMAT Vertical
```

[Reference](/en/operations/system-tables/asynchronous_metrics)

### System table `jemalloc_bins`

Contains information about memory allocations done via jemalloc allocator in different size classes (bins) aggregated from all arenas.

[Reference](/en/operations/system-tables/jemalloc_bins)

### Prometheus

All `jemalloc` related metrics from `asynchronous_metrics` are also exposed using Prometheus endpoint in both ClickHouse and Keeper.

[Reference](/en/operations/server-configuration-parameters/settings#prometheus)

### `jmst` 4LW command in Keeper

Keeper supports `jmst` 4LW command which returns [basic allocator statistics](https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Basic-Allocator-Statistics).

Example:
```sh
echo jmst | nc localhost 9181
```
