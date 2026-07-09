---
description: '介绍 ClickHouse 中内存分配分析的页面'
sidebar_label: '25.9 之前版本的内存分配分析'
slug: /operations/allocation-profiling-old
title: '25.9 之前版本的内存分配分析'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="allocation-profiling-for-versions-before-259">
  # 25.9 之前版本的内存分配分析
</div>

ClickHouse 使用 [jemalloc](https://github.com/jemalloc/jemalloc) 作为全局内存分配器。Jemalloc 自带了一些用于内存分配采样和分析的工具。
为方便进行内存分配分析，除了 Keeper 中的四字母词 (4LW) 命令外，还提供了 `SYSTEM` 命令。

<div id="sampling-allocations-and-flushing-heap-profiles">
  ## 对内存分配进行采样并刷新堆内存剖析
</div>

如果要在 `jemalloc` 中对内存分配进行采样和剖析，需要通过环境变量 &#96;MALLOC&#95;CONF&#96;&#96; 启动并启用剖析功能的 ClickHouse/Keeper：

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_active:true
```

`jemalloc` 会对内存分配进行采样，并在内部存储相关信息。

你可以运行以下命令，让 `jemalloc` 刷新当前的 profile：

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

默认情况下，堆内存剖析文件会生成在 `/tmp/jemalloc_clickhouse._pid_._seqnum_.heap`，其中 `_pid_` 是 ClickHouse 的 PID，`_seqnum_` 是当前堆内存profile的全局序列号。
对于 Keeper，默认文件为 `/tmp/jemalloc_keeper._pid_._seqnum_.heap`，规则相同。

你也可以通过在 `MALLOC_CONF` 环境变量中追加 `prof_prefix` 选项来指定其他位置。
例如，如果你想在 `/data` 文件夹中生成 profile，并将文件名前缀设为 `my_current_profile`，可以使用以下环境变量运行 ClickHouse/Keeper：

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_prefix:/data/my_current_profile
```

生成的文件名会附加前缀 PID 和序列号。

<div id="analyzing-heap-profiles">
  ## 分析堆内存剖析
</div>

生成堆内存剖析后，需要对其进行分析。
为此，可以使用 `jemalloc` 提供的工具 [jeprof](https://github.com/jemalloc/jemalloc/blob/dev/bin/jeprof.in)。它可以通过多种方式安装：

* 使用系统的包管理器
* 克隆 [jemalloc repo](https://github.com/jemalloc/jemalloc)，并在根目录运行 `autogen.sh`。这样会在 `bin` 目录中生成 `jeprof` 脚本

:::note
`jeprof` 使用 `addr2line` 生成 stacktraces，这个过程可能会非常慢。
如果确实如此，建议安装该工具的[替代实现](https://github.com/gimli-rs/addr2line)。

```bash
git clone https://github.com/gimli-rs/addr2line.git --depth=1 --branch=0.23.0
cd addr2line
cargo build --features bin --release
cp ./target/release/addr2line path/to/current/addr2line
```

:::

可以使用 `jeprof` 从堆内存剖析生成多种不同格式的输出。
建议运行 `jeprof --help`，了解其用法以及该工具提供的各种选项。

通常，`jeprof` 命令的用法如下：

```sh
jeprof path/to/binary path/to/heap/profile --output_format [ > output_file]
```

如果你想比较两个 profile 之间发生了哪些内存分配，可以设置 `base` 参数：

```sh
jeprof path/to/binary --base path/to/first/heap/profile path/to/second/heap/profile --output_format [ > output_file]
```

<div id="examples">
  ### 示例
</div>

* 如果您想生成一个文本文件，并将每个过程分别写在单独一行中：

```sh
jeprof path/to/binary path/to/heap/profile --text > result.txt
```

* 如果你想生成包含调用图的 PDF 文件：

```sh
jeprof path/to/binary path/to/heap/profile --pdf > result.pdf
```

<div id="generating-flame-graph">
  ### 生成火焰图
</div>

`jeprof` 可生成可用于构建火焰图的折叠栈。

你需要使用 `--collapsed` 参数：

```sh
jeprof path/to/binary path/to/heap/profile --collapsed > result.collapsed
```

之后，你可以使用多种不同的工具来可视化 collapsed stacks。

其中最常用的是 [FlameGraph](https://github.com/brendangregg/FlameGraph)，它包含一个名为 `flamegraph.pl` 的脚本：

```sh
cat result.collapsed | /path/to/FlameGraph/flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

另一个值得关注的工具是 [speedscope](https://www.speedscope.app/)，它可以让你以更直观的交互方式分析收集到的调用栈。

<div id="controlling-allocation-profiler-during-runtime">
  ## 在运行时控制剖析器
</div>

如果 ClickHouse/Keeper 在启用剖析器的情况下启动，则支持在运行时禁用/启用内存分配分析的额外命令。
使用这些命令后，就能更方便地仅分析特定时间段。

要禁用剖析器：

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

要启用剖析器：

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

还可以通过设置 `prof_active` 选项来控制剖析器的初始状态，该选项默认处于启用状态。
例如，如果你不希望在启动期间对内存分配进行采样，而只想在启动完成后再采样，就可以在之后启用剖析器。你可以使用以下环境变量启动 ClickHouse/Keeper：

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_active:false
```

稍后可再启用剖析器。

<div id="additional-options-for-profiler">
  ## 剖析器的其他选项
</div>

`jemalloc` 提供了许多与剖析器相关的选项，可通过修改 `MALLOC_CONF` 环境变量进行控制。
例如，可使用 `lg_prof_sample` 控制分配采样之间的时间间隔。
如果你希望每分配 N 字节就转储一次堆内存profile数据，可以通过 `lg_prof_interval` 启用。

建议查阅 `jemalloc` 的[参考页面](https://jemalloc.net/jemalloc.3.html)，获取完整的选项列表。

<div id="other-resources">
  ## 其他资源
</div>

ClickHouse/Keeper 以多种不同方式暴露与 `jemalloc` 相关的指标。

:::warning 警告
请务必注意，这些指标彼此之间并未同步，其值可能会逐渐偏离。
:::

<div id="system-table-asynchronous_metrics">
  ### 系统表 `asynchronous_metrics`
</div>

```sql
SELECT *
FROM system.asynchronous_metrics
WHERE metric LIKE '%jemalloc%'
FORMAT Vertical
```

[参考](/zh/operations/system-tables/asynchronous_metrics)

<div id="system-table-jemalloc_bins">
  ### 系统表 `jemalloc_bins`
</div>

汇总了所有 arena 中通过 jemalloc 分配器在不同大小类 (bins) 上的内存分配信息。

[参考](/zh/operations/system-tables/jemalloc_bins)

<div id="prometheus">
  ### Prometheus
</div>

`asynchronous_metrics` 中所有与 `jemalloc` 相关的指标，也都会通过 ClickHouse 和 Keeper 中的 Prometheus 端点对外暴露。

[参考](/zh/operations/server-configuration-parameters/settings#prometheus)

<div id="jmst-4lw-command-in-keeper">
  ### Keeper 中的 `jmst` 4LW 命令
</div>

Keeper 支持 `jmst` 4LW 命令，该命令会返回[基本分配器统计信息](https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Basic-Allocator-Statistics)：

```sh
echo jmst | nc localhost 9181
```