---
description: '关于 ClickHouse 中内存分配剖析的页面'
sidebar_label: '内存分配剖析'
slug: /operations/allocation-profiling
title: '内存分配剖析'
doc_type: 'guide'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="allocation-profiling">
  # 内存分配剖析
</div>

ClickHouse 使用 [jemalloc](https://github.com/jemalloc/jemalloc) 作为全局分配器。jemalloc 自带内存分配采样和剖析工具。

ClickHouse 和 Keeper 支持通过配置、查询设置、`SYSTEM` 命令以及 Keeper 中的 four letter word (4LW) 命令来控制采样。查看结果的方式有多种：

* 将样本收集到 `system.trace_log` 中，类型为 `JemallocSample`，以便进行逐查询分析。
* 通过内置的 [jemalloc web UI](#jemalloc-web-ui) 查看实时内存统计信息，并拉取堆内存剖析 (26.2+) 。
* 使用 [`system.jemalloc_profile_text`](#fetching-heap-profiles-from-sql) 直接通过 SQL 查询当前堆内存剖析 (26.2+) 。
* 将堆内存剖析落盘，并使用 [`jeprof`](#analyzing-heap-profile-files-with-jeprof) 进行分析。

:::note

本指南适用于 25.9+ 版本。
对于更早的版本，请参阅[适用于 25.9 之前版本的内存分配剖析](/zh/operations/allocation-profiling-old.md)。

:::

<div id="sampling-allocations">
  ## 对内存分配进行采样
</div>

要对内存分配进行采样并分析，请在启动 ClickHouse/Keeper 时启用 `jemalloc_enable_global_profiler` 配置：

```xml
<clickhouse>
    <jemalloc_enable_global_profiler>1</jemalloc_enable_global_profiler>
</clickhouse>
```

`jemalloc` 会对内存分配进行采样，并在内部存储这些信息。

你也可以使用 `jemalloc_enable_profiler` 设置按查询启用采样。

:::warning 警告
由于 ClickHouse 是一个内存分配密集型应用，jemalloc 采样可能会带来性能开销。
:::

<div id="storing-jemalloc-samples-in-system-trace-log">
  ## 在 `system.trace_log` 中存储 jemalloc 样本
</div>

你可以将 jemalloc 样本以 `JemallocSample` 类型存储到 `system.trace_log` 中。
要全局启用此功能，请使用 `jemalloc_collect_global_profile_samples_in_trace_log` 配置：

```xml
<clickhouse>
    <jemalloc_collect_global_profile_samples_in_trace_log>1</jemalloc_collect_global_profile_samples_in_trace_log>
</clickhouse>
```

:::warning 警告
由于 ClickHouse 是分配操作密集型应用，在 system.trace&#95;log 中收集所有样本可能会带来较高负载。
:::

你也可以通过 `jemalloc_collect_profile_samples_in_trace_log` 设置，针对单个查询启用此功能。

<div id="example-analyzing-memory-usage-trace-log">
  ### 示例：分析查询的内存使用量
</div>

首先，在启用 jemalloc profiler 的情况下运行查询，并将样本收集到 `system.trace_log` 中：

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
如果 ClickHouse 在启动时启用了 `jemalloc_enable_global_profiler`，则无需再启用 `jemalloc_enable_profiler`。
对于 `jemalloc_collect_global_profile_samples_in_trace_log` 和 `jemalloc_collect_profile_samples_in_trace_log` 也是如此。
:::

将 `system.trace_log` 刷写到存储：

```sql
SYSTEM FLUSH LOGS trace_log
```

然后查询它，获取随时间变化的累计内存使用量：

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

找出内存使用量最高时的时间：

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

根据该结果，查看在峰值时哪些分配调用栈最活跃：

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

<div id="jemalloc-web-ui">
  ## Jemalloc web UI
</div>

:::note
本节适用于 26.2+ 版本。
:::

ClickHouse 在 `/jemalloc` HTTP 端点提供了内置的 web UI，用于查看 jemalloc 内存统计信息。
它通过图表展示实时内存指标，包括 allocated、active、resident 和 mapped memory，以及按 arena 和 bin 划分的统计信息。
你还可以直接在该 UI 中拉取全局和按查询划分的堆内存剖析。

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```text
    http://localhost:8123/jemalloc
    ```

    服务器 UI 包含所有选项卡：摘要、Allocations、Arenas、操作、Global Profiler、Query Profiler 和 Raw Output。
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```text
    http://localhost:9182/jemalloc
    ```

    Keeper UI 可通过 HTTP 控制端口访问。该端口**默认处于禁用状态**，必须在 Keeper 配置中设置 `keeper_server.http_control.port` 才会显式启用：

    ```xml
    <clickhouse>
        <keeper_server>
            <http_control>
                <port>9182</port>
            </http_control>
        </keeper_server>
    </clickhouse>
    ```

    启用后，该 UI 提供与服务器相同的可视化内容——摘要、Allocations、Arenas、操作、Global Profiler 和 Raw Output——但不包含 Query Profiler 选项卡，因为它依赖 SQL 和 `system.trace_log`。

    :::warning 安全
    Keeper 的 HTTP 控制端口不提供应用层身份验证。与 ClickHouse Server 的 jemalloc UI 不同——后者所有数据查询都通过 SQL HTTP handler，并且需要用户名/密码凭据——Keeper 的 REST API 端点无需身份验证。这与其他 Keeper HTTP 控制端点 (commands、storage、dashboard) 一致。

    请使用网络层控制来限制对此端口的访问：将 Keeper 绑定到 localhost、使用 firewall 规则，或将其放在启用了身份验证的 reverse proxy 后面。未配置 `listen_host` 时，Keeper 默认仅监听 localhost。
    :::

    Keeper 还公开了 REST API 端点，便于程序化访问：

    * `GET /jemalloc/stats` — 原始 `malloc_stats_print` 输出
    * `GET /jemalloc/status` — 以 JSON 返回剖析状态 (`prof_enabled`、`prof_active`、`thread_active_init`、`lg_sample`)
    * `GET /jemalloc/profile?format={collapsed|raw}` — 刷写堆内存剖析并进行 server-side 符号化，返回适合 flame graph 渲染的 collapsed stacks (默认) 或原始 jemalloc 转储
  </TabItem>
</Tabs>

<div id="fetching-heap-profiles-from-sql">
  ## 从 SQL 获取堆内存剖析
</div>

:::note
本节适用于 26.2 及以上版本。
:::

`system.jemalloc_profile_text` 系统表可让你直接通过 SQL 获取并查看当前的 jemalloc 堆内存剖析，无需借助外部工具，也不用先将其刷写到磁盘。

该表只有一列：

| 列      | 类型     | 描述                        |
| ------ | ------ | ------------------------- |
| `line` | String | 符号化后的 jemalloc 堆内存剖析中的一行。 |

你可以直接查询该表——无需预先生成并刷写堆内存剖析：

```sql
SELECT * FROM system.jemalloc_profile_text
```

<div id="output-format">
  ### 输出格式
</div>

输出格式由 `jemalloc_profile_text_output_format` 设置控制，支持三个值：

* `raw` — jemalloc 生成的原始堆内存剖析。
* `symbolized` — 与 jeprof 兼容的格式，内嵌函数符号。由于符号已内嵌，`jeprof` 无需 ClickHouse 可执行文件即可分析输出。
* `collapsed` (默认) — 与 FlameGraph 兼容的折叠栈格式，每行一个调用栈，并附带字节数。

例如，要获取原始剖析结果：

```sql
SELECT * FROM system.jemalloc_profile_text
SETTINGS jemalloc_profile_text_output_format = 'raw'
```

要获取符号化输出：

```sql
SELECT * FROM system.jemalloc_profile_text
SETTINGS jemalloc_profile_text_output_format = 'symbolized'
```

<div id="fetching-heap-profiles-settings">
  ### 附加设置
</div>

* `jemalloc_profile_text_symbolize_with_inline` (Bool，默认：`true`) — 是否在符号解析时包含内联窗口帧。禁用此选项可显著提升符号解析速度，但会降低精度，因为内联函数调用将不会显示在调用栈中。仅影响 `symbolized` 和 `collapsed` 格式。
* `jemalloc_profile_text_collapsed_use_count` (Bool，默认：`false`) — 使用 `collapsed` 格式时，按分配次数而不是字节数聚合。

<div id="example-flamegraph-from-sql">
  ### 示例：从 SQL 生成火焰图
</div>

由于默认输出格式为 `collapsed`，你可以将输出直接通过管道传给 FlameGraph：

```sh
clickhouse-client -q "SELECT * FROM system.jemalloc_profile_text" | flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

要按分配次数而非字节数生成火焰图：

```sh
clickhouse-client -q "SELECT * FROM system.jemalloc_profile_text SETTINGS jemalloc_profile_text_collapsed_use_count = 1" | flamegraph.pl --color=mem --title="Allocation Count Flame Graph" --width 2400 > result.svg
```

<div id="flushing-heap-profiles">
  ## 将堆内存剖析刷写到磁盘
</div>

如果您需要将堆内存剖析保存为文件，以便使用 `jeprof` 进行离线分析，可以将其刷写到磁盘。

默认情况下，堆内存剖析文件会生成在 `/tmp/jemalloc_clickhouse._pid_._seqnum_.heap`，其中 `_pid_` 是 ClickHouse 的 PID，`_seqnum_` 是当前堆内存剖析的全局序列号。
对于 Keeper，默认文件为 `/tmp/jemalloc_keeper._pid_._seqnum_.heap`，规则相同。

要刷写当前剖析：

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC FLUSH PROFILE
    ```

    该命令会返回已刷写剖析文件的位置。
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmfp | nc localhost 9181
    ```
  </TabItem>
</Tabs>

您也可以通过在 `MALLOC_CONF` 环境变量中追加 `prof_prefix` 选项来指定其他位置。
例如，如果您希望在 `/data` 目录中生成剖析文件，并将文件名前缀设为 `my_current_profile`，可以使用以下环境变量运行 ClickHouse/Keeper：

```sh
MALLOC_CONF=prof_prefix:/data/my_current_profile
```

生成的文件名会在前缀后附加 PID 和序列号。

<div id="analyzing-heap-profile-files-with-jeprof">
  ## 使用 `jeprof` 分析堆内存剖析文件
</div>

将堆内存剖析刷写入磁盘后，可以使用 `jemalloc` 提供的工具 [jeprof](https://github.com/jemalloc/jemalloc/blob/dev/bin/jeprof.in) 进行分析。它可以通过多种方式安装：

* 使用系统的包管理器
* 克隆 [jemalloc 仓库](https://github.com/jemalloc/jemalloc)，并在根目录运行 `autogen.sh`。这样会在 `bin` 目录中生成 `jeprof` 脚本

可用的输出格式有很多种。运行 `jeprof --help` 可查看完整的选项列表。

<div id="symbolized-heap-profiles">
  ### 符号化的堆内存剖析
</div>

从 26.1+ 版本开始，当你使用 `SYSTEM JEMALLOC FLUSH PROFILE` 执行刷写操作时，ClickHouse 会自动生成符号化的堆内存剖析。
符号化的堆内存剖析 (带有 `.symbolized` 扩展名) 内嵌了函数符号，因此无需 ClickHouse 可执行文件即可用 `jeprof` 进行分析。

例如，当你运行：

```sql
SYSTEM JEMALLOC FLUSH PROFILE
```

ClickHouse 将返回符号化的堆内存剖析文件路径 (例如 `/tmp/jemalloc_clickhouse.12345.0.heap.symbolized`) 。

然后，您可以直接使用 `jeprof` 对其进行分析：

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --output_format [ > output_file]
```

:::note

**无需二进制文件**：使用符号化的堆内存剖析 (`.symbolized` 文件) 时，无需向 `jeprof` 提供 ClickHouse 可执行文件的路径。这使得在不同机器上，或在二进制文件更新后，分析剖析文件变得容易得多。

:::

如果你有较旧的、未符号化的堆内存剖析文件，并且仍可访问 ClickHouse 可执行文件，则可以使用传统方法：

```sh
jeprof path/to/clickhouse path/to/heap/profile --output_format [ > output_file]
```

:::note

对于未符号化的 profile，`jeprof` 会使用 `addr2line` 生成调用栈，这个过程可能非常慢。
如果遇到这种情况，建议安装该工具的[另一种实现](https://github.com/gimli-rs/addr2line)。

```bash
git clone https://github.com/gimli-rs/addr2line.git --depth=1 --branch=0.23.0
cd addr2line
cargo build --features bin --release
cp ./target/release/addr2line path/to/current/addr2line
```

或者，`llvm-addr2line` 也一样好用 (但请注意，`llvm-objdump` 与 `jeprof` 不兼容)

之后可像这样使用：`jeprof --tools addr2line:/usr/bin/llvm-addr2line,nm:/usr/bin/llvm-nm,objdump:/usr/bin/objdump,c++filt:/usr/bin/llvm-cxxfilt`

:::

比较两个 profile 时，可以使用 `--base` 参数：

```sh
jeprof --base /path/to/first.heap.symbolized /path/to/second.heap.symbolized --output_format [ > output_file]
```

<div id="examples">
  ### 示例
</div>

使用符号化的堆内存剖析 (推荐) ：

* 生成一个文本文件，每行写一个过程：

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --text > result.txt
```

* 生成带有调用图的 PDF 文件：

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --pdf > result.pdf
```

使用未符号化的 profile (需要二进制可执行文件) ：

* 生成一个文本文件，每行一个过程：

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --text > result.txt
```

* 生成包含调用图的 PDF 文件：

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --pdf > result.pdf
```

<div id="generating-flame-graph">
  ### 生成火焰图
</div>

`jeprof` 支持生成用于构建火焰图的折叠栈。

你需要使用 `--collapsed` 参数：

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --collapsed > result.collapsed
```

或者使用未符号化的堆内存剖析：

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --collapsed > result.collapsed
```

在此之后，你可以使用许多不同的工具来可视化折叠后的调用栈。

最常用的是 [FlameGraph](https://github.com/brendangregg/FlameGraph)，其中包含一个名为 `flamegraph.pl` 的脚本：

```sh
cat result.collapsed | /path/to/FlameGraph/flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

另一个有趣的工具是 [speedscope](https://www.speedscope.app/)，它可以让你以更交互式的方式分析收集到的调用栈。

<div id="additional-options-for-profiler">
  ## 剖析器的其他选项
</div>

`jemalloc` 提供了许多与剖析器相关的选项，可通过修改 `MALLOC_CONF` 环境变量进行控制。
例如，可以使用 `lg_prof_sample` 控制分配样本之间的时间间隔。
如果你想每分配 N 字节就转储一次堆内存剖析，可以通过 `lg_prof_interval` 启用该功能。

建议查阅 `jemalloc` 的[参考页面](https://jemalloc.net/jemalloc.3.html)，以获取完整的选项列表。

<div id="other-resources">
  ## 其他资源
</div>

ClickHouse/Keeper 会通过多种不同方式暴露与 `jemalloc` 相关的指标。

:::warning 警告
需要注意的是，这些指标彼此之间并未同步，数值可能会有偏差。
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

[参考资料](/zh/operations/system-tables/asynchronous_metrics)

<div id="system-table-jemalloc_bins">
  ### 系统表 `jemalloc_bins`
</div>

包含从所有 arena 汇总而来的、通过 jemalloc 分配器在不同大小类 (bin) 中的内存分配信息。

[参考](/zh/operations/system-tables/jemalloc_bins)

<div id="system-table-jemalloc_stats">
  ### 系统表 `jemalloc_stats` (26.2+)
</div>

以单个字符串返回 `malloc_stats_print()` 的完整输出。等同于 `SYSTEM JEMALLOC STATS` 命令。

```sql
SELECT * FROM system.jemalloc_stats
```

<div id="prometheus">
  ### Prometheus
</div>

`asynchronous_metrics` 中所有与 `jemalloc` 相关的指标，也会在 ClickHouse 和 Keeper 中通过 Prometheus 端点暴露。

[Reference](/zh/operations/server-configuration-parameters/settings#prometheus)

<div id="jmst-4lw-command-in-keeper">
  ### Keeper 中的 `jmst` 4LW 命令
</div>

Keeper 支持 `jmst` 4LW 命令，该命令会返回[基础分配器统计信息](https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Basic-Allocator-Statistics)：

```sh
echo jmst | nc localhost 9181
```