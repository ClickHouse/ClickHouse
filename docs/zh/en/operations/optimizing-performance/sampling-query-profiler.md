---
description: 'ClickHouse 采样查询分析器的文档'
sidebar_label: '查询分析'
sidebar_position: 54
slug: /operations/optimizing-performance/sampling-query-profiler
title: '采样查询分析器'
doc_type: '参考'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="sampling-query-profiler">
  # 采样查询分析器
</div>

ClickHouse 运行采样分析器，可用于分析查询执行过程。
借助该分析器，你可以找出查询执行期间最常用的源代码例程。
你还可以跟踪所耗费的 CPU 时间和挂钟时间，其中也包括空闲时间。

查询分析器在 ClickHouse Cloud 中默认自动启用。
以下示例查询会找出某个已分析查询中最常见的堆栈跟踪，并解析出函数名称和源代码位置：

:::tip
将 `query_id` 的值替换为你要分析的查询 ID。
:::

<Tabs groupId="deployment">
  <TabItem value="cloud" label="ClickHouse Cloud">
    在 ClickHouse Cloud 中，你可以点击查询结果表上方栏最右侧的 **&quot;...&quot;** (位于表/图表切换按钮旁) 来获取查询 ID。这会打开一个上下文菜单，你可以在其中点击 **&quot;Copy query ID&quot;**。

    使用 `clusterAllReplicas(default, system.trace_log)` 从集群中的所有节点查询：

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

  <TabItem value="self-managed" label="自管理">
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

<div id="self-managed-query-profiler">
  ## 在自管理部署中使用查询分析器
</div>

在自管理部署中，如需使用查询分析器，请按以下步骤操作：

<VerticalStepper headerLevel="h3">
  ### 安装带调试信息的 ClickHouse

  安装 `clickhouse-common-static-dbg` 软件包：

  1. 按照[“设置 Debian 仓库”](/zh/install/debian_ubuntu#setup-the-debian-repository)步骤中的说明进行操作
  2. 运行 `sudo apt-get install clickhouse-server clickhouse-client clickhouse-common-static-dbg`，安装包含调试信息的 ClickHouse 已编译二进制文件
  3. 运行 `sudo service clickhouse-server start` 启动服务器
  4. 运行 `clickhouse-client`。`clickhouse-common-static-dbg` 中的调试符号会被服务器自动识别，无需执行任何额外操作来启用它们

  ### 检查服务器配置

  请确保你的[服务器配置文件](/zh/operations/configuration-files)中已设置 [`trace_log`](../../operations/server-configuration-parameters/settings.md#trace_log) 部分。该部分默认已启用：

  ```xml
  <!-- Trace 日志。存储由查询分析器收集的堆栈跟踪。
       参见 query_profiler_real_time_period_ns 和 query_profiler_cpu_time_period_ns 设置。 -->
  <trace_log>
      <database>system</database>
      <table>trace_log</table>

      <partition_by>toYYYYMM(event_date)</partition_by>
      <flush_interval_milliseconds>7500</flush_interval_milliseconds>
      <max_size_rows>1048576</max_size_rows>
      <reserved_size_rows>8192</reserved_size_rows>
      <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
      <!-- 指示在发生 crash 时是否应将日志转储到磁盘 -->
      <flush_on_crash>false</flush_on_crash>
      <symbolize>true</symbolize>
  </trace_log>
  ```

  此部分配置了 [trace&#95;log](/zh/operations/system-tables/trace_log) 系统表，其中包含分析器运行的结果。
  请注意，此表中的数据仅对正在运行的服务器有效。
  服务器重启后，ClickHouse 不会清理该表，已存储的所有虚拟内存地址都可能失效。

  ### 配置分析器计时器

  设置 [`query_profiler_cpu_time_period_ns`](../../operations/settings/settings.md#query_profiler_cpu_time_period_ns) 或 [`query_profiler_real_time_period_ns`](../../operations/settings/settings.md#query_profiler_real_time_period_ns)。
  这两个设置可以同时使用。

  通过这些设置，你可以配置分析器计时器。
  由于它们属于 session settings，因此你可以为整个服务器、单个用户或用户 profile、当前交互式会话，以及每个单独的查询设置不同的采样频率。

  默认采样频率为每秒一个样本，并且 CPU 计时器和实际时间计时器都处于启用状态。
  该频率可以在不影响服务器性能的情况下，为 ClickHouse cluster 收集足够的信息。
  如果你需要分析每个单独的查询，请使用更高的采样频率。

  ### 分析 `trace_log` 系统表

  要分析 `trace_log` 系统表，请使用 [`allow_introspection_functions`](../../operations/settings/settings.md#allow_introspection_functions) 设置启用内部信息函数：

  ```sql
  SET allow_introspection_functions=1
  ```

  :::note
  出于安全原因，内部信息函数默认处于禁用状态
  :::

  使用 `addressToLine`、`addressToLineWithInlines`、`addressToSymbol` 和 `demangle` [内部信息函数](../../sql-reference/functions/introspection.md) 获取函数名称及其在 ClickHouse 代码中的位置。
  要获取某个查询的 profile，你需要对 `trace_log` 表中的数据进行聚合。
  你可以按单个函数或整个堆栈跟踪进行聚合。

  :::tip
  如果你需要将 `trace_log` 信息可视化，可以尝试 [flamegraph](/zh/interfaces/third-party/gui#clickhouse-flamegraph) 和 [speedscope](https://www.speedscope.app)。
  :::
</VerticalStepper>

<div id="flamegraph">
  ## 使用 `flameGraph` 函数生成火焰图
</div>

ClickHouse 提供了聚合函数 [`flameGraph`](/zh/sql-reference/aggregate-functions/reference/flame_graph)，可根据存储在 `trace_log` 中的堆栈跟踪直接生成火焰图。
输出为字符串数组，格式与 [flamegraph.pl](https://github.com/brendangregg/FlameGraph) 兼容。

**语法：**

```sql
flameGraph(traces, [size = 1], [ptr = 0])
```

**参数：**

* `traces` — 一条调用栈。[`Array(UInt64)`](/zh/sql-reference/data-types/array)。
* `size` — 用于内存分析的分配大小。[`Int64`](/zh/sql-reference/data-types/int-uint)。
* `ptr` — 分配地址。[`UInt64`](/zh/sql-reference/data-types/int-uint)。

当 `ptr` 非零时，`flameGraph` 会将大小和指针相同的内存分配 (`size > 0`) 与释放 (`size < 0`) 进行配对。
只显示尚未释放的分配。
无法配对的释放会被忽略。

<div id="cpu-flame-graph">
  ### CPU 火焰图
</div>

:::note
以下查询要求已安装 [flamegraph.pl](https://github.com/brendangregg/FlameGraph)。

你可以运行以下命令进行安装：

```bash
git clone https://github.com/brendangregg/FlameGraph
# Then use it as:
# ~/FlameGraph/flamegraph.pl
```

将以下查询中的 `flamegraph.pl` 替换为你本机上 `flamegraph.pl` 所在的路径
:::

```sql
SET query_profiler_cpu_time_period_ns = 10000000;
```

运行查询，然后构建火焰图：

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(arrayReverse(trace)))
        FROM system.trace_log
        WHERE trace_type = 'CPU' AND query_id = '<query_id>'" \
    | flamegraph.pl > flame_cpu.svg
```

<div id="memory-flame-graph-all">
  ### 内存火焰图 — 所有内存分配
</div>

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1;
```

运行查询，然后生成火焰图：

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size))
        FROM system.trace_log
        WHERE trace_type = 'MemorySample' AND query_id = '<query_id>'" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem.svg
```

<div id="memory-flame-graph-unfreed">
  ### 内存火焰图 — 未释放的内存分配
</div>

此 Variant 会按指针对分配与释放进行匹配，并且只显示查询期间未被释放的内存。

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1,
    use_uncompressed_cache = 1,
    merge_tree_max_rows_to_use_cache = 100000000000,
    merge_tree_max_bytes_to_use_cache = 1000000000000;
```

运行以下查询以生成火焰图：

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size, ptr))
        FROM system.trace_log
        WHERE trace_type = 'MemorySample' AND query_id = '<query_id>'" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_unfreed.svg
```

<div id="memory-flame-graph-time-point">
  ### 内存火焰图——某一时间点的活跃分配
</div>

这种方法可帮助你找出峰值内存占用，并将该时刻分配的内存可视化。

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1;
```

<div id="find-memory-usage-over-time">
  #### 查看内存使用情况随时间的变化
</div>

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

<div id="find-time-point-maximum-memory-usage">
  #### 查找内存使用量最大的时间点
</div>

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

<div id="build-flame-graph">
  #### 构建该时间点的活跃分配火焰图
</div>

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

<div id="build-flame-graph-deallocations">
  #### 构建该时间点之后内存释放的火焰图 (以了解后续释放了哪些内容)
</div>

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

<div id="example">
  ## 示例
</div>

以下代码片段：

* 按查询标识符和当前日期过滤 `trace_log` 数据。
* 按堆栈跟踪聚合。
* 使用内部信息函数生成以下报告：
  * 符号名称及其对应的源代码中的函数。
  * 这些函数在源代码中的位置。

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