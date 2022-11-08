---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
sidebar_position: 54
sidebar_label: "\u67E5\u8BE2\u5206\u6790"
---

# 采样查询探查器 {#sampling-query-profiler}

ClickHouse运行允许分析查询执行的采样探查器。 使用探查器，您可以找到在查询执行期间使用最频繁的源代码例程。 您可以跟踪CPU时间和挂钟花费的时间，包括空闲时间。

使用概要分析器:

-   设置 [trace_log](../server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) 服务器配置部分。

    本节配置 [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log) 系统表包含探查器运行的结果。 它是默认配置的。 请记住，此表中的数据仅对正在运行的服务器有效。 服务器重新启动后，ClickHouse不会清理表，所有存储的虚拟内存地址都可能无效。

-   设置 [query_profiler_cpu_time_period_ns](../settings/settings.md#query_profiler_cpu_time_period_ns) 或 [query_profiler_real_time_period_ns](../settings/settings.md#query_profiler_real_time_period_ns) 设置。 这两种设置可以同时使用。

    这些设置允许您配置探查器计时器。 由于这些是会话设置，您可以为整个服务器、单个用户或用户配置文件、交互式会话以及每个单个查询获取不同的采样频率。

默认采样频率为每秒一个采样，CPU和实时定时器都启用。 该频率允许收集有关ClickHouse集群的足够信息。 同时，使用此频率，profiler不会影响ClickHouse服务器的性能。 如果您需要分析每个单独的查询，请尝试使用更高的采样频率。

分析 `trace_log` 系统表:

-   安装 `clickhouse-common-static-dbg` 包。 看 [从DEB软件包安装](../../getting-started/install.md#install-from-deb-packages).

-   允许由内省功能 [allow_introspection_functions](../settings/settings.md#settings-allow_introspection_functions) 设置。

    出于安全原因，默认情况下禁用内省功能。

-   使用 `addressToLine`, `addressToSymbol` 和 `demangle` [内省功能](../../sql-reference/functions/introspection.md) 获取函数名称及其在ClickHouse代码中的位置。 要获取某些查询的配置文件，您需要从以下内容汇总数据 `trace_log` 桌子 您可以通过单个函数或整个堆栈跟踪聚合数据。

如果你需要想象 `trace_log` 信息，尝试 [flamegraph](../../interfaces/third-party/gui/#clickhouse-flamegraph) 和 [测速镜](https://github.com/laplab/clickhouse-speedscope).

## 示例 {#example}

在这个例子中，我们:

-   过滤 `trace_log` 数据由查询标识符和当前日期组成。

-   通过堆栈跟踪聚合。

-   使用内省功能，我们将得到一个报告:

    -   符号名称和相应的源代码函数。
    -   这些函数的源代码位置。

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
