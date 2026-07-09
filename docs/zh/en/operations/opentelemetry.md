---
description: '在 ClickHouse 中使用 OpenTelemetry 进行分布式链路追踪和指标采集的指南'
sidebar_label: '使用 OpenTelemetry 追踪 ClickHouse'
sidebar_position: 62
slug: /operations/opentelemetry
title: '使用 OpenTelemetry 追踪 ClickHouse'
doc_type: 'guide'
---

[OpenTelemetry](https://opentelemetry.io/) 是一种用于从分布式应用中采集链路追踪和指标的开放标准。ClickHouse 已对 OpenTelemetry 提供部分支持。

<div id="supplying-trace-context-to-clickhouse">
  ## 向 ClickHouse 传递 trace context
</div>

ClickHouse 接受 trace context HTTP 请求头，如 [W3C recommendation](https://www.w3.org/TR/trace-context/) 中所述。它也接受通过原生协议传递的 trace context，该协议用于 ClickHouse 服务器之间，或客户端与服务器之间的通信。进行手动测试时，可以使用 `--opentelemetry-traceparent` 和 `--opentelemetry-tracestate` 标志，向 `clickhouse-client` 提供符合 Trace Context recommendation 的 trace context 请求头。

如果未提供父 trace context，或者提供的 trace context 不符合上述 W3C 标准，ClickHouse 可以启动一个新的 trace，其概率由 [opentelemetry&#95;start&#95;trace&#95;probability](/zh/operations/settings/settings#opentelemetry_start_trace_probability) 设置控制。

<div id="propagating-the-trace-context">
  ## 传播 trace context
</div>

在以下情况下，trace context 会传递到下游服务：

* 对远程 ClickHouse 服务器发起查询时，例如使用 [Distributed](../engines/table-engines/special/distributed.md) 表引擎时。

* 使用 [url](../sql-reference/table-functions/url.md) 表函数时。trace context 信息会通过 HTTP 请求头 发送。

<div id="tracing-clickhouse-keeper-requests">
  ## 追踪 ClickHouse Keeper 请求
</div>

ClickHouse 支持对 [ClickHouse Keeper](../guides/sre/keeper/index.md) 请求进行 OpenTelemetry 追踪 (与 ZooKeeper 兼容的协调服务) 。此功能可让你深入了解 Keeper 操作的完整生命周期，从客户端提交请求到服务器端处理的全过程。

<div id="enabling-keeper-tracing">
  ### 启用 Keeper 链路追踪
</div>

要为 Keeper 请求启用链路追踪，请在 ZooKeeper/Keeper 客户端配置中设置以下参数：

```xml
<clickhouse>
    <zookeeper>
        <node>
            <host>keeper1</host>
            <port>9181</port>
        </node>
        <!-- Enable OpenTelemetry tracing context propagation -->
        <pass_opentelemetry_tracing_context>true</pass_opentelemetry_tracing_context>
    </zookeeper>
</clickhouse>
```

<div id="keeper-span-types">
  ### Keeper Span 类型
</div>

启用 tracing 后，ClickHouse 会为客户端侧和服务端侧的 Keeper 操作创建 spans：

**客户端侧 spans：**

* `zookeeper.create` — 创建新节点
* `zookeeper.get` — 获取节点数据
* `zookeeper.set` — 设置节点数据
* `zookeeper.remove` — 删除节点
* `zookeeper.list` — 列出子节点
* `zookeeper.exists` — 检查节点是否存在
* `zookeeper.multi` — 以原子方式执行多个操作
* `zookeeper.client.requests_queue` — 请求发送前在队列中等待的时间

**服务端侧 spans (Keeper) ：**

* `keeper.receive_request` — 接收并解析来自客户端的请求
* `keeper.dispatcher.requests_queue` — dispatcher 中的请求排队
* `keeper.write.pre_commit` — 在 Raft commit 前对写入请求进行预处理
* `keeper.write.commit` — 在 Raft commit 后处理写入请求
* `keeper.read.wait_for_write` — 读取请求等待其依赖的写入完成
* `keeper.read.process` — 处理读取请求
* `keeper.dispatcher.responses_queue` — dispatcher 中的响应排队
* `keeper.send_response` — 向客户端发送响应

<div id="sampling-and-performance">
  ### 采样与性能
</div>

为控制链路追踪开销，Keeper 实现了动态采样。采样率会根据请求大小在 1/10,000 到 1/10 之间自动调整。所有请求 (无论是否被采样) 的耗时都会记录到直方图指标中，用于性能监控。

<div id="tracing-the-clickhouse-itself">
  ## 跟踪 ClickHouse 本身
</div>

ClickHouse 会为每个查询以及部分查询执行阶段创建 `trace spans`，例如生成查询计划或执行分布式查询时。

要让这些跟踪信息真正发挥作用，必须将其导出到支持 OpenTelemetry 的监控系统中，例如 [Jaeger](https://jaegertracing.io/) 或 [Prometheus](https://prometheus.io/)。ClickHouse 不依赖任何特定的监控系统，而是仅通过系统表提供跟踪数据。OpenTelemetry `trace span` 信息中[标准要求](https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/overview.md#span)的部分存储在 [system.opentelemetry&#95;span&#95;log](../operations/system-tables/opentelemetry_span_log.md) 表中。

必须在服务器配置中启用该表，请参见默认配置文件 `config.xml` 中的 `opentelemetry_span_log` 元素。该表默认已启用。

标签或属性以两个并行数组的形式保存，分别包含键和值。可使用 [ARRAY JOIN](../sql-reference/statements/select/array-join.md) 来处理它们。

<div id="log-query-settings">
  ## 日志查询设置
</div>

[log&#95;query&#95;settings](settings/settings.md) 设置用于记录查询执行期间对查询设置所做的更改。启用后，对查询设置的任何修改都会记录到 OpenTelemetry span 日志中。此功能在生产环境中特别有用，可用于跟踪可能影响查询性能的配置变更。

<div id="integration-with-monitoring-systems">
  ## 与监控系统集成
</div>

目前，还没有现成的工具可以将 ClickHouse 中的 tracing 数据导出到监控系统。

在测试场景下，可以通过基于 [system.opentelemetry&#95;span&#95;log](../operations/system-tables/opentelemetry_span_log.md) 表的 [URL](../engines/table-engines/special/url.md) 引擎和 materialized view 来配置导出，这会将接收到的日志数据推送到 trace collector 的 HTTP 端点。例如，要将最小化的 span 数据推送到运行在 `http://localhost:9411` 的 Zipkin 实例，并使用 Zipkin v2 JSON 格式：

```sql
CREATE MATERIALIZED VIEW default.zipkin_spans
ENGINE = URL('http://127.0.0.1:9411/api/v2/spans', 'JSONEachRow')
SETTINGS output_format_json_named_tuples_as_objects = 1,
    output_format_json_array_of_rows = 1 AS
SELECT
    lower(hex(trace_id)) AS traceId,
    CASE WHEN parent_span_id = 0 THEN '' ELSE lower(hex(parent_span_id)) END AS parentId,
    lower(hex(span_id)) AS id,
    operation_name AS name,
    start_time_us AS timestamp,
    finish_time_us - start_time_us AS duration,
    cast(tuple('clickhouse'), 'Tuple(serviceName text)') AS localEndpoint,
    cast(tuple(
        attribute.values[indexOf(attribute.names, 'db.statement')]),
        'Tuple("db.statement" text)') AS tags
FROM system.opentelemetry_span_log
```

如果发生任何错误，出错的那部分日志数据会被悄无声息地丢弃。如果数据未到达，请检查服务器日志中的错误信息。

<div id="related-content">
  ## 相关内容
</div>

* 博客：[使用 ClickHouse 构建可观测性解决方案 - 第 2 部分 - 链路追踪](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)