---
description: '您可以监控硬件资源的使用情况以及 ClickHouse
  服务器指标。'
keywords: ['监控', '可观测性', '高级仪表板', '仪表板', '可观测性
    仪表板']
sidebar_label: '监控'
sidebar_position: 45
slug: /operations/monitoring
title: '监控'
doc_type: 'reference'
---

import Image from '@theme/IdealImage';

<div id="monitoring">
  # 监控
</div>

:::note
本指南概述的监控数据可在 ClickHouse Cloud 中查看。除了可通过下文介绍的内置仪表板查看外，基础和高级性能指标也可以直接在主服务控制台中查看。
:::

您可以监控：

* 硬件资源利用率。
* ClickHouse 服务器指标。

<div id="built-in-advanced-observability-dashboard">
  ## 内置高级可观测性仪表板
</div>

<Image img="https://github.com/ClickHouse/ClickHouse/assets/3936029/2bd10011-4a47-4b94-b836-d44557c7fdc1" alt="截图 2023-11-12 下午 6 08 58" size="md" />

ClickHouse 内置高级可观测性仪表板，可通过 `$HOST:$PORT/dashboard` 访问 (需要用户名和密码) ，其中显示以下指标：

* 每秒查询数
* CPU 使用量 (核)
* 运行中的查询数
* 运行中的合并任务数
* 每秒读取字节数
* IO 等待
* CPU 等待
* OS CPU 使用量 (用户态)
* OS CPU 使用量 (内核态)
* 从磁盘读取
* 从文件系统读取
* 已跟踪内存
* 每秒插入行数
* MergeTree parts 总数
* 单个分区的最大 parts 数

<div id="resource-utilization">
  ## 资源利用率
</div>

ClickHouse 还会自行监控硬件资源的状态，例如：

* 处理器的负载和温度。
* 存储系统、RAM 和网络的资源利用率。

这些数据会采集到 `system.asynchronous_metric_log` 表中。

<div id="clickhouse-server-metrics">
  ## ClickHouse 服务器指标
</div>

ClickHouse 服务器 内置了用于监控自身状态的监测机制。

要跟踪 server event，请使用服务器日志。请参阅 configuration file 中的 [日志记录器](../operations/server-configuration-parameters/settings.md#logger) 部分。

ClickHouse 会收集：

* 服务器使用计算资源的各类指标。
* 查询处理的常见统计信息。

你可以在 [system.metrics](/zh/operations/system-tables/metrics)、[system.events](/zh/operations/system-tables/events) 和 [system.asynchronous&#95;metrics](/zh/operations/system-tables/asynchronous_metrics) 表中找到这些指标。

你可以将 ClickHouse 配置为将指标导出到 [Graphite](https://github.com/graphite-project)。请参阅 ClickHouse 服务器 configuration file 中的 [Graphite 部分](../operations/server-configuration-parameters/settings.md#graphite)。在配置指标导出之前，你应先按照 Graphite 的官方[指南](https://graphite.readthedocs.io/en/latest/install.html)完成设置。

你可以将 ClickHouse 配置为将指标导出到 [Prometheus](https://prometheus.io)。请参阅 ClickHouse 服务器 configuration file 中的 [Prometheus 部分](../operations/server-configuration-parameters/settings.md#prometheus)。在配置指标导出之前，你应先按照 Prometheus 的官方[指南](https://prometheus.io/docs/prometheus/latest/installation/)完成设置。

此外，你还可以通过 HTTP API 监控服务器的可用性。向 `/ping` 发送 `HTTP GET` 请求。如果服务器可用，它会返回 `200 OK`。

要监控 cluster configuration 中的服务器，你应设置 [max&#95;replica&#95;delay&#95;for&#95;distributed&#95;queries](../operations/settings/settings.md#max_replica_delay_for_distributed_queries) parameter，并使用 HTTP resource `/replicas_status`。对 `/replicas_status` 的 request 会在副本可用且未落后于其他副本时返回 `200 OK`。如果某个副本存在延迟，则会返回 `503 HTTP_SERVICE_UNAVAILABLE`，并附带有关 gap 的信息。