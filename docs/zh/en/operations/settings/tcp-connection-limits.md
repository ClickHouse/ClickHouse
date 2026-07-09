---
description: 'TCP 连接限制。'
sidebar_label: 'TCP 连接限制'
slug: /operations/settings/tcp-connection-limits
title: 'TCP 连接限制'
doc_type: 'reference'
---

<div id="overview">
  ## 概述
</div>

你的 ClickHouse TCP 连接 (即通过[命令行客户端](https://clickhouse.com/docs/interfaces/client)建立的连接) 
可能会在查询次数达到一定数量或连接持续一段时间后自动断开。
断开后，不会自动重连 (除非由其他操作触发，
例如在命令行客户端中再次发送查询) 。

可以通过将服务器设置
`tcp_close_connection_after_queries_num` (用于查询限制) 
或 `tcp_close_connection_after_queries_seconds` (用于持续时间限制) 设为大于 0
来启用连接限制。
如果这两个限制都已启用，则连接会在任一限制先达到时关闭。

当达到限制并断开连接时，客户端会收到
`TCP_CONNECTION_LIMIT_REACHED` 异常，且**导致断开的那个查询绝不会被处理**。

<div id="query-limits">
  ## 查询限制
</div>

假设 `tcp_close_connection_after_queries_num` 设置为 N，则该连接允许
N 次成功查询。到第 N + 1 次查询时，客户端将断开连接。

每个处理过的查询都会计入查询限制。因此，连接命令行客户端时，
可能会自动执行一条初始的 system warnings 查询，这也会计入限制。

当 TCP 连接处于空闲状态时 (即在一段时间内未处理任何查询，
这段时长由会话设置 `poll_interval` 指定) ，当前累计的查询次数会重置为 0。
这意味着，如果连接期间出现空闲，
单个连接中的查询总数可能会超过
`tcp_close_connection_after_queries_num`。

<div id="duration-limits">
  ## 持续时间限制
</div>

连接持续时间从客户端建立连接时开始计算。
当超过 `tcp_close_connection_after_queries_seconds` 秒后，客户端会在下一次查询时断开连接。