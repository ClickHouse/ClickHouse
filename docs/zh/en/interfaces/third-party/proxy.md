---
description: '介绍适用于 ClickHouse 的第三方代理解决方案'
sidebar_label: '代理'
sidebar_position: 29
slug: /interfaces/third-party/proxy
title: '第三方开发者提供的代理服务器'
doc_type: '参考'
---

<div id="chproxy">
  ## chproxy
</div>

[chproxy](https://github.com/Vertamedia/chproxy) 是适用于 ClickHouse database 的 HTTP 代理和负载均衡器。

特性：

* 按用户进行路由和响应缓存。
* 灵活的限制配置。
* 自动续订 SSL 证书。

由 Go 语言实现。

<div id="kittenhouse">
  ## KittenHouse
</div>

[KittenHouse](https://github.com/VKCOM/kittenhouse) 被设计为部署在 ClickHouse 与应用服务器之间的本地代理，适用于应用侧无法缓冲 INSERT 数据或缓冲不便的场景。

特性：

* 内存中和磁盘上的数据缓冲。
* 按表路由。
* 负载均衡与健康检查。

由 Go 语言实现。

<div id="clickhouse-bulk">
  ## ClickHouse-Bulk
</div>

[ClickHouse-Bulk](https://github.com/nikepan/clickhouse-bulk) 是一个简单的 ClickHouse 数据写入采集器。

功能：

* 对请求进行分组，并按阈值或时间间隔发送。
* 支持多个远程服务器。
* 支持基本身份验证。

由 Go 语言实现。