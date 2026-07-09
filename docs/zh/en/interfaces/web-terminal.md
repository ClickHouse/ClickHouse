---
description: 'Web 终端文档：这是一个通过 WebSocket 在浏览器中提供 `clickhouse-client` 会话的界面'
sidebar_label: 'Web 终端'
sidebar_position: 22
slug: /interfaces/web-terminal
title: 'Web 终端'
doc_type: 'reference'
---

Web 终端是一个浏览器内界面，可通过 WebSocket 提供交互式 `clickhouse-client` 会话。可通过任意 ClickHouse HTTP 端口上的 `/webterminal` 路径访问该界面。

访问任意 ClickHouse HTTP 端口上的 `/webterminal` (例如 `http://localhost:8123/webterminal`) 即可打开终端。

<div id="enabling-the-feature">
  ## 启用和禁用该功能
</div>

`/webterminal` 端点默认启用，由 `enable_webterminal` 服务器设置控制。要禁用它，请将该设置设为 `false`；此后，对 `/webterminal` 的请求将返回 HTTP 状态码 `403 Forbidden`。

```xml
<clickhouse>
    <enable_webterminal>false</enable_webterminal>
</clickhouse>
```

:::note
`enable_webterminal` 已替代原先的 `allow_experimental_webterminal` 设置。为保持向后兼容，如果未设置 `enable_webterminal`，旧名称仍然会继续生效。
:::

<div id="authentication">
  ## 身份验证
</div>

Web 终端会针对与 HTTP 协议相同的 `Session` 和访问控制检查对用户进行身份验证，但凭据是通过已建立的 WebSocket 连接在带内传输的，而不是通过 HTTP 升级请求传递。WebSocket 握手完成后，浏览器会以 JSON 格式发送第一条消息：

```json
{"type": "auth", "user": "<user>", "password": "<password>"}
```

这样可以避免将凭据放在 URL 查询参数中，或放在附加到升级请求的 `Authorization` 请求头里，因为这些信息可能会出现在浏览器历史记录、服务器访问日志以及反向代理日志中。`/webterminal` 会刻意**不**读取升级请求中的 URL 参数、HTTP Basic 身份验证或 `X-ClickHouse-User`/`X-ClickHouse-Key` 请求头。

无效凭据会导致服务器以代码 `1008` 关闭 WebSocket；浏览器 UI 会重新提示输入凭据。

<div id="session">
  ## 会话界面如下
</div>

完成身份验证后，服务器会在伪终端中运行 `clickhouse-client`，并通过 WebSocket 转发其输入和输出。该会话提供完整的 `clickhouse-client` 使用体验，包括：

* 语法高亮。
* 自动补全。
* 多行查询。
* 命令历史记录 (在整个会话期间保存在服务器端) 。

该终端使用 [xterm.js](https://xtermjs.org/) 进行渲染。所有资源均由 ClickHouse 二进制文件本身提供——不会加载任何第三方 CDN。

<div id="play-integration">
  ## 与 `/play` 集成
</div>

[`/play`](/zh/interfaces/http) Web SQL UI 将 Web 终端嵌入为可停靠面板。你可以通过侧边栏中的终端图标切换显示，或者在查询编辑器为空时按 `~` 键。`/play` 页面会在加载时检测 `/webterminal` 是否可用，并在端点不可用时隐藏终端控件 (例如，当 `enable_webterminal` 设置为 `false` 时) 。

<div id="security">
  ## 安全注意事项
</div>

Web 终端会向任何能够通过 ClickHouse HTTP 端点完成身份验证的用户暴露一个类似交互式 shell 的会话，因此，适用于 HTTP 协议的注意事项同样适用于此处：

* 在不受信任的环境中，务必通过 HTTPS 提供 `/webterminal`，以保护凭据和会话流量。
* 应像限制对 HTTP 协议的访问一样，在网络层限制访问 (例如使用防火墙、反向代理或 `listen_host` 配置) 。
* 该端点会根据 `Host` 验证 `Origin` 请求头，以降低跨源 WebSocket 劫持风险；如果你在外部终止 TLS，请相应配置反向代理。
* 在经过负责 TLS 终止的反向代理时，尽管浏览器使用的是 `https`，到 ClickHouse 的上游连接仍是明文 `http`，因此严格的同源检查会拒绝合法连接。对于这类部署，请将 `webterminal_allowed_origins` 设置为允许打开 WebSocket 会话的完整来源列表，多个来源之间用逗号分隔；当此设置非空时，它会替代默认的同源检查。例如：`<webterminal_allowed_origins>https://example.com,https://app.example.com:8443</webterminal_allowed_origins>`。

该处理程序还会根据 RFC 6455 强制执行 WebSocket 协议一致性检查：未掩码的客户端帧、保留操作码、过大的或分片的控制帧以及保留的 RSV 位，都会以协议错误关闭代码被拒绝。

<div id="platform">
  ## 平台可用性
</div>

该处理程序可在 ClickHouse 支持的所有平台上编译。内置 `clickhouse-client` 运行器所使用的伪终端 layer 基于可移植的 POSIX 机制 (`posix_openpt`/`grantpt`/`unlockpt`) 实现，并针对 Linux 提供了一条使用线程安全 `ptsname_r` 的专用路径。当端点不可用时 (例如将 `enable_webterminal` 设置为 `false`) ，ClickHouse 首页以及 `/play` 中指向 `/webterminal` 的链接会自动隐藏。