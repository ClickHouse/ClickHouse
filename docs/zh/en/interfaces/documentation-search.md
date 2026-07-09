---
description: '内置文档搜索 Web UI 的文档，可通过 HTTP 接口的 `/docs` 路径访问，后端由 `system.documentation` 表提供支持'
sidebar_label: '文档搜索'
sidebar_position: 23
slug: /interfaces/documentation-search
title: '文档搜索'
doc_type: 'reference'
---

文档搜索页面是一个小巧、独立的 Web UI，用于对内嵌的参考文档进行即时搜索。它可通过任意 ClickHouse HTTP 端口上的 `/docs` 路径访问。

访问任意 ClickHouse HTTP 端口上的 `/docs` (例如 `http://localhost:8123/docs`) 即可打开该页面。

<div id="what-it-does">
  ## 功能说明
</div>

该页面会在你输入时通过 HTTP 查询 [`system.documentation`](/zh/operations/system-tables/documentation) 表，并渲染所选实体的 Markdown。由于它读取 `system.documentation`，因此涵盖了该系统表公开的所有实体——函数、聚合函数、表函数、表引擎、数据库引擎、数据类型、设置、格式、压缩编解码器、profile events、指标、系统表本身等等——并且始终与运行中 server 内嵌的文档保持一致。

在搜索框中输入内容后，匹配项会以按类型着色的列表显示；选择某个匹配项后，会渲染其文档。渲染内容包括：

* 实体标题旁的铅笔链接，可根据 `system.documentation` 的 `source` 列打开其在 GitHub 上的源文件；
* 代码块的 ClickHouse SQL 语法高亮，使用与 [`/play`](/zh/interfaces/http) UI 相同的内嵌词法分析器 (`Lexer.wasm`) ；
* 通过 [KaTeX](https://katex.org/) 渲染 TeX 数学公式 (例如 `corr` 页面中的公式) ；
* `:::note`/`:::tip`/… 提示块、带可分享链接的标题锚点，以及鼠标悬停在代码块上时显示的“复制”按钮；
* 相对链接会在应用内解析为另一个已有文档的实体；如果不存在，则解析到 `https://clickhouse.com/docs`；“Related”和“Alias of”引用会变为应用内链接。

当前搜索词、已打开的实体和所在章节都会同步到 URL 片段中，因此可以直接链接到特定页面或章节，并且可通过浏览器的后退/前进导航恢复。浅色/深色主题切换器 (支持自动检测) 与 `/play` 保持一致。

<div id="connecting">
  ## 连接
</div>

顶部栏中有 `URL`、`user` 和 `password` 输入框，与 `/play` 中的完全相同。页面由 ClickHouse 提供时，`URL` 默认使用当前 源站；当页面作为本地文件打开时，默认值为 `http://localhost:8123/`，因此也可以在本地打开该页面并连接到远程服务器。交叉链接名称缓存会在连接发生变化时自动重建。

<div id="assets">
  ## 资源
</div>

所有资源——包括 Markdown 渲染器 ([Marked](https://marked.js.org/)) 、数学渲染器 (KaTeX 及其字体) 以及 SQL 词法分析器——在通过 HTTP 提供页面时，都是直接由 ClickHouse 二进制文件本身提供的。ClickHouse 的 HTTP 源站不会加载任何第三方 CDN，因此页面是自包含的、可离线运行的，并且不会在处理凭据的同时执行第三方网络代码。

<div id="security">
  ## 安全注意事项
</div>

该页面会使用在请求头中填写的凭据向 ClickHouse HTTP 端点发出查询，因此，适用于 HTTP 协议的相同注意事项也适用于这里：

* 在不受信任的环境中，务必通过 HTTPS 提供 `/docs`，以保护凭据。
* 应像限制对 HTTP 协议的访问一样，在网络层限制访问 (防火墙、反向代理或 `listen_host` 配置) 。

`system.documentation` 仅包含嵌入在服务器中的静态参考文档，因此该页面不会暴露您任何表中的数据。