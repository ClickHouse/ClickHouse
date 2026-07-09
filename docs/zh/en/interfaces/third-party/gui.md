---
description: '用于 ClickHouse 的第三方 GUI 工具和应用程序列表'
sidebar_label: '可视化界面'
sidebar_position: 28
slug: /interfaces/third-party/gui
title: '第三方开发者提供的可视化界面'
doc_type: 'reference'
---

<div id="open-source">
  ## 开源
</div>

<div id="agx">
  ### agx
</div>

[agx](https://github.com/agnosticeng/agx) 是一款使用 Tauri 和 SvelteKit 构建的桌面应用，提供现代化界面，可借助 ClickHouse 的嵌入式数据库引擎 (chdb) 来探索和查询数据。

* 运行原生应用时可利用 chdb。
* 运行 Web 版本时可连接到 ClickHouse 实例。
* 采用 Monaco 编辑器，让你轻松上手。
* 提供多种持续演进的数据可视化方式。

<div id="ch-ui">
  ### ch-ui
</div>

[ch-ui](https://github.com/caioricciuti/ch-ui) 是一款简洁的 React.js 应用界面，面向 ClickHouse 数据库，专为执行查询和数据可视化而设计。它基于 React 和适用于 Web 的 ClickHouse 客户端构建，提供简洁且易用的 UI，便于进行数据库交互。

功能：

* ClickHouse 集成：轻松管理连接并执行查询。
* 响应式选项卡管理：可动态处理多个选项卡，例如查询选项卡和表选项卡。
* 性能优化：利用 IndexedDB 实现高效缓存和状态管理。
* 本地数据存储：所有数据均存储在浏览器本地，确保不会发送到其他任何地方。

<div id="chartdb">
  ### ChartDB
</div>

[ChartDB](https://chartdb.io) 是一款免费开源工具，只需一次查询即可对数据库 schema (包括 ClickHouse) 进行可视化和设计。它基于 React 构建，体验流畅且易于上手，无需数据库凭据或注册即可开始使用。

功能：

* Schema 可视化：可即时导入并可视化 ClickHouse schema，包括包含 materialized view 和标准视图的 ER 图，并显示与各表的引用关系。
* AI 驱动的 DDL 导出：可轻松生成 DDL 脚本，便于更好地管理 schema 和编写文档。
* 支持多种 SQL 方言：兼容多种 SQL 方言，适用于不同的数据库环境。
* 无需注册或凭据：所有功能都可直接在浏览器中使用，兼顾便捷性与安全性。

[ChartDB 源代码](https://github.com/chartdb/chartdb)。

<div id="datastoria">
  ### DataStoria
</div>

[DataStoria](https://github.com/FrankChen021/datastoria) 是一款由 AI 驱动的 Web 控制台应用，可在统一界面中管理多个 ClickHouse 集群。

功能：

* **AI 驱动的智能分析**：使用自然语言探索数据、优化和修复 SQL 查询，并将数据可视化。
* **官方 ClickHouse Agent Skills 集成**：利用[官方最佳实践](https://github.com/ClickHouse/agent-skills)向 AI 获取数据库优化建议。
* **智能错误诊断**：通过精确的行号和列号高亮即时定位语法错误，并一键获取 AI 驱动的修复建议。
* **系统表检查**：借助强大的可视化仪表板和筛选器，深入分析 `system.query_log`、`system.query_views_log`、`system.zookeeper`、`system.ddl_distributed_queue`、`system.part_log` 和 `system.processes`，快速了解您的集群。
* **一键 Explain**：通过可视化 AST 和管道视图，立即理解查询执行计划。
* **依赖关系图**：将表之间的关系可视化，并通过 Materialized Views、分布式表和外部系统追踪数据流。
* **集群监控**：通过实时指标、merge 操作、复制状态、查询性能等监控所有节点。
* **隐私与安全**：所有 SQL 查询都会直接从您的浏览器发送到 ClickHouse server，确保完全私密。

[DataStoria 文档](https://docs.datastoria.app)。

<div id="datapup">
  ### DataPup
</div>

[DataPup](https://github.com/DataPupOrg/DataPup) 是一款现代化的跨平台数据库客户端，集成 AI 辅助功能，并原生支持 ClickHouse。

功能：

* AI 驱动的 SQL 查询辅助，提供智能建议
* 原生支持 ClickHouse 连接，并可安全处理凭据
* 界面美观且易于访问，提供多种主题 (浅色、深色和彩色变体) 
* 支持对查询结果进行高级筛选和探索
* 支持跨平台 (macOS、Windows、Linux) 
* 性能快速且响应灵敏
* 开源，并采用 MIT 许可证

<div id="dory">
  ### Dory
</div>

[Dory](https://github.com/dorylab/dory) 是一款 AI 原生的 SQL 工作区，对 ClickHouse 提供一流支持，并内置 AI。

功能：

* 用于 SQL 生成、解释和调试的 AI Copilot
* 在统一工作区中管理和查询多个 ClickHouse 集群
* 支持 schema 感知的 SQL 自动补全和多选项卡查询工作区
* 可对查询结果进行交互式探索，支持筛选和可视化
* AI 驱动的表摘要，帮助理解数据集
* 支持 SSH 隧道的 ClickHouse 直连
* 面向开发者的现代化界面，支持浅色、深色等主题
* 跨平台桌面应用 (macOS、Windows、Linux) ，并支持 Docker
* 开源并采用 MIT 许可证

<div id="clickhouse-schemaflow-visualizer">
  ### ClickHouse Schema Flow Visualizer
</div>

[ClickHouse Schema Flow Visualizer](https://github.com/FulgerX2007/clickhouse-schemaflow-visualizer) 是一款用于可视化 ClickHouse 表关系的开源 Web 应用。
它会连接到 ClickHouse 实例，解析 `system.tables` 元数据 (引擎类型、依赖项、materialized view 的 SELECT) ，并渲染交互式表级数据流图以及列级关系图，其中每条边上都标注了转换表达式。图表使用 Dagre 自动布局，并以纯内联 SVG 形式渲染——不会加载任何客户端侧的图表运行时。

功能：

* 通过直观的侧边栏浏览 ClickHouse 数据库和表
* 数据流视图：表级上游来源和下游 materialized view
* 关系视图：列级映射，并在每条边上显示解析后的转换表达式 (例如 `toStartOfHour(scheduled_departure)`、`avgState(delay_minutes)`) 
* 为 `MergeTree`、`Replicated*`、`Distributed`、`MaterializedView` 和 `Dictionary` 提供具备引擎感知的图标和颜色编码
* 在关系视图中点击某一列，可高亮显示其在整个管道中的完整数据路径
* 实时侧边栏过滤器，以及可跳转到任意表、列或引擎的 `Ctrl+K` / `⌘K` 命令面板
* 可选的元数据叠加层，用于显示每个表的行数和磁盘占用大小
* 将当前图表导出为独立的 HTML 文件
* 支持与 ClickHouse 建立 TLS 连接，并可选择跳过验证以及使用自定义 CA / client 证书

[ClickHouse Schema Flow Visualizer - 源代码](https://github.com/FulgerX2007/clickhouse-schemaflow-visualizer)

<div id="tabix">
  ### Tabix
</div>

[Tabix](https://github.com/tabixio/tabix) 项目提供的 ClickHouse Web 界面。

功能：

* 可直接在浏览器中使用 ClickHouse，无需安装额外软件。
* 带语法高亮的查询编辑器。
* 命令自动补全。
* 用于以图形方式分析查询执行情况的工具。
* 配色方案选项。

[Tabix 文档](https://tabix.io/doc/).

<div id="houseops">
  ### HouseOps
</div>

[HouseOps](https://github.com/HouseOps/HouseOps) 是适用于 OSX、Linux 和 Windows 的 UI/IDE。

功能：

* 带语法高亮的查询构建器，可在表格视图或 JSON 视图中查看响应。
* 将查询结果导出为 CSV 或 JSON。
* 带说明的进程列表。编写模式。可停止 (`KILL`) 进程。
* 数据库关系图。显示所有表及其列，以及附加信息。
* 快速查看列大小。
* 服务器配置。

以下功能计划在后续开发中提供：

* 数据库管理。
* 用户管理。
* 实时数据分析。
* 集群监控。
* 集群管理。
* 监控复制表和 Kafka 表。

<div id="lighthouse">
  ### LightHouse
</div>

[LightHouse](https://github.com/VKCOM/lighthouse) 是一个适用于 ClickHouse 的轻量级 Web 界面。

功能：

* 支持筛选和查看元数据的表列表。
* 支持筛选和排序的表预览。
* 执行只读查询。

<div id="redash">
  ### Redash
</div>

[Redash](https://github.com/getredash/redash) 是一个数据可视化平台。

Redash 支持包括 ClickHouse 在内的多种数据源，还可以将来自不同数据源的查询结果联接成一个最终数据集。

功能：

* 强大的查询编辑器。
* 数据库资源管理器。
* 可视化工具，支持以不同形式展示数据。

<div id="grafana">
  ### Grafana
</div>

[Grafana](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/) 是一个用于监控和可视化的平台。

“Grafana 让您无论指标存储在何处，都能够对其进行查询、可视化、告警并深入了解。您还可以与团队一起创建、浏览和共享仪表盘，打造数据驱动文化。深受社区信赖与喜爱。” — grafana.com。

ClickHouse 数据源插件支持将 ClickHouse 用作后端数据库。

<div id="qryn">
  ### qryn
</div>

[qryn](https://metrico.in) 是一个面向 ClickHouse 的多协议、高性能可观测性技术栈&#x20;*&#x20;(前身为 cLoki)&#x20;*，提供原生 Grafana 集成，让用户能够从任何支持 Loki/LogQL、Prometheus/PromQL、OTLP/Tempo、Elastic、InfluxDB 等的 agent 中摄取并分析日志、指标和链路追踪数据。

特性：

* 内置 Explore UI 和 LogQL 命令行客户端，用于查询、提取和可视化数据
* 原生支持 Grafana API，无需插件即可进行查询、处理、摄取、链路追踪和告警
* 强大的管道，可从日志、事件、链路追踪等数据中动态搜索、过滤并提取信息
* 摄取和 PUSH API 与 LogQL、PromQL、InfluxDB、Elastic 等完全兼容
* 开箱即用，可直接配合 Promtail、Grafana-Agent、Vector、Logstash、Telegraf 等 Agents 使用

<div id="dbeaver">
  ### DBeaver
</div>

[DBeaver](https://dbeaver.io/) - 支持 ClickHouse 的通用桌面数据库客户端。

功能：

* 支持带有语法高亮和自动补全的查询开发。
* 支持按过滤器筛选和搜索元数据的表列表。
* 表数据预览。
* 全文搜索。

默认情况下，DBeaver 不使用 session 进行连接 (例如命令行客户端会使用) 。如果你需要 session 支持 (例如为当前 session 设置参数) ，请编辑驱动连接属性，并将 `session_id` 设置为一个随机字符串 (其底层使用的是 HTTP connection) 。之后，你就可以在查询窗口中使用任何设置了。

<div id="clickhouse-cli">
  ### clickhouse-cli
</div>

[clickhouse-cli](https://github.com/hatarist/clickhouse-cli) 是 ClickHouse 的一款替代命令行客户端，使用 Python 3 编写。

功能：

* 自动补全。
* 为查询和数据输出提供语法高亮。
* 支持对数据输出使用分页器。
* 自定义的 PostgreSQL 风格命令。

<div id="clickhouse-flamegraph">
  ### clickhouse-flamegraph
</div>

[clickhouse-flamegraph](https://github.com/Slach/clickhouse-flamegraph) 是一款专门用于将 `system.trace_log` 以 [flamegraph](http://www.brendangregg.com/flamegraphs.html) 形式可视化的工具。

<div id="clickhouse-plantuml">
  ### clickhouse-plantuml
</div>

[cickhouse-plantuml](https://pypi.org/project/clickhouse-plantuml/) 是一个用于生成表结构 [PlantUML](https://plantuml.com/) 图的脚本。

<div id="clickhouse-table-graph">
  ### ClickHouse table graph
</div>

[ClickHouse table graph](https://github.com/mbaksheev/clickhouse-table-graph) 是一个简单的命令行客户端工具，用于可视化 ClickHouse 表之间的依赖关系。该工具会从 `system.tables` 表中提取表之间的关联，并生成 [mermaid](https://mermaid.js.org/syntax/flowchart.html) 格式的依赖关系流程图。借助这个工具，您可以轻松直观地查看表依赖关系，并理解 ClickHouse 数据库中的数据流。得益于 mermaid，生成的流程图不仅美观，也便于直接添加到 markdown 文档中。

<div id="xeus-clickhouse">
  ### xeus-clickhouse
</div>

[xeus-clickhouse](https://github.com/wangfenjin/xeus-clickhouse) 是 ClickHouse 的一个 Jupyter 内核，支持在 Jupyter 中使用 SQL 查询 ClickHouse 数据。

<div id="mindsdb">
  ### MindsDB Studio
</div>

[MindsDB](https://mindsdb.com/) 是一个面向包括 ClickHouse 在内各类数据库的开源 AI 层，让你能够轻松开发、训练和部署最先进的机器学习模型。MindsDB Studio(GUI) 可让你基于数据库训练新模型、解读模型生成的预测结果、识别潜在的数据偏差，并借助可解释 AI 功能评估和可视化模型准确性，从而更快地适配和优化你的机器学习模型。

<div id="dbm">
  ### DBM
</div>

[DBM](https://github.com/devlive-community/dbm) 是一款面向 ClickHouse 的可视化管理工具！

功能：

* 支持查询历史 (分页、全部清除等) 
* 支持按选定的 SQL 子句进行查询
* 支持终止查询
* 支持表管理 (元数据、删除、预览) 
* 支持数据库管理 (删除、创建) 
* 支持自定义查询
* 支持管理多个数据源 (连接测试、监控) 
* 支持监控 (处理器、连接、查询) 
* 支持数据迁移

<div id="bytebase">
  ### Bytebase
</div>

[Bytebase](https://bytebase.com) 是一款面向团队的 Web 开源 schema 变更与版本控制工具，支持包括 ClickHouse 在内的多种数据库。

特性：

* 开发者与 DBA 之间的 schema 审核。
* Database-as-Code，在 GitLab 等 VCS 中对 schema 进行版本控制，并在代码提交后触发部署。
* 结合按环境划分的 policy，简化部署流程。
* 完整的 migration 历史。
* schema 漂移检测。
* 备份与恢复。
* RBAC。

<div id="zeppelin-interpreter-for-clickhouse">
  ### Zeppelin-Interpreter-for-ClickHouse
</div>

[Zeppelin-Interpreter-for-ClickHouse](https://github.com/SiderZhang/Zeppelin-Interpreter-for-ClickHouse) 是一个适用于 ClickHouse 的 [Zeppelin](https://zeppelin.apache.org) 解释器。与 JDBC 解释器相比，它能为长时间运行的查询提供更好的超时控制。

<div id="clickcat">
  ### ClickCat
</div>

[ClickCat](https://github.com/clickcat-project/ClickCat) 是一个易用的用户界面，让您能够搜索、浏览并可视化 ClickHouse 数据。

功能：

* 在线 SQL 编辑器，无需安装即可运行 SQL 代码。
* 您可以查看所有进程和变更操作。对于尚未完成的进程，可以在 UI 中将其终止。
* 指标包括集群分析、数据分析和查询分析。

<div id="clickvisual">
  ### ClickVisual
</div>

[ClickVisual](https://clickvisual.net/) ClickVisual 是一个轻量级开源日志查询、分析与告警可视化平台。

功能：

* 支持一键创建日志分析库
* 支持日志采集配置管理
* 支持自定义索引配置
* 支持告警配置
* 支持细粒度到库和表级别的权限配置

<div id="clickmate">
  ### ClickHouse-Mate
</div>

[ClickHouse-Mate](https://github.com/metrico/clickhouse-mate) 是一个基于 Angular 的 Web 客户端和用户界面，用于在 ClickHouse 中搜索和浏览数据。

功能：

* ClickHouse SQL 查询自动补全
* 快速导航数据库和表树
* 高级结果筛选与排序
* 内嵌的 ClickHouse SQL 文档
* 查询预设和历史记录
* 100% 基于浏览器，无需 server/backend

可通过 GitHub Pages 立即使用该客户端：https://metrico.github.io/clickhouse-mate/

<div id="uptrace">
  ### Uptrace
</div>

[Uptrace](https://github.com/uptrace/uptrace) 是一款 APM 工具，基于 OpenTelemetry 和 ClickHouse，提供分布式链路追踪和指标能力。

功能：

* [OpenTelemetry 链路追踪](https://uptrace.dev/opentelemetry/distributed-tracing.html)、指标和日志。
* 通过 AlertManager 发送 Email/Slack/PagerDuty 通知。
* 可用于聚合 spans 的类 SQL 查询语言。
* 用于查询指标的类 PromQL 语言。
* 预置的指标仪表盘。
* 通过 YAML 配置支持多用户/多项目。

<div id="clickhouse-monitoring">
  ### clickhouse-monitoring
</div>

[clickhouse-monitoring](https://github.com/duyet/clickhouse-monitoring) 是一个简单的 Next.js 仪表板，依托 `system.*` 表来帮助监控您的 ClickHouse 集群并提供整体概览。

功能：

* 查询监控：当前查询、查询历史、查询资源 (内存、已读取的 parts、file&#95;open 等) 、开销最高的查询、最常使用的表或列等。
* 集群监控：总内存/CPU 使用量、分布式队列、全局设置、MergeTree 设置、指标等。
* 表和 parts 信息：大小、行数、压缩、part 大小等，并可细化到列级别。
* 实用工具：ZooKeeper 数据探索、查询 EXPLAIN、终止查询等。
* 指标可视化图表：查询和资源使用量、合并/变更次数、合并性能、查询性能等。

<div id="ckibana">
  ### CKibana
</div>

[CKibana](https://github.com/TongchengOpenSource/ckibana) 是一款轻量级服务，让你能够通过原生 Kibana UI 轻松搜索、探索并可视化 ClickHouse 数据。

功能：

* 将原生 Kibana UI 发出的图表请求转换为 ClickHouse 查询语法。
* 支持采样和缓存等高级功能，以提升查询性能。
* 尽可能降低用户从 ElasticSearch 迁移到 ClickHouse 后的学习成本。

<div id="telescope">
  ### Telescope
</div>

[Telescope](https://iamtelescope.net/) 是一个现代化的 Web 界面，用于浏览存储在 ClickHouse 中的日志。它提供了易于使用的 UI，可用于查询、可视化和管理日志数据，并支持细粒度的访问控制。

功能：

* 简洁、响应迅速的 UI，具备强大的筛选功能和可自定义的字段选择。
* FlyQL 语法，支持直观且表达力强的日志筛选。
* 基于时间的图表，支持 group-by，并涵盖嵌套的 JSON、Map 和 Array 字段。
* 可选支持原生 SQL `WHERE` 查询，用于高级筛选 (带权限检查) 。
* 已保存视图：可保存并共享查询和布局的自定义 UI 配置。
* 基于角色的访问控制 (RBAC) 以及 GitHub 身份验证集成。
* 在 ClickHouse 侧无需额外部署任何代理或组件。

[Telescope 源代码](https://github.com/iamtelescope/telescope) · [在线演示](https://demo.iamtelescope.net)

<div id="clicklens">
  ### ClickLens
</div>

[ClickLens](https://ntk148v.github.io/clicklens/) 是一个现代、强大且易用的 Web 界面，用于管理和监控 ClickHouse 数据库。它为开发者、分析师和管理员提供了一套全面的工具，帮助他们高效地与 ClickHouse 集群交互。ClickHouse 是一个出色的分析型数据库，但通过命令行客户端或基础工具来管理它可能并不容易。ClickLens 通过提供以下功能弥补了这一不足：

* Discover - 灵活、类似 Kibana 的任意表数据探索
* SQL 控制台 - 编写、执行和分析查询，支持语法高亮和流式结果
* 实时监控 - 实时关注集群健康状况、查询性能和资源使用情况
* Schema Explorer - 浏览数据库、表、列、parts 等对象
* Access Control - 直接在 UI 中管理用户和角色
* 原生 RBAC - 你的 UI 权限直接来源于 ClickHouse 授权

[ClickLens 源代码](https://github.com/ntk148v/clicklens)

<div id="chouse-ui">
  ### CHouse UI
</div>

[CHouse UI](https://chouse-ui.com) 是一个开源、自托管的 ClickHouse Web 界面，专为**在生产环境中运行 ClickHouse 的团队**打造。大多数工具只把某一项做到极致——查询工作区、仪表板、AI 助手或集群监控；而 CHouse UI 则将这些能力*合而为一*：既有团队访问控制层，也有多集群统一监控，以及一位自治的只读 AI SRE。不同于需要直接提供数据库凭据的客户端，它会在服务端加密存储这些信息，并通过自身的**基于角色的访问控制 (RBAC) **层进行访问管控，因此浏览器永远不会看到 ClickHouse 密码。

功能：

* **团队访问与安全** - 应用级 RBAC (预定义 + 自定义 角色、细粒度的按 database/table 划分的数据访问规则) 、带有真实 session 上下文的审计日志，以及使用 AES-256-GCM 加密的服务端凭据。
* **多集群统一监控** - 在一个面板中查看所有已配置 集群 (status、内存、活动查询、异常、趋势微图) ，每张卡片都独立 polling，并由后端快照轮询器提供支持。
* **Chouse AI — Fleet Doctor** - 一个自治的只读 AI SRE：它使用受限的、仅允许 `system.*` 的 `SELECT` 工具 (ClickHouse `readonly=1`) 扫描整个集群群组，定位根本原因，并生成结构化报告，其中包含高开销查询深度分析和建议的 rewrites。它绝不会修改 集群。
* **监控选项卡中的 AI** - 在 Query Logs 的某一行上使用“使用 Chouse AI 优化” (rewrite + 前后 `EXPLAIN` 估算对比 + 在 SQL 工作区中打开) ，以及在 `system.errors` 某一行或某个 part-log 条目上一键“诊断”。
* **阈值告警** - 节点内存百分比、单查询内存和长时间运行查询规则，可发送到 Slack 和电子邮件——并且在超出阈值时附带自治式根因分析。
* **完整工作区** - Monaco SQL 编辑器、schema 浏览器、支持终止查询的实时查询视图、ClickHouse 原生监控 (内存明细、parts/merges、副本延迟、延迟百分位数) 以及数据导入/导出。

开源 (Apache 2.0) ，优先面向本地部署——所有功能开箱即用，没有付费版本。

[CHouse UI 源代码](https://github.com/daun-gatal/chouse-ui)

<div id="clickhouse-flow">
  ### clickhouse-flow
</div>

[clickhouse-flow](https://github.com/MikeAmputer/clickhouse-flow) 是一款开源工具，用于可视化 ClickHouse 表、视图和 materialized views 之间的数据流和依赖关系。

特性：

* 根据 ClickHouse 元数据自动构建 schema 图。
* 可视化经由 materialized views 的数据流。
* 提供用于探索 schema 结构的交互式 UI。
* 可将图表导出为 PDF 或 SVG，便于编写文档和共享。
* 提供基于 Docker 的部署方式，便于在开发环境中快速完成设置。

<div id="commercial">
  ## 商业版
</div>

<div id="datagrip">
  ### DataGrip
</div>

[DataGrip](https://www.jetbrains.com/datagrip/) 是 JetBrains 推出的数据库 IDE，专门支持 ClickHouse。它还内置于其他基于 IntelliJ 的工具中，例如 PyCharm、IntelliJ IDEA、GoLand、PhpStorm 等。

功能：

* 代码自动补全非常快。
* ClickHouse 语法高亮。
* 支持 ClickHouse 特有的功能，例如嵌套列、表引擎。
* 数据编辑器。
* 重构。
* 搜索和导航。

<div id="yandex-datalens">
  ### Yandex DataLens
</div>

[Yandex DataLens](https://yandex.cloud/en/services/datalens) 是一项数据可视化和分析服务。

功能：

* 提供丰富的可视化形式，从简单的柱状图到复杂的仪表盘。
* 仪表盘可以公开分享。
* 支持包括 ClickHouse 在内的多种数据源。
* 提供基于 ClickHouse 的 materialized 数据存储。

对于低负载项目，DataLens 可[免费使用](https://yandex.cloud/en/docs/datalens/pricing)，甚至支持商业用途。

* [DataLens 文档](https://yandex.cloud/en/docs/datalens/)。
* [教程](https://yandex.cloud/en/docs/solutions/datalens/data-from-ch-visualization)：如何将 ClickHouse 数据库中的数据可视化。

<div id="holistics-software">
  ### Holistics Software
</div>

[Holistics](https://www.holistics.io/) 是一个全栈数据平台和商业智能工具。

功能：

* 支持通过电子邮件、Slack 和 Google 表格按计划自动发送报告。
* SQL 编辑器，支持可视化、版本控制、自动补全、可复用的查询组件和动态过滤器。
* 通过 iframe 嵌入报告和仪表盘分析功能。
* 具备数据准备和 ETL 能力。
* 支持用于数据关系映射的 SQL 数据建模。

<div id="looker">
  ### Looker
</div>

[Looker](https://looker.com) 是一个数据平台和商业智能工具，支持包括 ClickHouse 在内的 50 多种数据库方言。Looker 提供 SaaS 平台和自托管两种部署方式。用户可以通过浏览器使用 Looker 探索数据、构建可视化和仪表盘、设置定时报表，并与同事分享洞察。Looker 还提供了一套丰富的工具，可将这些功能嵌入其他应用程序，并通过 API
将数据与其他应用程序集成。

功能：

* 使用 LookML 轻松灵活地进行开发。LookML 是一种支持经过整理的
  [数据建模](https://looker.com/platform/data-modeling) 的语言，可为报表编写者和最终用户提供支持。
* 通过 Looker 的 [Data Actions](https://looker.com/platform/actions) 实现强大的工作流集成。

[如何在 Looker 中配置 ClickHouse。](https://docs.looker.com/setup-and-management/database-config/clickhouse)

<div id="seektable">
  ### SeekTable
</div>

[SeekTable](https://www.seektable.com) 是一款用于数据探索和运营报表的自助式 BI 工具，同时提供云服务和自托管版本。SeekTable 的报表可嵌入任何 Web 应用。

功能：

* 面向业务用户的易用型报表构建器。
* 强大的报表参数，支持 SQL 筛选和报表专用的查询自定义。
* 可通过 native TCP/IP 端点和 HTTP(S) 接口连接到 ClickHouse (两种不同的驱动) 。
* 可在维度/度量定义中充分使用 ClickHouse SQL dialect 的全部能力。
* 用于自动生成报表的 [Web API](https://www.seektable.com/help/web-api-integration)。
* 支持通过账户数据的[备份/恢复](https://www.seektable.com/help/self-hosted-backup-restore)实现报表开发流程；数据模型 (cubes) /报表配置采用便于阅读的 XML 格式，并可存储在版本控制系统中。

SeekTable 对个人用户[免费](https://www.seektable.com/help/cloud-pricing)开放。

[如何在 SeekTable 中配置 ClickHouse 连接。](https://www.seektable.com/help/clickhouse-pivot-table)

<div id="chadmin">
  ### Chadmin
</div>

[Chadmin](https://github.com/bun4uk/chadmin) 是一个简洁的 UI，可用于查看 ClickHouse 集群中当前正在运行的查询及其相关信息，也可以在需要时将其终止。

<div id="tablum_io">
  ### TABLUM.IO
</div>

[TABLUM.IO](https://tablum.io/)——一款用于 ETL 和可视化的在线查询与分析工具。它支持连接到 ClickHouse，通过功能强大的 SQL 控制台查询数据，也可从静态文件和第三方服务加载数据。TABLUM.IO 能将数据结果可视化为图表和表格。

功能：

* ETL：从常见数据库、本地和远程文件以及 API 调用中加载数据。
* 功能强大的 SQL 控制台，支持语法高亮和可视化查询构建器。
* 将数据可视化为图表和表格。
* 数据物化和子查询。
* 将数据报告发送到 Slack、Telegram 或电子邮件。
* 通过专有 API 构建数据管道。
* 以 JSON、CSV、SQL、HTML 格式导出数据。
* 基于 Web 的界面。

TABLUM.IO 可作为自托管解决方案运行 (以 Docker 镜像形式) ，也可在云端运行。
许可证：[商业](https://tablum.io/pricing)产品，提供 3 个月免费试用期。

可免费试用[云端版本](https://tablum.io/try)。
在 [TABLUM.IO](https://tablum.io/) 了解有关该产品的更多信息

<div id="ckman">
  ### CKMAN
</div>

[CKMAN](https://www.github.com/housepower/ckman) 是一款用于管理和监控 ClickHouse 集群的工具！

功能：

* 通过浏览器界面快速便捷地自动部署集群
* 支持对集群进行扩容和缩容
* 实现集群数据的负载均衡
* 在线升级集群
* 可在页面上修改集群配置
* 提供集群节点监控和 ZooKeeper 监控
* 监控表和分区状态，并支持慢 SQL 语句监控
* 提供简洁易用的 SQL 执行页面

<div id="1bench">
  ### 1bench
</div>

[1bench](https://1bench.dev) 是一款面向多种数据库的原生桌面 GUI，并为 ClickHouse 提供一流支持——涵盖服务器概览、schema 管理、向量搜索以及大型结果集浏览。

功能：

* 连接时提供服务器概览——可一目了然地查看 version、运行时间、正在运行的查询、活跃 merge 操作、parts 与存储大小、副本状态，以及集群和节点。
* 可视化查询构建器 (列选择器、过滤器、排序、limit) ，并配有支持语法高亮的 Monaco SQL 编辑器，以及按连接保存的查询历史。
* 可视化 `CREATE TABLE` 向导，支持 `MergeTree` 变体、`ORDER BY`、`PARTITION BY`、`SETTINGS`，以及自动封装 `Nullable()`。
* 原生支持 ClickHouse 类型——`Nullable`、`Array`、`LowCardinality`、嵌套对象。
* 支持向量搜索——将 `Array(Float32)` embedding 列渲染为紧凑的向量单元，支持 2D embedding 可视化，并可通过 `cosineDistance` 查找相似项。
* 支持在结果表中进行行内数据编辑，并可批次保存，同时支持使用 ClickHouse 原生格式导入和导出 CSV/JSON/SQL。
* 连接选项包括：HTTP/HTTPS、用于连接位于防火墙后的私网集群的 SSH 隧道，以及适合安全浏览生产环境的可选只读模式。
* 兼容 ClickHouse Cloud 和自托管部署。