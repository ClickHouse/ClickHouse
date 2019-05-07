# 第三方开发的可视化界面

## 开源

### Tabix

ClickHouse Web 界面 [Tabix](https://github.com/tabixio/tabix).

主要功能：

- 浏览器直接连接 ClickHouse，不需要安装其他软件。
- 高亮语法的编辑器。
- 自动命令补全。
- 查询命令执行的图形分析工具。
- 配色方案选项。

[Tabix 文档](https://tabix.io/doc/).


### HouseOps

[HouseOps](https://github.com/HouseOps/HouseOps) 是一个交互式 UI/IDE 工具，可以运行在 OSX, Linux and Windows 平台中。

主要功能：

- 查询高亮语法提示，可以以表格或 JSON 格式查看数据。
- 支持导出 CSV 或 JSON 格式数据。
- 支持查看查询执行的详情，支持 KILL 查询。
- 图形化显示，支持显示数据库中所有的表和列的详细信息。
- 快速查看列占用的空间。
- 服务配置。

以下功能正在计划开发：
- 数据库管理
- 用户管理
- 实时数据分析
- 集群监控
- 集群管理
- 监控副本情况以及 Kafka 引擎表

### LightHouse

[LightHouse](https://github.com/VKCOM/lighthouse) 是ClickHouse的轻量级Web界面。

特征：

 - 包含过滤和元数据的表列表。
 - 带有过滤和排序的表格预览。
 - 只读查询执行。
 
### DBeaver

[DBeaver](https://dbeaver.io/) 具有ClickHouse支持的通用桌面数据库客户端。

特征：

 - 使用语法高亮显示查询开发。
 - 表格预览。
 - 自动完成。

### clickhouse-cli

[clickhouse-cli](https://github.com/hatarist/clickhouse-cli) 是ClickHouse的替代命令行客户端，用Python 3编写。

特征：
 - 自动完成。
 - 查询和数据输出的语法高亮显示。
 - 寻呼机支持数据输出。
 - 自定义PostgreSQL类命令。

## 商业

### DataGrip

[DataGrip](https://www.jetbrains.com/datagrip/) 是JetBrains的数据库IDE，专门支持ClickHouse。 它还嵌入到其他基于IntelliJ的工具中：PyCharm，IntelliJ IDEA，GoLand，PhpStorm等。

特征：

 - 非常快速的代码完成。
 - ClickHouse语法高亮显示。
 - 支持ClickHouse特有的功能，例如嵌套列，表引擎。
 - 数据编辑器。
 - 重构。
 - 搜索和导航。

[来源文章](https://clickhouse.yandex/docs/zh/interfaces/third-party/gui/) <!--hide-->
