---
description: 'ClickHouse Keeper HTTP API 和内置 Web 仪表板文档'
sidebar_label: 'Keeper HTTP API'
sidebar_position: 70
slug: /operations/utilities/clickhouse-keeper-http-api
title: 'Keeper HTTP API 和仪表板'
doc_type: 'reference'
---

ClickHouse Keeper 提供 HTTP API 和内置 Web 仪表板，用于监控、健康检查和存储管理。
该接口允许运维人员通过 Web 浏览器或 HTTP 客户端查看 cluster 状态、执行命令以及管理 Keeper 存储。

<div id="configuration">
  ## 配置
</div>

要启用 HTTP API，请在 `keeper_server` 配置中添加 `http_control` 部分：

```xml
<keeper_server>
    <!-- Other keeper_server configuration -->

    <http_control>
        <port>9182</port>
        <!-- <secure_port>9443</secure_port> -->
    </http_control>
</keeper_server>
```

<div id="configuration-options">
  ### 配置选项
</div>

| 设置                                        | 默认值      | 描述                    |
| ----------------------------------------- | -------- | --------------------- |
| `http_control.port`                       | -        | 用于仪表板和 API 的 HTTP 端口  |
| `http_control.secure_port`                | -        | HTTPS 端口 (需要 SSL 配置)  |
| `http_control.readiness.endpoint`         | `/ready` | 就绪探针的自定义路径            |
| `http_control.storage.session_timeout_ms` | `30000`  | 存储 API 操作的会话超时时间      |

<div id="endpoints">
  ## 端点
</div>

<div id="dashboard">
  ### 仪表板
</div>

* **路径**: `/dashboard`
* **方法**: GET
* **说明**: 提供用于监控和管理 Keeper 的内置 Web 仪表板

该仪表板提供：

* 集群状态实时可视化
* 节点监控 (角色、延迟、连接) 
* 存储浏览器
* 命令执行界面

<div id="readiness-probe">
  ### 就绪探针
</div>

* **路径**: `/ready` (可配置) 
* **方法**: GET
* **描述**: 健康检查端点

成功时的响应 (HTTP 200) ：

```json
{
  "status": "ok",
  "details": {
    "role": "leader",
    "hasLeader": true
  }
}
```

<div id="commands-api">
  ### 命令 API
</div>

* **路径**: `/api/v1/commands/{command}`
* **方法**: GET, POST
* **说明**: 执行 Four-Letter Word 命令或 ClickHouse Keeper Client 命令行客户端命令

查询参数：

* `command` - 要执行的命令
* `cwd` - 基于路径的命令的当前工作目录 (默认值：`/`) 

示例：

```bash
# Four-Letter Word command
curl http://localhost:9182/api/v1/commands/stat

# ZooKeeper CLI command
curl "http://localhost:9182/api/v1/commands/ls?command=ls%20'/'&cwd=/"
```

<div id="storage-api">
  ### 存储 API
</div>

* **基础路径**: `/api/v1/storage`
* **说明**: 用于 Keeper 存储操作的 REST API

存储 API 遵循 REST 约定，HTTP 方法表示操作类型：

| 操作 | 路径                                     | 方法     | 状态码 | 说明       |
| -- | -------------------------------------- | ------ | --- | -------- |
| 获取 | `/api/v1/storage/{path}`               | GET    | 200 | 获取节点数据   |
| 列出 | `/api/v1/storage/{path}?children=true` | GET    | 200 | 列出子节点    |
| 检查 | `/api/v1/storage/{path}`               | HEAD   | 200 | 检查节点是否存在 |
| 创建 | `/api/v1/storage/{path}`               | POST   | 201 | 创建新节点    |
| 更新 | `/api/v1/storage/{path}?version={v}`   | PUT    | 200 | 更新节点数据   |
| 删除 | `/api/v1/storage/{path}?version={v}`   | DELETE | 204 | 删除节点     |