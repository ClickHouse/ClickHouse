---
description: '插入到 `QueryRunner` 表中的记录表示由该引擎在本地或远程集群上以 “发出即忘” 模式执行的查询。'
sidebar_label: 'QueryRunner'
sidebar_position: 55
slug: /engines/table-engines/special/query-runner
title: 'QueryRunner 表引擎'
doc_type: 'reference'
---

<div id="queryrunner-table-engine">
  # QueryRunner 表引擎
</div>

插入到 `QueryRunner` 表中的记录表示要由该引擎执行的查询。
该引擎可用于异步查询执行、对生成的查询进行分批执行、将查询发送到远程集群、基准测试、模糊测试，以及使用影子流量进行测试。

<div id="creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE runner
(
    query String,
    database String,
    settings Map(LowCardinality(String), String)
)
ENGINE = QueryRunner
SETTINGS
    cluster = 'cluster_name',
    shard = '1',
    mode = 'asynchronous',
    threads = 4,
    max_queue_size = 1000
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | INVOKER | NONE }];
```

该表必须使用允许列的子集创建：`query`、`database`、`settings`。
其中，`query` 列是必填项，其他列为可选项。

| 列          | 类型                    | 含义                           |
| ---------- | --------------------- | ---------------------------- |
| `query`    | `String`              | 要执行的查询。                      |
| `database` | `String`              | 该查询的默认数据库。如果为空，则使用服务器的默认数据库。 |
| `settings` | `Map(String, String)` | 应用于该查询的设置。                   |

<div id="engine-settings">
  ## 引擎设置
</div>

| 设置               | 默认值              | 含义                                                                                             |
| ---------------- | ---------------- | ---------------------------------------------------------------------------------------------- |
| `cluster`        | `''`             | 要将查询发送到的集群名称。如果为空，则在本地执行查询。                                                                    |
| `shard`          | `'1'`            | 要将查询发送到的集群分片的从 1 开始的索引；也可以使用 `'random'` 为每个查询随机选择一个分片，或使用 `'all'` 在每个分片上运行每个查询。需要设置 `cluster`。 |
| `mode`           | `'asynchronous'` | 在 `synchronous` 模式下，INSERT 会在插入批次中的所有查询都完成后返回。在 `asynchronous` 模式下，INSERT 会在查询进入队列后立即返回。       |
| `threads`        | `4`              | 执行查询的后台线程数。                                                                                    |
| `max_queue_size` | `1000`           | 队列中允许的最大查询数。当队列已满时，新插入的查询会被丢弃，并记录错误。                                                           |

<div id="details">
  ## 详细信息
</div>

该表只允许 `INSERT` 查询。
这些查询以“发出即忘”的方式执行：如果发生异常，不会重试，
并且 `SELECT` 查询的结果会被丢弃 (保留结果的唯一方式是 `INSERT SELECT`) 。
每个查询是否成功都可以在 `system.query_log` 表中查看；由
此引擎发起的查询会在发起服务器上标记为 `is_internal = 1`。

排队中的查询会保存在内存中，服务器重启后不会保留。服务器关闭时
 (或对该表执行 `DROP`/`DETACH` 时) ，尚未开始的查询会被丢弃。对于
已经在执行的查询，已分发到集群的会被取消，而在本地运行的则会等待其执行完成。

当要运行的查询本身是 `INSERT` 时，其数据必须以内联形式提供——`INSERT ... VALUES (...)`、
`INSERT ... SELECT ...`，或在查询文本中包含数据的 `INSERT ... FORMAT ...`。不支持
从独立数据流接收数据的 `INSERT`。

<div id="local-mode-and-sql-security">
  ## 本地模式与 SQL SECURITY
</div>

如果未设置 `cluster`，查询会在本地服务器上执行。
这些查询以哪个用户身份运行，由 `SQL SECURITY` 子句决定：

* `INVOKER` (默认) ：查询以执行 `INSERT` 的用户身份运行。
* `DEFINER`：查询以指定的 `DEFINER` 用户身份运行。由于插入的查询是任意的，因此向此类表授予 `INSERT` 相当于委托 `DEFINER` 的全部特权。
* `NONE`：查询在没有用户身份的情况下以完全访问权限运行。要求在创建表时具有 `ALLOW_SQL_SECURITY_NONE` 授权。

<div id="cluster-mode">
  ## 集群模式
</div>

指定 `cluster` 设置时，查询会发送到指定的集群。

目标分片由 `shard` 选择：可以是固定的 从 1 开始索引 (默认为 `'1'`) ，也可以是为每个查询随机选择一个分片的 `'random'`，或是在集群的每个分片上都执行每个查询的 `'all'`。分片内的副本则根据 server 的 `load_balancing` 设置来选择。

`database` 列用于将到 remote server 的 connection 的 默认数据库 设置为指定值。由于 默认数据库 对每个 connection 只设置一次，因此每个不同的 `database` 值都会使用各自的 connection pool。该 pool 会在首次使用时创建，并在表的整个 lifetime 内复用。

`DEFINER` 和 `SQL SECURITY` 仅在本地模式下生效，将它们与 `cluster` 设置一起使用会导致错误。在 remote server 上，查询会使用集群 configuration 中的 credentials 进行身份验证，并作为普通的初始查询运行：它们会以 `is_initial_query = 1` 和各自的 `query_id` 记录在 `system.query_log` 中 (不与生成它们的 INSERT 关联) 。在发起查询的 server 上，这些已分发的查询会以 `is_internal = 1` 记录在 `system.query_log` 中。

由于该 engine 会丢弃 query results，因此它始终以 `discard_query_data = 1` 运行已分发的查询，所以 SELECT 查询的结果数据不会通过网络传输 (这会覆盖 `settings` 列中设置的任何 `discard_query_data` 值) 。

<div id="waiting-for-queries-to-finish">
  ## 等待查询结束
</div>

在异步模式下，可使用以下查询进行阻塞，直到迄今为止提交到该表的所有查询都已结束：

```sql
SYSTEM WAIT QUERY RUNNER runner;
```

<div id="example">
  ## 示例
</div>

重放查询日志中最近执行的 `SELECT` 查询：

```sql
INSERT INTO runner (query, database, settings)
SELECT query, current_database, Settings
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query AND NOT is_internal AND query_kind = 'Select'
  AND event_time > now() - INTERVAL 1 HOUR;
```