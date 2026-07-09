---
description: 'SYSTEM 语句文档'
sidebar_label: 'SYSTEM'
sidebar_position: 36
slug: /sql-reference/statements/system
title: 'SYSTEM 语句'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="system-statements">
  # SYSTEM 语句
</div>

<div id="reload-embedded-dictionaries">
  ## SYSTEM RELOAD EMBEDDED DICTIONARIES
</div>

重新加载所有[内部字典](./create/dictionary/overview.md)。
默认情况下，内部字典已禁用。
无论内部字典的更新结果如何，始终返回 `Ok.`。

<div id="reload-dictionaries">
  ## SYSTEM RELOAD DICTIONARIES
</div>

`SYSTEM RELOAD DICTIONARIES` 查询会重新加载状态为 `LOADED` 的字典 (参见 [`system.dictionaries`](/zh/operations/system-tables/dictionaries) 的 `status` 列) ，即此前已成功加载过的字典。
默认情况下，字典采用延迟加载 (参见 [dictionaries&#95;lazy&#95;load](../../operations/server-configuration-parameters/settings.md#dictionaries_lazy_load)) ，因此不会在启动时自动加载，而是在首次访问时才会初始化，例如使用 [`dictGet`](/zh/sql-reference/functions/ext-dict-functions#dictGet) 函数，或对 `ENGINE = Dictionary` 的表执行 `SELECT` 时。

**语法**

```sql
SYSTEM RELOAD DICTIONARIES [ON CLUSTER cluster_name]
```

<div id="reload-dictionary">
  ## SYSTEM RELOAD DICTIONARY
</div>

完全重新加载字典 `dictionary_name`，无论该字典当前处于何种状态 (LOADED / NOT&#95;LOADED / FAILED) 。
无论字典更新结果如何，始终返回 `Ok.`。

```sql
SYSTEM RELOAD DICTIONARY [ON CLUSTER cluster_name] dictionary_name
```

可通过查询 `system.dictionaries` 表来查看字典状态。

```sql
SELECT name, status FROM system.dictionaries;
```

<div id="reload-models">
  ## SYSTEM RELOAD MODELS
</div>

:::note
此语句和 `SYSTEM RELOAD MODEL` 仅会从 clickhouse-library-bridge 中卸载 catboost 模型。函数 `catboostEvaluate()`
如果模型尚未加载，会在首次访问时将其加载。
:::

卸载所有 CatBoost 模型。

**语法**

```sql
SYSTEM RELOAD MODELS [ON CLUSTER cluster_name]
```

<div id="reload-model">
  ## SYSTEM RELOAD MODEL
</div>

卸载 `model_path` 处的 CatBoost 模型。

**语法**

```sql
SYSTEM RELOAD MODEL [ON CLUSTER cluster_name] <model_path>
```

<div id="reload-functions">
  ## SYSTEM RELOAD FUNCTIONS
</div>

从配置文件中重新加载所有已注册的[可执行用户自定义函数](/zh/sql-reference/functions/udf#executable-user-defined-functions)，或重新加载其中一个。

**语法**

```sql
SYSTEM RELOAD FUNCTIONS [ON CLUSTER cluster_name]
SYSTEM RELOAD FUNCTION [ON CLUSTER cluster_name] function_name
```

<div id="reload-asynchronous-metrics">
  ## SYSTEM RELOAD ASYNCHRONOUS METRICS
</div>

重新计算所有[异步指标](../../operations/system-tables/asynchronous_metrics.md)。由于异步指标会根据设置 [asynchronous&#95;metrics&#95;update&#95;period&#95;s](../../operations/server-configuration-parameters/settings.md) 定期更新，因此通常无需使用此语句手动更新。

```sql
SYSTEM RELOAD ASYNCHRONOUS METRICS [ON CLUSTER cluster_name]
```

<div id="drop-dns-cache">
  ## SYSTEM CLEAR|DROP DNS CACHE
</div>

清除 ClickHouse 的内部 DNS 缓存。有时 (对于较旧版本的 ClickHouse) ，在基础设施发生变化时 (例如更改另一台 ClickHouse 服务器或字典所使用服务器的 IP 地址) ，需要使用此命令。

如需更便捷的 (自动) 缓存管理，请参见 `disable_internal_dns_cache`、`dns_cache_max_entries`、`dns_cache_update_period` 参数。

<div id="drop-mark-cache">
  ## SYSTEM CLEAR|DROP MARK CACHE
</div>

清空标记缓存。

<div id="drop-primary-index-cache">
  ## SYSTEM CLEAR|DROP PRIMARY INDEX CACHE
</div>

清除主索引缓存。该缓存会在内存中保存 [`MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md) 表的主键。
其大小由服务器级别的设置 [`primary_index_cache_size`](../../operations/server-configuration-parameters/settings.md#primary_index_cache_size) 控制。

<div id="drop-iceberg-metadata-cache">
  ## SYSTEM CLEAR|DROP ICEBERG METADATA CACHE
</div>

清空 Iceberg 元数据缓存。

<div id="drop-avro-schema-cache">
  ## SYSTEM CLEAR|DROP AVRO SCHEMA CACHE
</div>

清除 `AvroConfluent` 格式使用的按 URL 划分的 Confluent Schema Registry 缓存。此操作会删除 schema 拉取缓存 (id → schema) 和 schema 注册缓存 (subject + schema → id) ，因此后续读写都会回退到 registry 服务器。当 registry 侧的 schema 被删除或重写时，或者需要在测试中验证 registry 的幂等性时，此命令会很有用。

<div id="drop-parquet-metadata-cache">
  ## SYSTEM DROP PARQUET METADATA CACHE
</div>

清空 Parquet 元数据缓存。

<div id="drop-point-in-polygon-cache">
  ## SYSTEM CLEAR|DROP POINT IN POLYGON CACHE
</div>

清除函数 [`pointInPolygon`](../functions/geo/coordinates.md#pointinpolygon) 使用的预处理常量多边形缓存。已配置的大小上限 (服务器设置 `point_in_polygon_cache_size`) 保持不变，因此该缓存之后仍会继续接收条目。若要禁用该缓存，请将 `point_in_polygon_cache_size` 设置为 `0`。

<div id="drop-text-index-caches">
  ## SYSTEM CLEAR|DROP TEXT INDEX CACHES
</div>

清除文本索引的标记缓存、头部缓存和 postings 缓存。

如果你想单独清除其中某个缓存，可以运行

* `SYSTEM CLEAR TEXT INDEX TOKENS CACHE`,
* `SYSTEM CLEAR TEXT INDEX HEADER CACHE`，或
* `SYSTEM CLEAR TEXT INDEX POSTINGS CACHE`

<div id="drop-index-mark-cache">
  ## SYSTEM CLEAR|DROP INDEX MARK CACHE
</div>

清除次级 (数据跳过) 索引的索引标记缓存。

<div id="drop-index-uncompressed-cache">
  ## SYSTEM CLEAR|DROP INDEX UNCOMPRESSED CACHE
</div>

清除次级 (数据跳过) 索引的未压缩块缓存。

<div id="drop-mmap-cache">
  ## SYSTEM CLEAR|DROP MMAP CACHE
</div>

清空内存映射文件缓存。

<div id="drop-page-cache">
  ## SYSTEM CLEAR|DROP PAGE CACHE
</div>

清除用户态页缓存，即 ClickHouse 自有的内存中缓存，用于缓存从底层存储读取的数据。

<div id="drop-vector-similarity-index-cache">
  ## SYSTEM CLEAR|DROP VECTOR SIMILARITY INDEX CACHE
</div>

清空向量相似度索引缓存。

<div id="drop-connections-cache">
  ## SYSTEM CLEAR|DROP CONNECTIONS CACHE
</div>

清除用于出站连接的 HTTP 连接池缓存。

<div id="drop-s3-client-cache">
  ## SYSTEM CLEAR|DROP S3 CLIENT CACHE
</div>

清除 S3 客户端缓存。

<div id="prewarm-mark-cache">
  ## SYSTEM PREWARM MARK CACHE
</div>

将表的标记预加载到 [标记缓存](#drop-mark-cache) 中。还会将次级索引标记预加载到 [index 标记缓存](#drop-index-mark-cache) 中。

```sql
SYSTEM PREWARM MARK CACHE [ON CLUSTER cluster_name] [db.]table
```

<div id="prewarm-primary-index-cache">
  ## SYSTEM PREWARM PRIMARY INDEX CACHE
</div>

将 `MergeTree` 表的主索引预加载到[主索引缓存](#drop-primary-index-cache)中。

```sql
SYSTEM PREWARM PRIMARY INDEX CACHE [ON CLUSTER cluster_name] [db.]table
```

<div id="drop-disk-metadata-cache">
  ## SYSTEM CLEAR|DROP DISK METADATA CACHE
</div>

清除指定磁盘的元数据缓存。

```sql
SYSTEM DROP DISK METADATA CACHE <disk_name>
```

<div id="sync-filesystem-cache">
  ## SYSTEM SYNC FILESYSTEM CACHE
</div>

使 ClickHouse 文件系统缓存的内存状态与磁盘上实际存在的缓存文件同步，并返回每个已缓存 File 段的 `cache_name`、`path` 以及已下载的 `size`。可选的缓存名称可将该操作限制为单个缓存。

```sql
SYSTEM SYNC FILESYSTEM CACHE ['<cache_name>']
```

<div id="drop-distributed-cache">
  ## SYSTEM CLEAR|DROP DISTRIBUTED CACHE
</div>

:::note
`SYSTEM CLEAR|DROP DISTRIBUTED CACHE` 仅在 ClickHouse Cloud 中可用。
:::

清除分布式缓存。使用 `CONNECTIONS` 可仅清除到分布式缓存服务器的缓存连接，或者传入服务器标识符以指定单个服务器。

```sql
SYSTEM DROP DISTRIBUTED CACHE [CONNECTIONS | 'server_id']
```

<div id="drop-replica">
  ## SYSTEM DROP REPLICA
</div>

可以使用以下语法删除 `ReplicatedMergeTree` 表的死亡副本：

```sql
SYSTEM DROP REPLICA 'replica_name' FROM TABLE database.table;
SYSTEM DROP REPLICA 'replica_name' FROM DATABASE database;
SYSTEM DROP REPLICA 'replica_name';
SYSTEM DROP REPLICA 'replica_name' FROM ZKPATH '/path/to/table/in/zk';
```

该查询会删除 ZooKeeper 中 `ReplicatedMergeTree` 的副本路径。当某个副本已失效，且由于对应的表已不存在，无法再通过 `DROP TABLE` 从 ZooKeeper 中删除其元数据时，这个操作就很有用。它只会删除非活动/过期副本，不能删除本地副本；如需删除本地副本，请使用 `DROP TABLE`。`DROP REPLICA` 不会删除任何表，也不会从磁盘中移除任何数据或元数据。

第一种会删除 `database.table` 表中 `'replica_name'` 副本的元数据。
第二种会对该数据库中的所有复制表执行相同操作。
第三种会对本地服务器上的所有复制表执行相同操作。
第四种适用于在某个表的其他所有副本都已被删除时，移除失效副本的元数据。它要求显式指定表路径。该路径必须与创建表时传给 `ReplicatedMergeTree` engine 第一个参数的路径相同。

<div id="drop-database-replica">
  ## SYSTEM DROP DATABASE REPLICA
</div>

`Replicated` 数据库的失效副本可使用以下语法删除：

```sql
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM DATABASE database;
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'];
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM ZKPATH '/path/to/table/in/zk';
```

与 `SYSTEM DROP REPLICA` 类似，但当没有可供执行 `DROP DATABASE` 的数据库时，它会从 ZooKeeper 中删除 `Replicated` 数据库副本的路径。请注意，它不会删除 `ReplicatedMergeTree` 的副本 (因此你可能还需要使用 `SYSTEM DROP REPLICA`) 。分片和副本名称就是创建数据库时在 `Replicated` engine 的 arguments 中指定的名称。此外，这些名称也可以从 `system.clusters` 的 `database_shard_name` 和 `database_replica_name` 列中获取。如果缺少 `FROM SHARD` clause，那么 `replica_name` 必须是完整的副本名称，格式为 `shard_name|replica_name`。

<div id="drop-uncompressed-cache">
  ## SYSTEM CLEAR|DROP UNCOMPRESSED CACHE
</div>

清除未压缩数据缓存。
未压缩数据缓存可通过查询/用户/profile 级设置 [`use_uncompressed_cache`](../../operations/settings/settings.md#use_uncompressed_cache) 启用或禁用。
其大小可通过服务器级设置 [`uncompressed_cache_size`](../../operations/server-configuration-parameters/settings.md#uncompressed_cache_size) 进行配置。

<div id="drop-compiled-expression-cache">
  ## SYSTEM CLEAR|DROP COMPILED EXPRESSION CACHE
</div>

清除已编译表达式缓存。
可通过查询/用户/profile 级别的设置 [`compile_expressions`](../../operations/settings/settings.md#compile_expressions) 启用或禁用已编译表达式缓存。

<div id="drop-query-condition-cache">
  ## SYSTEM CLEAR|DROP QUERY CONDITION CACHE
</div>

清除查询条件缓存。

<div id="drop-query-cache">
  ## SYSTEM CLEAR|DROP 查询缓存
</div>

```sql
SYSTEM CLEAR QUERY CACHE;
SYSTEM CLEAR QUERY CACHE TAG '<tag>'
```

清除[查询缓存](../../operations/query-cache.md)。
如果指定了标签，则仅删除查询缓存中带有该标签的条目。

<div id="system-drop-schema-format">
  ## SYSTEM CLEAR|DROP FORMAT SCHEMA CACHE
</div>

清除从 [`format_schema_path`](../../operations/server-configuration-parameters/settings.md#format_schema_path) 加载的 schema 缓存。

支持的目标：

* Protobuf：从内存中移除已导入的 Protobuf Message 定义。
* Files：删除本地缓存于 [`format_schema_path`](../../operations/server-configuration-parameters/settings.md#format_schema_path) 中的 schema file，这些文件会在 `format_schema_source` 设置为 `query` 时生成。
  注意：如果未指定目标，则会同时清除这两种缓存。

```sql
SYSTEM CLEAR|DROP FORMAT SCHEMA CACHE [FOR Protobuf/Files]
```

<div id="flush-logs">
  ## SYSTEM FLUSH LOGS
</div>

将缓冲的日志消息刷写到系统表中，例如 `system.query_log`。由于大多数系统表默认的刷写间隔为 7.5 秒，因此该语句主要用于调试。
即使消息队列为空，也会创建系统表。

```sql
SYSTEM FLUSH LOGS [ON CLUSTER cluster_name] [log_name|[database.table]] [, ...]
```

如果你不想全部刷新，可以通过传入日志名称或其目标表来刷新一个或多个单独的日志：

```sql
SYSTEM FLUSH LOGS query_log, system.query_views_log;
```

<div id="reload-config">
  ## SYSTEM RELOAD CONFIG
</div>

重新加载 ClickHouse 配置。用于配置存储在 ZooKeeper 中时。请注意，`SYSTEM RELOAD CONFIG` 不会重新加载存储在 ZooKeeper 中的 `USER` 配置；它只会重新加载存储在 `users.xml` 中的 `USER` 配置。要重新加载所有 `USER` 配置，请使用 `SYSTEM RELOAD USERS`

```sql
SYSTEM RELOAD CONFIG [ON CLUSTER cluster_name]
```

<div id="reload-users">
  ## SYSTEM RELOAD USERS
</div>

重新加载所有访问存储，包括：users.xml、本地磁盘访问存储，以及存储在 ZooKeeper 中的复制访问存储。

```sql
SYSTEM RELOAD USERS [ON CLUSTER cluster_name]
```

<div id="shutdown">
  ## SYSTEM SHUTDOWN
</div>

<CloudNotSupportedBadge />

正常关闭 ClickHouse (类似于 `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`)

<div id="kill">
  ## SYSTEM KILL
</div>

终止 ClickHouse 进程 (例如 `kill -9 {$ pid_clickhouse-server}`)

<div id="instrument">
  ## SYSTEM INSTRUMENT
</div>

使用 LLVM 的 XRay 功能管理插桩点；该功能仅在以 `ENABLE_XRAY=1` 构建 ClickHouse 时可用。
这样无需修改源代码，即可在生产环境中进行调试和性能分析，并且额外开销极小。
在未添加任何插桩点时，性能损耗几乎可以忽略不计，因为它只会在那些长度超过 200 条指令的函数入口和出口处，额外添加一次跳转到附近地址的指令。

<div id="instrument-add">
  ### SYSTEM INSTRUMENT ADD
</div>

添加一个新的插桩点。已插桩的函数可在 [`system.instrumentation`](../../operations/system-tables/instrumentation.md) 系统表中查看。对于同一个函数，可以添加多个 handler，它们会按添加插桩的顺序依次执行。
要进行插桩的函数可从 [`system.symbols`](../../operations/system-tables/symbols.md) 系统表中获取。

可添加到函数上的 handler 共有三种类型：

**语法**

```sql
SYSTEM INSTRUMENT ADD FUNCTION HANDLER [ARGUMENTS]
```

其中，`FUNCTION` 可以是任意函数，也可以是某个函数的子串，例如 `QueryMetricLog::startQuery`；`handler` 则为以下之一

<div id="instrument-add-log">
  #### LOG
</div>

在函数的 `ENTRY` 或 `EXIT` 时打印作为参数提供的文本以及堆栈跟踪。

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG ENTRY 'this is a log printed at entry'
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG EXIT 'this is a log printed at exit'
```

<div id="instrument-add-sleep">
  #### SLEEP
</div>

在 `ENTRY` 或 `EXIT` 时暂停固定的秒数：

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0.5
```

或者，如需随机取一个服从均匀分布的秒数，请提供以空白字符分隔的最小值和最大值：

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0 1
```

<div id="instrument-add-profile">
  #### PROFILE
</div>

用于衡量函数从 `ENTRY` 到 `EXIT` 之间所耗费的时间。
分析结果存储在 [`system.trace_log`](../../operations/system-tables/trace_log.md) 中，并可转换为
[Chrome Event Trace 格式](../../operations/system-tables/trace_log.md#chrome-event-trace-format)。

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' PROFILE
```

<div id="instrument-remove">
  ### SYSTEM INSTRUMENT REMOVE
</div>

可通过以下方式移除单个插桩点：

```sql
SYSTEM INSTRUMENT REMOVE ID
```

使用 `ALL` 关键字选择全部：

```sql
SYSTEM INSTRUMENT REMOVE ALL
```

子查询返回的一组 ID：

```sql
SYSTEM INSTRUMENT REMOVE (SELECT id FROM system.instrumentation WHERE handler = 'log')
```

或与给定 function&#95;name 匹配的所有插桩点：

```sql
SYSTEM INSTRUMENT REMOVE 'QueryMetricLog::startQuery'
```

插桩点信息可从 [`system.instrumentation`](../../operations/system-tables/instrumentation.md) 系统表中获取。

<div id="managing-distributed-tables">
  ## 管理分布式表
</div>

ClickHouse 可以管理[分布式](../../engines/table-engines/special/distributed.md)表。当用户向这些表插入数据时，ClickHouse 会先创建一个待发送到集群节点的数据队列，然后再异步发送这些数据。你可以使用 [`STOP DISTRIBUTED SENDS`](#stop-distributed-sends)、[FLUSH DISTRIBUTED](#flush-distributed) 和 [`START DISTRIBUTED SENDS`](#start-distributed-sends) 查询来管理队列的处理过程。你也可以通过 [`distributed_foreground_insert`](../../operations/settings/settings.md#distributed_foreground_insert) 设置，以同步方式插入分布式数据。

<div id="stop-distributed-sends">
  ### SYSTEM STOP DISTRIBUTED SENDS
</div>

禁用在向分布式表插入数据时进行的后台数据分发。

```sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

:::note
如果启用了 [`prefer_localhost_replica`](../../operations/settings/settings.md#prefer_localhost_replica) (默认启用) ，数据仍然会插入到本地分片。
:::

<div id="flush-distributed">
  ### SYSTEM FLUSH DISTRIBUTED
</div>

强制 ClickHouse 以同步方式将数据发送到集群节点。如果任一节点不可用，ClickHouse 就会抛出异常并停止执行查询。你可以反复重试该查询，直到成功；当所有节点都恢复在线后，查询就会成功。

你也可以通过 `SETTINGS` 子句覆盖某些设置，这有助于绕过一些临时限制，例如 `max_concurrent_queries_for_all_users` 或 `max_memory_usage`。

```sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name> [ON CLUSTER cluster_name] [SETTINGS ...]
```

:::note
每个待处理的块都会按照初始 INSERT 查询中的设置存储到磁盘上，因此有时你可能需要覆盖这些设置。
:::

<div id="start-distributed-sends">
  ### SYSTEM START DISTRIBUTED SENDS
</div>

启用向分布式表插入数据时的后台数据分发。

```sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

<div id="stop-listen">
  ### SYSTEM STOP LISTEN
</div>

关闭套接字，并通过指定协议在指定端口上优雅地终止与服务器的现有连接。

但是，如果未在 clickhouse-server 配置中指定相应的协议设置，此命令将不会生效。

```sql
SYSTEM STOP LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

* 如果指定了 `CUSTOM 'protocol'` 修饰符，则会停止在服务器配置的 protocols 部分中定义、名称为指定值的自定义协议。
* 如果指定了 `QUERIES ALL [EXCEPT .. [,..]]` 修饰符，则会停止所有协议，除非在 `EXCEPT` 子句中指定了例外。
* 如果指定了 `QUERIES DEFAULT [EXCEPT .. [,..]]` 修饰符，则会停止所有默认协议，除非在 `EXCEPT` 子句中指定了例外。
* 如果指定了 `QUERIES CUSTOM [EXCEPT .. [,..]]` 修饰符，则会停止所有自定义协议，除非在 `EXCEPT` 子句中指定了例外。

<div id="start-listen">
  ### SYSTEM START LISTEN
</div>

允许在指定协议上建立新的连接。

但是，如果指定端口和协议上的服务器并非通过 SYSTEM STOP LISTEN 命令停止的，则此命令不会起作用。

```sql
SYSTEM START LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

<div id="managing-mergetree-tables">
  ## 管理 MergeTree 表
</div>

ClickHouse 可管理 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 表中的后台进程。

<div id="stop-merges">
  ### SYSTEM STOP MERGES
</div>

<CloudNotSupportedBadge />

可停止 MergeTree 家族中的表的后台合并操作：

```sql
SYSTEM STOP MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

:::note
对表执行 `DETACH / ATTACH` 会为该表启动后台合并，即使此前已停止所有 MergeTree 表的合并也是如此。
:::

<div id="start-merges">
  ### SYSTEM START MERGES
</div>

<CloudNotSupportedBadge />

用于启动 MergeTree 家族中的表的后台合并：

```sql
SYSTEM START MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

<div id="stop-ttl-merges">
  ### SYSTEM STOP TTL MERGES
</div>

<CloudNotSupportedBadge />

可停止对 MergeTree 家族中的表根据 [TTL 表达式](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) 在后台执行的旧数据删除操作：
即使表不存在或表未使用 MergeTree 引擎，也会返回 `Ok.`。当数据库不存在时，则会返回错误：

```sql
SYSTEM STOP TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="start-ttl-merges">
  ### SYSTEM START TTL MERGES
</div>

<CloudNotSupportedBadge />

用于为 MergeTree 家族中的表启动基于 [生存时间 (TTL) 表达式](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) 的后台旧数据删除：
即使表不存在，也会返回 `Ok.`。如果数据库不存在，则会返回错误：

```sql
SYSTEM START TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="stop-moves">
  ### SYSTEM STOP MOVES
</div>

可停止对 MergeTree 家族中的表按 [带有 TO VOLUME 或 TO DISK 子句的 TTL 表达式](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) 执行的后台数据移动：
即使表不存在，也会返回 `Ok.`。当数据库不存在时，会返回错误：

```sql
SYSTEM STOP MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="start-moves">
  ### SYSTEM START MOVES
</div>

可为 MergeTree 家族中的表启动后台数据移动，依据是[包含 TO VOLUME 和 TO DISK 子句的 TTL 表达式](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl)：
即使表不存在，也会返回 `Ok.`。如果数据库不存在，则会返回错误：

```sql
SYSTEM START MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="query_language-system-unfreeze">
  ### SYSTEM UNFREEZE
</div>

从所有磁盘中清除具有指定名称的冻结备份。有关解冻单个 parts 的更多信息，请参见 [ALTER TABLE table&#95;name UNFREEZE WITH NAME ](/zh/sql-reference/statements/alter/partition#unfreeze-partition)

```sql
SYSTEM UNFREEZE WITH NAME <backup_name>
```

<div id="wait-loading-parts">
  ### SYSTEM WAIT LOADING PARTS
</div>

等待，直到表中所有异步加载的数据parts (过期数据parts) 都已加载完成。

```sql
SYSTEM WAIT LOADING PARTS [ON CLUSTER cluster_name] [db.]merge_tree_family_table_name
```

<div id="managing-replicatedmergetree-tables">
  ## 管理 ReplicatedMergeTree 表
</div>

ClickHouse 可以管理 [ReplicatedMergeTree](/zh/engines/table-engines/mergetree-family/replication) 表中与后台复制相关的各类进程。

<div id="stop-fetches">
  ### SYSTEM STOP FETCHES
</div>

<CloudNotSupportedBadge />

可停止对 `ReplicatedMergeTree` 家族中表的已插入 parts 进行后台拉取：
无论表引擎是什么，甚至表或数据库不存在，始终都会返回 `Ok.`。

```sql
SYSTEM STOP FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-fetches">
  ### SYSTEM START FETCHES
</div>

<CloudNotSupportedBadge />

可为 `ReplicatedMergeTree` 家族中的表启动对已插入 parts 的后台拉取：
无论表引擎是什么，甚至表或数据库不存在，也始终返回 `Ok.`。

```sql
SYSTEM START FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-replicated-sends">
  ### SYSTEM STOP REPLICATED SENDS
</div>

可停止 `ReplicatedMergeTree` 家族的表中新插入的 parts 向集群中其他副本执行的后台发送操作：

```sql
SYSTEM STOP REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-replicated-sends">
  ### SYSTEM START REPLICATED SENDS
</div>

可以为 `ReplicatedMergeTree` 家族中的表启动向集群中其他副本发送新插入 parts 的后台任务：

```sql
SYSTEM START REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-replication-queues">
  ### SYSTEM STOP REPLICATION QUEUES
</div>

可停止 `ReplicatedMergeTree` 家族表中存储于 Zookeeper 的复制队列后台任务。可能的后台任务类型包括：合并、拉取、变更，以及带有 ON CLUSTER 子句的 DDL 语句：

```sql
SYSTEM STOP REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-replication-queues">
  ### SYSTEM START REPLICATION QUEUES
</div>

可启动存储在 Zookeeper 中、供 `ReplicatedMergeTree` 家族表使用的复制队列中的后台拉取任务。可能的后台任务类型包括：合并、拉取、变更，以及带有 ON CLUSTER 子句的 DDL 语句：

```sql
SYSTEM START REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-pulling-replication-log">
  ### SYSTEM STOP PULLING REPLICATION LOG
</div>

停止从 `ReplicatedMergeTree` 表的复制日志向复制队列加载新条目。

```sql
SYSTEM STOP PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-pulling-replication-log">
  ### SYSTEM START PULLING REPLICATION LOG
</div>

撤销 `SYSTEM STOP PULLING REPLICATION LOG` 的效果。

```sql
SYSTEM START PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="sync-replica">
  ### SYSTEM SYNC REPLICA
</div>

等待 `ReplicatedMergeTree` 表与集群中的其他副本同步完成，但等待时间不超过 `receive_timeout` 秒。

```sql
SYSTEM SYNC REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name [IF EXISTS] [STRICT | LIGHTWEIGHT [FROM 'srcReplica1'[, 'srcReplica2'[, ...]]] | PULL]
```

运行此语句后，`[db.]replicated_merge_tree_family_table_name` 会将公共复制日志中的命令拉取到自己的复制队列中，然后该查询会一直等待，直到副本处理完所有已拉取的命令。支持以下修饰符：

* 使用 `IF EXISTS` (自 25.6 起可用) 时，如果表不存在，查询也不会报错。这在向集群添加新副本时非常有用：即使该副本已经属于集群配置的一部分，但表仍处于创建和同步过程中，也不会出错。
* 如果指定了 `STRICT` 修饰符，则查询会等待复制队列清空。如果复制队列中持续出现新的条目，`STRICT` 版本可能永远无法成功。
* 如果指定了 `LIGHTWEIGHT` 修饰符，则查询只会等待 `GET_PART`、`ATTACH_PART`、`DROP_RANGE`、`REPLACE_RANGE` 和 `DROP_PART` 条目处理完成。
  此外，`LIGHTWEIGHT` 修饰符还支持可选的 `FROM &#39;srcReplicas&#39;` 子句，其中 &#39;srcReplicas&#39; 是以逗号分隔的源副本名称列表。该扩展允许仅关注来自指定源副本的复制任务，从而实现更有针对性的同步。
* 如果指定了 `PULL` 修饰符，则查询会从 ZooKeeper 拉取新的复制队列条目，但不会等待任何条目被处理。

<div id="sync-database-replica">
  ### SYNC DATABASE REPLICA
</div>

等待指定的[Replicated 数据库](/zh/engines/database-engines/replicated)应用完该数据库 DDL 队列中的所有 schema 变更。

**语法**

```sql
SYSTEM SYNC DATABASE REPLICA replicated_database_name;
```

<div id="restart-replica">
  ### SYSTEM RESTART REPLICA
</div>

可重新初始化 `ReplicatedMergeTree` 表的 Zookeeper 会话状态；该操作会将当前状态与作为事实依据的 Zookeeper 进行比较，并在需要时向 Zookeeper 队列添加任务。
基于 ZooKeeper 数据初始化复制队列的方式，与 `ATTACH TABLE` 语句相同。表会在短时间内无法执行任何操作。

```sql
SYSTEM RESTART REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

<div id="restore-replica">
  ### SYSTEM RESTORE REPLICA
</div>

如果数据[可能]仍然存在，但 ZooKeeper 元数据已丢失，则可恢复副本。

仅适用于只读 `ReplicatedMergeTree` 表。

在以下情况后可执行该查询：

* ZooKeeper 根路径 `/` 丢失。
* 副本路径 `/replicas` 丢失。
* 单个副本路径 `/replicas/replica_name/` 丢失。

副本会附加在本地找到的 parts，并将其信息发送到 ZooKeeper。
如果副本在元数据丢失前已有的 parts 未过期，就不会从其他副本重新拉取它们 (因此，恢复副本并不意味着要通过网络重新下载所有数据) 。

:::note
所有状态的 parts 都会被移动到 `detached/` 文件夹中。数据丢失前处于活动状态的 parts (committed) 会被附加。
:::

<div id="restore-database-replica">
  ### SYSTEM RESTORE DATABASE REPLICA
</div>

如果数据[可能]仍然存在，但 Zookeeper 元数据已丢失，则恢复副本。

**语法**

```sql
SYSTEM RESTORE DATABASE REPLICA repl_db [ON CLUSTER cluster]
```

**示例**

```sql
CREATE DATABASE repl_db
ENGINE=Replicated("/clickhouse/repl_db", shard1, replica1);

CREATE TABLE repl_db.test_table (n UInt32)
ENGINE = ReplicatedMergeTree
ORDER BY n PARTITION BY n % 10;

-- zookeeper_delete_path("/clickhouse/repl_db", recursive=True) <- root loss.

SYSTEM RESTORE DATABASE REPLICA repl_db;
```

**语法**

```sql
SYSTEM RESTORE REPLICA [db.]replicated_merge_tree_family_table_name [ON CLUSTER cluster_name]
```

另一种语法：

```sql
SYSTEM RESTORE REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

**示例**

在多个服务器上创建一个表。当 ZooKeeper 中该副本的元数据丢失时，由于元数据缺失，该表会以只读方式附加。最后一个查询需要在每个副本上执行。

```sql
CREATE TABLE test(n UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/', '{replica}')
ORDER BY n PARTITION BY n % 10;

INSERT INTO test SELECT * FROM numbers(1000);

-- zookeeper_delete_path("/clickhouse/tables/test", recursive=True) <- root loss.

SYSTEM RESTART REPLICA test;
SYSTEM RESTORE REPLICA test;
```

另一种方法：

```sql
SYSTEM RESTORE REPLICA test ON CLUSTER cluster;
```

<div id="restart-replicas">
  ### SYSTEM RESTART REPLICAS
</div>

可为所有 `ReplicatedMergeTree` 表重新初始化 Zookeeper 会话状态；该操作会将当前状态与作为真实来源的 Zookeeper 进行比较，并在需要时向 Zookeeper 队列添加任务

<div id="drop-filesystem-cache">
  ### SYSTEM CLEAR|DROP FILESYSTEM CACHE
</div>

用于清除文件系统缓存。

```sql
SYSTEM CLEAR FILESYSTEM CACHE [ON CLUSTER cluster_name]
```

<div id="sync-file-cache">
  ### SYSTEM SYNC FILE CACHE
</div>

:::note
该操作开销较大，并且存在被滥用的风险。
:::

会执行 sync 系统调用。

```sql
SYSTEM SYNC FILE CACHE [ON CLUSTER cluster_name]
```

<div id="load-primary-key">
  ### SYSTEM LOAD PRIMARY KEY
</div>

加载指定表或所有表的主键。

```sql
SYSTEM LOAD PRIMARY KEY [db.]name
```

```sql
SYSTEM LOAD PRIMARY KEY
```

<div id="unload-primary-key">
  ### SYSTEM UNLOAD PRIMARY KEY
</div>

卸载指定表或所有表的主键。

```sql
SYSTEM UNLOAD PRIMARY KEY [db.]name
```

```sql
SYSTEM UNLOAD PRIMARY KEY
```

<div id="managing-refreshable-materialized-views">
  ## 管理可刷新materialized view
</div>

用于控制[可刷新materialized view](../../sql-reference/statements/create/view.md#refreshable-materialized-view)执行的后台任务的命令

使用它们时，请留意 [`system.view_refreshes`](../../operations/system-tables/view_refreshes.md)。

<div id="stop-view-stop-views">
  ### SYSTEM STOP [REPLICATED] VIEW, STOP VIEWS
</div>

禁用指定视图或所有可刷新materialized view的周期性刷新。如果刷新正在进行，也会一并取消。

如果该视图位于 Replicated 或 Shared 数据库中，`STOP VIEW` 仅影响当前副本，而 `STOP REPLICATED VIEW` 会影响所有副本。

:::note
停止状态不会在服务器重启后保留。重启后，视图将恢复按其已配置的刷新计划执行刷新。
在 Replicated 或 Shared 数据库中，`SYSTEM STOP VIEW` 仅影响当前副本。使用 `SYSTEM STOP REPLICATED VIEW` 可停止所有副本上的刷新。
:::

```sql
SYSTEM STOP VIEW [db.]name
```

```sql
SYSTEM STOP VIEWS
```

<div id="start-view-start-views">
  ### SYSTEM START [REPLICATED] VIEW, START VIEWS
</div>

为指定视图或所有可刷新materialized view启用周期性刷新。不会立即触发刷新。

如果该视图位于 Replicated 或 Shared 数据库中，`START VIEW` 会撤销 `STOP VIEW` 的效果，而 `START REPLICATED VIEW` 会撤销 `STOP REPLICATED VIEW` 的效果。`START VIEW` 还会撤销 `PAUSE VIEW` 的效果。

```sql
SYSTEM START VIEW [db.]name
```

```sql
SYSTEM START VIEWS
```

<div id="pause-view-pause-views">
  ### SYSTEM PAUSE VIEW, PAUSE VIEWS
</div>

禁用指定视图或所有可刷新materialized view的周期性刷新。
与 `SYSTEM STOP VIEW` 不同，`SYSTEM PAUSE VIEW` 不会中断已在进行中的刷新：当前正在运行的刷新会正常完成，只有后续刷新会被阻止。

可使用 `SYSTEM START VIEW` 或 `SYSTEM START VIEWS` 恢复。

:::note
暂停状态不会在服务器重启后保留。重启后，视图将恢复按其已配置的刷新计划执行刷新。
在 Replicated 或 Shared 数据库中，`SYSTEM PAUSE VIEW` 仅影响当前副本。
:::

```sql
SYSTEM PAUSE VIEW [db.]name
```

```sql
SYSTEM PAUSE VIEWS
```

<div id="refresh-view">
  ### SYSTEM REFRESH VIEW
</div>

立即触发对指定视图执行一次计划外刷新。

```sql
SYSTEM REFRESH VIEW [db.]name
```

<div id="wait-view">
  ### SYSTEM WAIT VIEW
</div>

等待正在执行的刷新完成。如果当前没有正在运行的刷新，则立即返回。如果最近一次刷新尝试失败，则会报错。

可在创建新的可刷新materialized view (不带 EMPTY 关键字) 后立即使用，以等待初始刷新完成。

如果该视图位于 Replicated 或 Shared 数据库中，且刷新正在另一个副本上运行，则会等待该刷新完成。

```sql
SYSTEM WAIT VIEW [db.]name
```

<div id="cancel-view">
  ### SYSTEM CANCEL VIEW
</div>

如果当前副本上指定视图正在刷新，则中断并取消该刷新；否则不执行任何操作。

```sql
SYSTEM CANCEL VIEW [db.]name
```

<div id="flush-object-storage-queue">
  ## SYSTEM FLUSH OBJECT STORAGE QUEUE
</div>

阻塞执行，直到指定文件被给定的 [S3Queue](../../engines/table-engines/integrations/s3queue.md) 或 [AzureQueue](../../engines/table-engines/integrations/azure-queue.md) 表处理完成，或永久失败。如果该文件已处理，则立即返回。如果该文件已永久失败 (所有重试均已耗尽) ，则会引发错误。

```sql
SYSTEM FLUSH OBJECT STORAGE QUEUE [db.]table_name PATH 'path'
```