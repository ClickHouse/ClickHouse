---
description: '该引擎支持将 ClickHouse 与 NATS 集成，用于发布或订阅消息 subject，并在有新消息时进行处理。'
sidebar_label: 'NATS'
sidebar_position: 140
slug: /engines/table-engines/integrations/nats
title: 'NATS 表引擎'
doc_type: 'guide'
---

该引擎支持将 ClickHouse 与 [NATS](https://nats.io/) 集成。

`NATS` 可用于：

* 发布或订阅消息 subject。
* 在有新消息时进行处理。

<div id="creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = NATS SETTINGS
    nats_url = 'host:port',
    nats_subjects = 'subject1,subject2,...',
    nats_format = 'data_format'[,]
    [nats_schema = '',]
    [nats_num_consumers = N,]
    [nats_queue_group = 'group_name',]
    [nats_secure = false,]
    [nats_max_reconnect = N,]
    [nats_reconnect_wait = N,]
    [nats_server_list = 'host1:port1,host2:port2,...',]
    [nats_skip_broken_messages = N,]
    [nats_max_block_size = N,]
    [nats_flush_interval_ms = N,]
    [nats_username = 'user',]
    [nats_password = 'password',]
    [nats_token = 'clickhouse',]
    [nats_credential_file = '/var/nats_credentials',]
    [nats_startup_connect_tries = 5,]
    [nats_max_rows_per_message = 1,]
    [nats_handle_error_mode = 'default']
```

必选参数：

* `nats_url` – host:port (例如 `localhost:4222`) 。
* `nats_subjects` – NATS 表要订阅/发布的 subject 列表。支持通配符 subject，例如 `foo.*.bar` 或 `baz.>`
* `nats_format` – 消息格式。使用与 SQL `FORMAT` 函数相同的表示法，例如 `JSONEachRow`。更多信息，请参见 [Formats](../../../interfaces/formats.md) 部分。

可选参数：

* `nats_schema` – 如果格式需要 schema 定义，则必须使用此参数。例如，[Cap&#39;n Proto](https://capnproto.org/) 需要提供 schema file 的 path，以及根对象 `schema.capnp:Message` 的名称。
* `nats_stream` – NATS JetStream 中现有 stream 的名称。
* `nats_consumer_name` – NATS JetStream 中现有持久化拉取消费者的名称。
* `nats_num_consumers` – 每个表的消费者数量。默认值：`1`。仅适用于 NATS core；如果单个消费者的吞吐量不足，可以指定更多消费者。
* `nats_queue_group` – NATS 订阅者的 queue group 名称。默认值为表名。
* `nats_max_reconnect` – 已弃用且无任何作用，系统会以 `nats_reconnect_wait` 超时持续执行重连。
* `nats_reconnect_wait` – 每次重连尝试之间等待的毫秒数。默认值：`2000`。
* `nats_server_list` - 用于连接的 server 列表。可指定为连接到 NATS 集群。
* `nats_skip_broken_messages` - 每个块中，NATS 消息 parser 对与 schema 不兼容消息的容忍数量。默认值：`0`。如果 `nats_skip_broken_messages = N`，则引擎会跳过 *N* 条无法解析的 NATS 消息 (一条消息等于一行数据) 。
* `nats_max_block_size` - 为从 NATS 刷新数据而由 poll 收集的行数。默认值：[max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size)。
* `nats_flush_interval_ms` - 刷新从 NATS 读取的数据的超时时间。默认值：[stream&#95;flush&#95;interval&#95;ms](/zh/operations/settings/settings#stream_flush_interval_ms)。
* `nats_username` - NATS 用户名。
* `nats_password` - NATS 密码。
* `nats_token` - NATS 认证 token。
* `nats_credential_file` - NATS credentials 文件的 path。
* `nats_startup_connect_tries` - 启动时的连接尝试次数。默认值：`5`。
* `nats_max_rows_per_message` — 对于基于行的 formats，一条 NATS 消息中可写入的最大行数。 (默认值：`1`) 。
* `nats_handle_error_mode` — 如何处理 NATS 引擎中的错误。Possible values：default (如果无法解析消息，则抛出异常) ，stream (异常消息和原始消息将保存在虚拟列 `_error` 和 `_raw_message` 中) 。

SSL 连接：

如需建立安全连接，请使用 `nats_secure = 1`。
证书验证由 `CLICKHOUSE_NATS_TLS_SECURE` 环境变量控制；
如果证书已过期、为自签名证书、缺失或因其他原因无效，请将 `CLICKHOUSE_NATS_TLS_SECURE=0` 以禁用验证。

写入 NATS 表：

如果表只从一个 subject 读取，任何 insert 都会发布到同一个 subject。
但是，如果表从多个 subject 读取，则需要指定要发布到哪个 subject。
因此，向包含多个 subject 的表中插入数据时，需要设置 `stream_like_engine_insert_queue`。
您可以从该表读取的 subject 中选择一个，并将数据发布到该 subject。例如：

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = NATS
    SETTINGS nats_url = 'localhost:4444',
             nats_subjects = 'subject1,subject2',
             nats_format = 'JSONEachRow';

  INSERT INTO queue
  SETTINGS stream_like_engine_insert_queue = 'subject2'
  VALUES (1, 1);
```

还可以添加 format 设置，以及与 nats 相关的设置。

示例：

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64,
    date DateTime
  ) ENGINE = NATS
    SETTINGS nats_url = 'localhost:4444',
             nats_subjects = 'subject1',
             nats_format = 'JSONEachRow',
             date_time_input_format = 'best_effort';
```

可以通过 ClickHouse 配置文件添加 NATS 服务器配置。
更具体地说，您可以为 NATS 引擎添加密码：

```xml
<nats>
    <user>click</user>
    <password>house</password>
    <token>clickhouse</token>
</nats>
```

<div id="description">
  ## 描述
</div>

`SELECT` 并不特别适合用于读取消息 (调试场景除外) ，因为每条消息只能读取一次。更实用的做法是使用 [materialized views](../../../sql-reference/statements/create/view.md) 创建实时线程。为此，请按以下步骤操作：

1. 使用该 engine 创建一个 NATS 消费者，并将其视为数据 stream。
2. 创建一个具有所需结构的表。
3. 创建一个 materialized view，将来自该 engine 的数据转换后写入先前创建的表。

当 `MATERIALIZED VIEW` 连接到该 engine 后，它就会开始在后台收集数据。这样，你就可以持续从 NATS 接收消息，并使用 `SELECT` 将其转换为所需的 format。
一个 NATS 表可以拥有任意数量的 materialized view。它们不会直接从该表读取数据，而是接收新的记录 (以块的形式) ；通过这种方式，你可以将数据写入多个明细级别不同的表 (带分组聚合和不带分组聚合) 。

示例：

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = NATS
    SETTINGS nats_url = 'localhost:4444',
             nats_subjects = 'subject1',
             nats_format = 'JSONEachRow',
             date_time_input_format = 'best_effort';

  CREATE TABLE daily (key UInt64, value UInt64)
    ENGINE = MergeTree() ORDER BY key;

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT key, value FROM queue;

  SELECT key, value FROM daily ORDER BY key;
```

要停止接收流数据或更改转换逻辑，请分离 materialized view：

```sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

如果要使用 `ALTER` 修改目标表，我们建议先禁用物化视图，以避免目标表与视图数据之间出现不一致。

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_subject` - NATS 消息的 subject。数据类型：`String`。

当 `nats_handle_error_mode='stream'` 时，还会有以下虚拟列：

* `_raw_message` - 未能成功解析的原始消息。数据类型：`Nullable(String)`。
* `_error` - 解析失败时产生的异常消息。数据类型：`Nullable(String)`。

注意：只有在解析过程中发生异常时，才会填充 `_raw_message` 和 `_error` 这两个虚拟列；消息成功解析时，它们始终为 `NULL`。

<div id="data-formats-support">
  ## 数据格式支持
</div>

NATS 引擎支持 ClickHouse 支持的所有[格式](../../../interfaces/formats.md)。
单条 NATS 消息中的行数取决于格式是基于行还是基于块：

* 对于基于行的格式，可通过设置 `nats_max_rows_per_message` 来控制单条 NATS 消息中的行数。
* 对于基于块的格式，无法将块拆分成更小的分片，但单个块中的行数可以通过通用设置 [max&#95;block&#95;size](/zh/operations/settings/settings#max_block_size) 来控制。

<div id="using-jetstream">
  ## 使用 JetStream
</div>

在结合 NATS JetStream 使用 NATS 引擎之前，您必须先创建一个 NATS stream 和一个持久化拉取消费者。为此，例如可以使用 [NATS CLI](https://github.com/nats-io/natscli) package 中的 `nats` utility：

<details>
  <summary>创建 stream</summary>

  ```bash
  $ nats stream add
  ? Stream 名称 stream_name
  ? Subjects stream_subject
  ? 存储 file
  ? 复制 1
  ? 保留策略 Limits
  ? 丢弃策略 Old
  ? Stream 消息数限制 -1
  ? 每个 Subject 的消息数限制 -1
  ? Stream 总大小 -1
  ? 消息 TTL -1
  ? 最大消息大小 -1
  ? 重复跟踪时间窗口 2m0s
  ? 允许消息 Roll-up 否
  ? 允许删除消息 是
  ? 允许清除 subjects 或整个 stream 是
  Stream stream_name 已创建

  已创建的 Stream stream_name 信息 2025-10-03 14:12:51

                  Subjects: stream_subject
                  副本: 1
                   存储: File

  选项:

                 保留策略: Limits
                 确认: true
                 丢弃策略: Old
                 重复窗口: 2m0s
                Direct Get: true
           允许删除消息: true
                允许清除: true
       允许按消息设置 TTL: false
              允许 Rollup: false

  限制:

               最大消息数: unlimited
         每个 Subject 的最大值: unlimited
                最大字节数: unlimited
                  最大时长: unlimited
             最大消息大小: unlimited
               最大消费者数: unlimited

  状态:

                  消息数: 0
                     字节: 0 B
                首个序列: 0
                最后序列: 0
               活跃消费者: 0
  ```
</details>

<details>
  <summary>创建持久化拉取消费者</summary>

  ```bash
  $ nats consumer add
  ? 选择一个 Stream stream_name
  ? 消费者名称 consumer_name
  ? 投递目标（拉取消费者留空） 
  ? 起始策略 (all, new, last, subject, 1h, msg sequence) all
  ? 确认策略 explicit
  ? 重放策略 instant
  ? 按 subjects 过滤 Stream（留空表示全部） 
  ? 允许的最大投递次数 -1
  ? 最大待确认数 0
  ? 仅投递请求头而不包含消息体 否
  ? 添加重试退避策略 否
  已创建 Consumer stream_name > consumer_name 的信息 2025-10-03T14:13:51+03:00

  配置:

                      名称: consumer_name
                 Pull 模式: true
                 投递策略: All
                 Ack 策略: Explicit
                   Ack 等待: 30.00s
                 重放策略: Instant
             最大待确认数: 1,000
           最大等待中的 Pull 数: 512

  状态:

         最后投递的消息: Consumer sequence: 0 Stream sequence: 0
                 确认下限: Consumer sequence: 0 Stream sequence: 0
                 未确认消息: 0 / 最大 1,000
               重新投递的消息: 0
                 未处理的消息: 0
             等待中的 Pull: 0 / 最大 512
  ```
</details>

创建好 stream 和持久化拉取消费者后，我们就可以创建一个使用 NATS 引擎的表。为此，您需要初始化：nats&#95;stream、nats&#95;consumer&#95;name 和 nats&#95;subjects：

```SQL
CREATE TABLE nats_jet_stream (
    key UInt64,
    value UInt64
  ) ENGINE NATS 
    SETTINGS  nats_url = 'localhost:4222',
              nats_stream = 'stream_name',
              nats_consumer_name = 'consumer_name',
              nats_subjects = 'stream_subject',
              nats_format = 'JSONEachRow';
```