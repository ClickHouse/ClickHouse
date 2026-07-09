---
description: '该引擎支持将 ClickHouse 与 RabbitMQ 集成。'
sidebar_label: 'RabbitMQ'
sidebar_position: 170
slug: /engines/table-engines/integrations/rabbitmq
title: 'RabbitMQ 表引擎'
doc_type: 'guide'
---

该引擎可用于将 ClickHouse 与 [RabbitMQ](https://www.rabbitmq.com) 集成。

`RabbitMQ` 可让您：

* 发布或订阅数据流。
* 在流可用后立即进行处理。

<div id="creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = RabbitMQ SETTINGS
    rabbitmq_host_port = 'host:port' [or rabbitmq_address = 'amqp(s)://guest:guest@localhost/vhost'],
    rabbitmq_exchange_name = 'exchange_name',
    rabbitmq_format = 'data_format'[,]
    [rabbitmq_exchange_type = 'exchange_type',]
    [rabbitmq_routing_key_list = 'key1,key2,...',]
    [rabbitmq_secure = 0,]
    [rabbitmq_schema = '',]
    [rabbitmq_num_consumers = N,]
    [rabbitmq_num_queues = N,]
    [rabbitmq_queue_base = 'queue',]
    [rabbitmq_persistent = 0,]
    [rabbitmq_skip_broken_messages = N,]
    [rabbitmq_max_block_size = N,]
    [rabbitmq_flush_interval_ms = N,]
    [rabbitmq_queue_settings_list = 'x-dead-letter-exchange=my-dlx,x-max-length=10,x-overflow=reject-publish',]
    [rabbitmq_queue_consume = false,]
    [rabbitmq_address = '',]
    [rabbitmq_vhost = '/',]
    [rabbitmq_username = '',]
    [rabbitmq_password = '',]
    [rabbitmq_commit_on_select = false,]
    [rabbitmq_max_rows_per_message = 1,]
    [rabbitmq_handle_error_mode = 'default']
```

必需参数：

* `rabbitmq_host_port` – 主机:端口 (例如 `localhost:5672`) 。
* `rabbitmq_exchange_name` – RabbitMQ exchange 的名称。
* `rabbitmq_format` – 消息格式。使用与 SQL `FORMAT` 函数相同的表示法，例如 `JSONEachRow`。更多信息请参见[格式](../../../interfaces/formats.md)部分。

可选参数：

* `rabbitmq_exchange_type` – RabbitMQ exchange 的类型：`direct`、`fanout`、`topic`、`headers`、`consistent_hash`。默认值：`fanout`。
* `rabbitmq_routing_key_list` – 以逗号分隔的路由键列表。
* `rabbitmq_schema` – 如果格式需要 schema 定义，则必须使用此参数。例如，[Cap&#39;n Proto](https://capnproto.org/) 需要提供 schema file 的路径以及根对象 `schema.capnp:Message` 的名称。
* `rabbitmq_num_consumers` – 每个表的消费者数量。如果单个消费者的吞吐量不足，请指定更多消费者。默认值：`1`
* `rabbitmq_num_queues` – 队列总数。增加此数量可显著提升性能。默认值：`1`。
* `rabbitmq_queue_base` - 指定队列名称的提示信息。此设置的用例见下文。
* `rabbitmq_persistent` - 如果设置为 1 (true)，插入查询的投递模式将设为 2 (将消息标记为“持久”) 。默认值：`0`。
* `rabbitmq_skip_broken_messages` – 每个块中，RabbitMQ 消息解析器对与 schema 不兼容消息的容忍数量。如果 `rabbitmq_skip_broken_messages = N`，则引擎会跳过 *N* 条无法解析的 RabbitMQ 消息 (一条消息等于一行数据) 。默认值：`0`。
* `rabbitmq_max_block_size` - 从 RabbitMQ 刷新数据前收集的行数。默认值：[max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size)。
* `rabbitmq_flush_interval_ms` - 从 RabbitMQ 刷新数据的超时时间。默认值：[stream&#95;flush&#95;interval&#95;ms](/zh/operations/settings/settings#stream_flush_interval_ms)。
* `rabbitmq_queue_settings_list` - 允许在创建队列时设置 RabbitMQ 参数。可用参数：`x-max-length`、`x-max-length-bytes`、`x-message-ttl`、`x-expires`、`x-priority`、`x-max-priority`、`x-overflow`、`x-dead-letter-exchange`、`x-queue-type`。队列会自动启用 `durable` 设置。
* `rabbitmq_address` - 用于连接的地址。使用此设置或 `rabbitmq_host_port`。
* `rabbitmq_vhost` - RabbitMQ vhost。默认值：`'/'`。
* `rabbitmq_queue_consume` - 使用用户自定义队列，不执行任何 RabbitMQ 设置操作：声明 exchanges、队列和绑定。默认值：`false`。
* `rabbitmq_username` - RabbitMQ 用户名。
* `rabbitmq_password` - RabbitMQ 密码。
* `reject_unhandled_messages` - 出现错误时拒绝消息 (向 RabbitMQ 发送 negative acknowledgement) 。如果在 `rabbitmq_queue_settings_list` 中定义了 `x-dead-letter-exchange`，则会自动启用此设置。
* `rabbitmq_commit_on_select` - 执行 select 查询时提交消息。默认值：`false`。
* `rabbitmq_max_rows_per_message` — 对于基于行的格式，一条 RabbitMQ 消息中可写入的最大行数。默认值：`1`。
* `rabbitmq_empty_queue_backoff_start_ms` — 当 RabbitMQ 队列为空时，重新调度读取的起始 backoff 点。
* `rabbitmq_empty_queue_backoff_end_ms` — 当 RabbitMQ 队列为空时，重新调度读取的结束 backoff 点。
* `rabbitmq_empty_queue_backoff_step_ms` — 当 RabbitMQ 队列为空时，重新调度读取的 backoff 步长。
* `rabbitmq_handle_error_mode` — RabbitMQ 引擎 的错误处理方式。Possible values: default (如果消息解析失败，将抛出异常) 、stream (异常消息和原始消息将保存在虚拟列 `_error` 和 `_raw_message` 中) 、dead&#95;letter&#95;queue (与错误相关的数据将保存在 system.dead&#95;letter&#95;queue 中) 。

<div id="ssl-connection">
  ### SSL 连接
</div>

在连接地址中使用 `rabbitmq_secure = 1` 或 `amqps`：`rabbitmq_address = 'amqps://guest:guest@localhost/vhost'`。
所用库的默认行为是不会检查所建立的 TLS 连接是否足够安全。无论证书已过期、自签名、缺失还是无效，都会直接允许该连接。未来可能会实现更严格的证书检查。

还可以在 RabbitMQ 相关设置之外添加格式设置。

示例：

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64,
    date DateTime
  ) ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = 'localhost:5672',
                            rabbitmq_exchange_name = 'exchange1',
                            rabbitmq_format = 'JSONEachRow',
                            rabbitmq_num_consumers = 5,
                            date_time_input_format = 'best_effort';
```

应通过 ClickHouse 配置文件添加 RabbitMQ 服务器配置。

所需配置：

```xml
 <rabbitmq>
    <username>root</username>
    <password>clickhouse</password>
 </rabbitmq>
```

附加配置：

```xml
 <rabbitmq>
    <vhost>clickhouse</vhost>
 </rabbitmq>
```

<div id="description">
  ## 描述
</div>

`SELECT` 并不特别适合用来读取消息 (调试场景除外) ，因为每条消息只能读取一次。更实用的做法是使用 [materialized views](../../../sql-reference/statements/create/view.md) 创建实时线程。为此：

1. 使用该引擎 创建一个 RabbitMQ 消费者，并将其视为数据流。
2. 创建一个具有所需结构的表。
3. 创建一个 materialized view，将来自该引擎 的数据转换后写入前面创建的表中。

当 `MATERIALIZED VIEW` 连接到该引擎 后，就会开始在后台收集数据。这样，你就可以持续从 RabbitMQ 接收消息，并使用 `SELECT` 将其转换为所需格式。
一个 RabbitMQ 表可以拥有任意多个 materialized view。

数据可根据 `rabbitmq_exchange_type` 和指定的 `rabbitmq_routing_key_list` 进行路由。
每个表最多只能有一个 exchange。一个 exchange 可以由多个表共享，从而支持同时路由到多个表。

exchange type 选项：

* `direct` - 路由基于 key 的精确匹配。示例表 key 列表：`key1,key2,key3,key4,key5`，消息 key 可以等于其中任意一个。
* `fanout` - 路由到所有表 (exchange 名称相同的表) ，与 key 无关。
* `topic` - 路由基于以点分隔的 key 模式。示例：`*.logs`、`records.*.*.2020`、`*.2018,*.2019,*.2020`。
* `headers` - 路由基于 `key=value` 匹配，并使用 `x-match=all` 或 `x-match=any` 设置。示例表 key 列表：`x-match=all,format=logs,type=report,year=2020`。
* `consistent_hash` - 数据会在所有已绑定的表之间均匀分布 (exchange 名称相同的表) 。请注意，必须通过 RabbitMQ plugin 启用此 exchange type：`rabbitmq-plugins enable rabbitmq_consistent_hash_exchange`。

设置 `rabbitmq_queue_base` 可用于以下场景：

* 让不同的表共享队列，以便为同一组队列注册多个消费者，从而提升性能。如果使用 `rabbitmq_num_consumers` 和/或 `rabbitmq_num_queues` 设置，并且这些参数相同，则可以实现队列的精确匹配。
* 在并非所有消息都被成功消费时，能够从某些持久化队列恢复读取。要从某个特定队列恢复消费，请在 `rabbitmq_queue_base` 设置中指定其名称，并且不要指定 `rabbitmq_num_consumers` 和 `rabbitmq_num_queues` (默认值为 1) 。要恢复消费为某个特定表声明的所有队列，只需指定相同的设置：`rabbitmq_queue_base`、`rabbitmq_num_consumers`、`rabbitmq_num_queues`。默认情况下，队列名称对各个表都是唯一的。
* 复用这些队列，因为它们被声明为 durable 且不会被自动删除。 (可通过任意 RabbitMQ CLI 工具删除。)

为了提高性能，接收到的消息会被分组成大小为 [max&#95;insert&#95;block&#95;size](/zh/operations/settings/settings#max_insert_block_size) 的块。如果该块未能在 [stream&#95;flush&#95;interval&#95;ms](../../../operations/server-configuration-parameters/settings.md) 毫秒内形成，则无论块是否完整，数据都会被 flush 到表中。

如果在指定 `rabbitmq_exchange_type` 的同时还指定了 `rabbitmq_num_consumers` 和/或 `rabbitmq_num_queues` 设置，那么：

* 必须启用 `rabbitmq-consistent-hash-exchange` plugin。
* 必须指定已发布消息的 `message_id` 属性 (每条消息/批次唯一) 。

对于插入查询，会为每条已发布消息添加消息元数据：`messageID` 和 `republished` 标志 (如果消息发布超过一次，则为 true) ——可通过消息请求头访问。

不要对 inserts 和 materialized views 使用同一个表。

示例：

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = 'localhost:5672',
                            rabbitmq_exchange_name = 'exchange1',
                            rabbitmq_exchange_type = 'headers',
                            rabbitmq_routing_key_list = 'format=logs,type=report,year=2020',
                            rabbitmq_format = 'JSONEachRow',
                            rabbitmq_num_consumers = 5;

  CREATE TABLE daily (key UInt64, value UInt64)
    ENGINE = MergeTree() ORDER BY key;

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT key, value FROM queue;

  SELECT key, value FROM daily ORDER BY key;
```

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_exchange_name` - RabbitMQ exchange 名称。数据类型：`String`。
* `_channel_id` - 声明接收该消息的消费者时所在的 ChannelID。数据类型：`String`。
* `_delivery_tag` - 已接收消息的 DeliveryTag。其作用域限定在各个 channel 内。数据类型：`UInt64`。
* `_redelivered` - 消息的 `redelivered` 标志。数据类型：`UInt8`。
* `_message_id` - 已接收消息的 messageID；如果消息发布时设置了该值，则为非空。数据类型：`String`。
* `_timestamp` - 已接收消息的时间戳；如果消息发布时设置了该值，则为非空。数据类型：`UInt64`。

当 `rabbitmq_handle_error_mode='stream'` 时的附加虚拟列：

* `_raw_message` - 无法成功解析的原始消息。数据类型：`Nullable(String)`。
* `_error` - 解析失败期间产生的异常消息。数据类型：`Nullable(String)`。

注意：`_raw_message` 和 `_error` 虚拟列仅会在解析期间发生异常时填充；当消息成功解析时，它们始终为 `NULL`。

<div id="caveats">
  ## 注意事项
</div>

即使你可以在表定义中指定[默认列表达式](/zh/sql-reference/statements/create/table.md/#default_values) (例如 `DEFAULT`、`MATERIALIZED`、`ALIAS`) ，这些设置也会被忽略。相应的列将填入其类型对应的默认值。

<div id="data-formats-support">
  ## 数据格式支持
</div>

RabbitMQ 引擎支持 ClickHouse 支持的所有[格式](../../../interfaces/formats.md)。
单条 RabbitMQ 消息包含多少行，取决于所用格式是基于行还是基于块：

* 对于基于行的格式，单条 RabbitMQ 消息中的行数可通过设置 `rabbitmq_max_rows_per_message` 来控制。
* 对于基于块的格式，我们无法将块拆分得更小，但单个块中的行数可以通过通用设置 [max&#95;block&#95;size](/zh/operations/settings/settings#max_block_size) 来控制。