---
toc_priority: 10
toc_title: RabbitMQ
---

# RabbitMQ 引擎 {#rabbitmq-engine}

该引擎允许 ClickHouse 与 [RabbitMQ](https://www.rabbitmq.com) 进行集成.

`RabbitMQ` 可以让你:

- 发布或订阅数据流。
- 在数据流可用时进行处理。

## 创建一张表 {#table_engine-rabbitmq-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = RabbitMQ SETTINGS
    rabbitmq_host_port = 'host:port',
    rabbitmq_exchange_name = 'exchange_name',
    rabbitmq_format = 'data_format'[,]
    [rabbitmq_exchange_type = 'exchange_type',]
    [rabbitmq_routing_key_list = 'key1,key2,...',]
    [rabbitmq_row_delimiter = 'delimiter_symbol',]
    [rabbitmq_schema = '',]
    [rabbitmq_num_consumers = N,]
    [rabbitmq_num_queues = N,]
    [rabbitmq_queue_base = 'queue',]
    [rabbitmq_deadletter_exchange = 'dl-exchange',]
    [rabbitmq_persistent = 0,]
    [rabbitmq_skip_broken_messages = N,]
    [rabbitmq_max_block_size = N,]
    [rabbitmq_flush_interval_ms = N]
```

必要参数:

-   `rabbitmq_host_port` – 主机名:端口号 (比如, `localhost:5672`).
-   `rabbitmq_exchange_name` – RabbitMQ exchange 名称.
-   `rabbitmq_format` – 消息格式. 使用与SQL`FORMAT`函数相同的标记，如`JSONEachRow`。 更多信息，请参阅 [Formats](../../../interfaces/formats.md) 部分.

可选参数:

-   `rabbitmq_exchange_type` – RabbitMQ exchange 的类型: `direct`, `fanout`, `topic`, `headers`, `consistent_hash`. 默认是: `fanout`.
-   `rabbitmq_routing_key_list` – 一个以逗号分隔的路由键列表.
-   `rabbitmq_row_delimiter` – 用于消息结束的分隔符.
-   `rabbitmq_schema` – 如果格式需要模式定义，必须使用该参数。比如, [Cap’n Proto](https://capnproto.org/) 需要模式文件的路径以及根 `schema.capnp:Message` 对象的名称.
-   `rabbitmq_num_consumers` – 每个表的消费者数量。默认：`1`。如果一个消费者的吞吐量不够，可以指定更多的消费者.
-   `rabbitmq_num_queues` – 队列的总数。默认值: `1`. 增加这个数字可以显著提高性能.
-   `rabbitmq_queue_base` -  指定一个队列名称的提示。这个设置的使用情况如下.
-   `rabbitmq_deadletter_exchange` - 为[dead letter exchange](https://www.rabbitmq.com/dlx.html)指定名称。你可以用这个 exchange 的名称创建另一个表，并在消息被重新发布到 dead letter exchange 的情况下收集它们。默认情况下，没有指定 dead letter exchange。Specify name for a [dead letter exchange](https://www.rabbitmq.com/dlx.html).
-   `rabbitmq_persistent` - 如果设置为 1 (true), 在插入查询中交付模式将被设置为 2 (将消息标记为 'persistent'). 默认是: `0`.
-   `rabbitmq_skip_broken_messages` – RabbitMQ 消息解析器对每块模式不兼容消息的容忍度。默认值：`0`. 如果 `rabbitmq_skip_broken_messages = N`，那么引擎将跳过 *N* 个无法解析的 RabbitMQ 消息（一条消息等于一行数据）。
-   `rabbitmq_max_block_size`
-   `rabbitmq_flush_interval_ms`

同时，格式的设置也可以与 rabbitmq 相关的设置一起添加。

示例:

``` sql
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

RabbitMQ 服务器配置应使用 ClickHouse 配置文件添加。

必要配置:

``` xml
 <rabbitmq>
    <username>root</username>
    <password>clickhouse</password>
 </rabbitmq>
```

可选配置:

``` xml
 <rabbitmq>
    <vhost>clickhouse</vhost>
 </rabbitmq>
```

## 描述 {#description}

`SELECT`对于读取消息不是特别有用（除了调试），因为每个消息只能读取一次。使用[物化视图](../../../sql-reference/statements/create.md#create-view)创建实时线程更为实用。要做到这一点:

1.  使用引擎创建一个 RabbitMQ 消费者，并将其视为一个数据流。
2.  创建一个具有所需结构的表。
3.  创建一个物化视图，转换来自引擎的数据并将其放入先前创建的表中。

当`物化视图`加入引擎时，它开始在后台收集数据。这允许您持续接收来自 RabbitMQ 的消息，并使用 `SELECT` 将它们转换为所需格式。
一个 RabbitMQ 表可以有多个你需要的物化视图。

数据可以根据`rabbitmq_exchange_type`和指定的`rabbitmq_routing_key_list`进行通道。
每个表不能有多于一个 exchange。一个 exchange 可以在多个表之间共享 - 因为可以使用路由让数据同时进入多个表。

Exchange 类型的选项:

-   `direct` - 路由是基于精确匹配的键。例如表的键列表: `key1,key2,key3,key4,key5`, 消息键可以是等同他们中的任意一个.
-   `fanout` - 路由到所有的表 (exchange 名称相同的情况) 无论是什么键都是这样.
-   `topic` - 路由是基于带有点分隔键的模式. 比如: `*.logs`, `records.*.*.2020`, `*.2018,*.2019,*.2020`.
-   `headers` -  路由是基于`key=value`的匹配，设置为`x-match=all`或`x-match=any`. 例如表的键列表: `x-match=all,format=logs,type=report,year=2020`.
-   `consistent_hash` - 数据在所有绑定的表之间均匀分布 (exchange 名称相同的情况). 请注意，这种 exchange 类型必须启用 RabbitMQ 插件: `rabbitmq-plugins enable rabbitmq_consistent_hash_exchange`.

设置`rabbitmq_queue_base`可用于以下情况:

-   来让不同的表共享队列, 这样就可以为同一个队列注册多个消费者，这使得性能更好。如果使用`rabbitmq_num_consumers`和/或`rabbitmq_num_queues`设置，在这些参数相同的情况下，实现队列的精确匹配。
-   以便在不是所有消息都被成功消费时，能够恢复从某些持久队列的阅读。要从一个特定的队列恢复消耗 - 在`rabbitmq_queue_base`设置中设置其名称，不要指定`rabbitmq_num_consumers`和`rabbitmq_num_queues`（默认为1）。要恢复所有队列的消费，这些队列是为一个特定的表所声明的 - 只要指定相同的设置。`rabbitmq_queue_base`, `rabbitmq_num_consumers`, `rabbitmq_num_queues`。默认情况下，队列名称对表来说是唯一的。
-   以重复使用队列，因为它们被声明为持久的，并且不会自动删除。可以通过任何 RabbitMQ CLI 工具删除）

为了提高性能，收到的消息被分组为大小为 [max_insert_block_size](../../../operations/server-configuration-parameters/settings.md#settings-max_insert_block_size) 的块。如果在[stream_flush_interval_ms](../../../operations/server-configuration-parameters/settings.md)毫秒内没有形成数据块，无论数据块是否完整，数据都会被刷到表中。

如果`rabbitmq_num_consumers`和/或`rabbitmq_num_queues`设置与`rabbitmq_exchange_type`一起被指定，那么:

-   必须启用`rabbitmq-consistent-hash-exchange` 插件.
-   必须指定已发布信息的 `message_id`属性（对于每个信息/批次都是唯一的）。

对于插入查询时有消息元数据，消息元数据被添加到每个发布的消息中:`messageID`和`republished`标志（如果值为true，则表示消息发布不止一次） - 可以通过消息头访问。

不要在插入和物化视图中使用同一个表。

示例:

``` sql
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

## 虚拟列 {#virtual-columns}

-   `_exchange_name` - RabbitMQ exchange 名称.
-   `_channel_id` - 接收消息的消费者所声明的频道ID.
-   `_delivery_tag` - 收到消息的DeliveryTag. 以每个频道为范围.
-   `_redelivered` - 消息的`redelivered`标志.
-   `_message_id` - 收到的消息的ID；如果在消息发布时被设置，则为非空.
-   `_timestamp` - 收到的消息的时间戳；如果在消息发布时被设置，则为非空.

[原始文章](https://clickhouse.tech/docs/en/engines/table-engines/integrations/rabbitmq/) <!--hide-->
