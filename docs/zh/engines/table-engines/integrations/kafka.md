# Kafka {#kafka}

此引擎与 [Apache Kafka](http://kafka.apache.org/) 结合使用。

Kafka 特性：

-   发布或者订阅数据流。
-   容错存储机制。
-   处理流数据。

<a name="table_engine-kafka-creating-a-table"></a>

老版格式：

    Kafka(kafka_broker_list, kafka_topic_list, kafka_group_name, kafka_format
          [, kafka_row_delimiter, kafka_schema, kafka_num_consumers])

新版格式：

    Kafka SETTINGS
      kafka_broker_list = 'localhost:9092',
      kafka_topic_list = 'topic1,topic2',
      kafka_group_name = 'group1',
      kafka_format = 'JSONEachRow',
      kafka_row_delimiter = '\n',
      kafka_schema = '',
      kafka_num_consumers = 2

必要参数：

-   `kafka_broker_list` – 以逗号分隔的 brokers 列表 (`localhost:9092`)。
-   `kafka_topic_list` – topic 列表 (`my_topic`)。
-   `kafka_group_name` – Kafka 消费组名称 (`group1`)。如果不希望消息在集群中重复，请在每个分片中使用相同的组名。
-   `kafka_format` – 消息体格式。使用与 SQL 部分的 `FORMAT` 函数相同表示方法，例如 `JSONEachRow`。了解详细信息，请参考 `Formats` 部分。

可选参数：

-   `kafka_row_delimiter` - 每个消息体（记录）之间的分隔符。
-   `kafka_schema` – 如果解析格式需要一个 schema 时，此参数必填。例如，[普罗托船长](https://capnproto.org/) 需要 schema 文件路径以及根对象 `schema.capnp:Message` 的名字。
-   `kafka_num_consumers` – 单个表的消费者数量。默认值是：`1`，如果一个消费者的吞吐量不足，则指定更多的消费者。消费者的总数不应该超过 topic 中分区的数量，因为每个分区只能分配一个消费者。

示例：

``` sql
  CREATE TABLE queue (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1', 'JSONEachRow');

  SELECT * FROM queue LIMIT 5;

  CREATE TABLE queue2 (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka SETTINGS kafka_broker_list = 'localhost:9092',
                            kafka_topic_list = 'topic',
                            kafka_group_name = 'group1',
                            kafka_format = 'JSONEachRow',
                            kafka_num_consumers = 4;

  CREATE TABLE queue2 (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1')
              SETTINGS kafka_format = 'JSONEachRow',
                       kafka_num_consumers = 4;
```

消费的消息会被自动追踪，因此每个消息在不同的消费组里只会记录一次。如果希望获得两次数据，则使用另一个组名创建副本。

消费组可以灵活配置并且在集群之间同步。例如，如果群集中有10个主题和5个表副本，则每个副本将获得2个主题。 如果副本数量发生变化，主题将自动在副本中重新分配。了解更多信息请访问 http://kafka.apache.org/intro。

`SELECT` 查询对于读取消息并不是很有用（调试除外），因为每条消息只能被读取一次。使用物化视图创建实时线程更实用。您可以这样做：

1.  使用引擎创建一个 Kafka 消费者并作为一条数据流。
2.  创建一个结构表。
3.  创建物化视图，改视图会在后台转换引擎中的数据并将其放入之前创建的表中。

当 `MATERIALIZED VIEW` 添加至引擎，它将会在后台收集数据。可以持续不断地从 Kafka 收集数据并通过 `SELECT` 将数据转换为所需要的格式。

示例：

``` sql
  CREATE TABLE queue (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1', 'JSONEachRow');

  CREATE TABLE daily (
    day Date,
    level String,
    total UInt64
  ) ENGINE = SummingMergeTree(day, (day, level), 8192);

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT toDate(toDateTime(timestamp)) AS day, level, count() as total
    FROM queue GROUP BY day, level;

  SELECT level, sum(total) FROM daily GROUP BY level;
```

为了提高性能，接受的消息被分组为 [max_insert_block_size](../../../operations/settings/settings.md#settings-max_insert_block_size) 大小的块。如果未在 [stream_flush_interval_ms](../../../operations/settings/settings.md#stream-flush-interval-ms) 毫秒内形成块，则不关心块的完整性，都会将数据刷新到表中。

停止接收主题数据或更改转换逻辑，请 detach 物化视图：

      DETACH TABLE consumer;
      ATTACH TABLE consumer;

如果使用 `ALTER` 更改目标表，为了避免目标表与视图中的数据之间存在差异，推荐停止物化视图。

## 配置 {#pei-zhi}

与 `GraphiteMergeTree` 类似，Kafka 引擎支持使用ClickHouse配置文件进行扩展配置。可以使用两个配置键：全局 (`kafka`) 和 主题级别 (`kafka_*`)。首先应用全局配置，然后应用主题级配置（如果存在）。

``` xml
  <!-- Global configuration options for all tables of Kafka engine type -->
  <kafka>
    <debug>cgrp</debug>
    <auto_offset_reset>smallest</auto_offset_reset>
  </kafka>

  <!-- Configuration specific for topic "logs" -->
  <kafka_logs>
    <retry_backoff_ms>250</retry_backoff_ms>
    <fetch_min_bytes>100000</fetch_min_bytes>
  </kafka_logs>
```

有关详细配置选项列表，请参阅 [librdkafka配置参考](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)。在 ClickHouse 配置中使用下划线 (`_`) ，并不是使用点 (`.`)。例如，`check.crcs=true` 将是 `<check_crcs>true</check_crcs>`。

[原始文章](https://clickhouse.tech/docs/zh/operations/table_engines/kafka/) <!--hide-->
