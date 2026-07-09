---
description: 'Kafka 表引擎可用于与 Apache Kafka 配合工作，让您能够发布或订阅
  数据流、构建容错存储，并在流可用时
  对其进行处理。'
sidebar_label: 'Kafka'
sidebar_position: 110
slug: /engines/table-engines/integrations/kafka
title: 'Kafka 表引擎'
keywords: ['Kafka', '表引擎']
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="kafka-table-engine">
  # Kafka 表引擎
</div>

:::tip
如果您使用的是 ClickHouse Cloud，我们建议改用 [ClickPipes](/zh/integrations/clickpipes)。ClickPipes 原生支持私网连接、摄取与集群资源的独立扩缩容，并为将流式 Kafka 数据摄取到 ClickHouse 提供全面监控。
:::

* 发布或订阅数据流。
* 构建具备容错能力的存储。
* 在数据流可用时立即处理。

<div id="creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [ALIAS expr1],
    name2 [type2] [ALIAS expr2],
    ...
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'host:port',
    kafka_topic_list = 'topic1,topic2,...',
    kafka_group_name = 'group_name',
    kafka_format = 'data_format'[,]
    [kafka_security_protocol = '',]
    [kafka_sasl_mechanism = '',]
    [kafka_sasl_username = '',]
    [kafka_sasl_password = '',]
    [kafka_autodetect_client_rack = '',]
    [kafka_schema = '',]
    [kafka_num_consumers = N,]
    [kafka_max_block_size = 0,]
    [kafka_skip_broken_messages = N,]
    [kafka_commit_every_batch = 0,]
    [kafka_client_id = '',]
    [kafka_poll_timeout_ms = 0,]
    [kafka_poll_max_batch_size = 0,]
    [kafka_flush_interval_ms = 0,]
    [kafka_consumer_reschedule_ms = 0,]
    [kafka_thread_per_consumer = 0,]
    [kafka_handle_error_mode = 'default',]
    [kafka_commit_on_select = false,]
    [kafka_consumer_acquire_timeout_ms = 30000,]
    [kafka_max_rows_per_message = 1,]
    [kafka_compression_codec = '',]
    [kafka_compression_level = -1];
```

必填参数：

* `kafka_broker_list` — 以逗号分隔的消息代理列表 (例如 `localhost:9092`) 。
* `kafka_topic_list` — Kafka topic 列表。
* `kafka_group_name` — Kafka 消费者组。系统会分别跟踪每个组的读取偏移量。如果不希望消息在集群中重复，请在所有位置使用相同的组名。
* `kafka_format` — 消息格式。使用与 SQL `FORMAT` 函数相同的表示法，例如 `JSONEachRow`。更多信息，请参见 [格式](../../../interfaces/formats.md) 部分。

可选参数：

* `kafka_security_protocol` - 用于与消息代理通信的协议。可选值：`plaintext`、`ssl`、`sasl_plaintext`、`sasl_ssl`。
* `kafka_sasl_mechanism` - 用于身份验证的 SASL 机制。可选值：`GSSAPI`、`PLAIN`、`SCRAM-SHA-256`、`SCRAM-SHA-512`、`OAUTHBEARER`、`AWS_MSK_IAM`。
* `kafka_aws_region` - 用于 MSK IAM 身份验证的 AWS 区域。若未指定，则会从 broker 地址自动检测。使用 PrivateLink 别名或不包含区域信息的自定义 DNS 主机名时，必须显式指定。默认值：空 (自动检测) 。
* `kafka_sasl_username` - 与 `PLAIN` 和 `SASL-SCRAM-..` 机制配合使用的 SASL 用户名。
* `kafka_sasl_password` - 与 `PLAIN` 和 `SASL-SCRAM-..` 机制配合使用的 SASL 密码。
* `kafka_schema` — 如果格式需要 schema 定义，则必须使用此参数。例如，[Cap&#39;n Proto](https://capnproto.org/) 需要提供 schema file 的路径以及根对象 `schema.capnp:Message` 的名称。
* `kafka_schema_registry_skip_bytes` — 使用带有封装请求头的 Schema Registry 时，每条消息开头需要跳过的字节数 (例如，AWS Glue Schema Registry 包含 19 字节的封装) 。范围：`[0, 255]`。默认值：`0`。
* `kafka_num_consumers` — 每个表的消费者数量。如果单个消费者的吞吐量不足，请增加消费者数量。消费者总数不应超过 topic 中的 partition 数量，因为每个 partition 只能分配给一个消费者；同时也不得超过部署 ClickHouse 的服务器上的物理 CPU 核心数。默认值：`1`。
* `kafka_max_block_size` — 单次 poll 的最大批次大小 (按消息数计) 。默认值：[max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size)。
* `kafka_skip_broken_messages` — 每个块内，Kafka 消息解析器对与 schema 不兼容消息的容忍数量。如果 `kafka_skip_broken_messages = N`，则引擎会跳过 *N* 条无法解析的 Kafka 消息 (一条消息对应一行数据) 。默认值：`0`。
* `kafka_commit_every_batch` — 每处理完一个已消费的批次就执行 commit，而不是在写入整个块后只执行一次 commit。默认值：`0`。
* `kafka_client_id` — client 标识符。默认为空。
* `kafka_poll_timeout_ms` — 单次从 Kafka poll 的超时时间。默认值：[stream&#95;poll&#95;timeout&#95;ms](../../../operations/settings/settings.md#stream_poll_timeout_ms)。
* `kafka_poll_max_batch_size` — 单次 Kafka poll 可拉取的最大消息数。默认值：[max&#95;block&#95;size](/zh/operations/settings/settings#max_block_size)。
* `kafka_flush_interval_ms` — 从 Kafka flush 数据的超时时间。默认值：[stream&#95;flush&#95;interval&#95;ms](/zh/operations/settings/settings#stream_flush_interval_ms)。
* `kafka_consumer_reschedule_ms` — Kafka stream processing 停滞时的重新调度间隔 (例如，没有可消费的消息时) 。此设置控制消费者重试 poll 前的等待时间。不得超过 `kafka_consumers_pool_ttl_ms`。默认值：`500` 毫秒。
* `kafka_thread_per_consumer` — 为每个消费者分配独立线程。启用后，每个消费者都会独立并行 flush 数据 (否则，会将多个消费者的行合并形成一个块) 。默认值：`0`。
* `kafka_handle_error_mode` — Kafka 引擎的错误处理方式。可选值：default (如果消息解析失败，则抛出异常) 、stream (异常消息和原始消息将保存在虚拟列 `_error` 和 `_raw_message` 中) 、dead&#95;letter&#95;queue (与错误相关的数据将保存在 system.dead&#95;letter&#95;queue 中) 。
* `kafka_commit_on_select` — 执行 select 查询时提交消息。默认值：`false`。
* `kafka_consumer_acquire_timeout_ms` — 在 `Kafka2` 表上直接执行 `SELECT` 查询时 (使用基于 Keeper 的 offset 存储) ，获取 Kafka 消费者的超时时间 (毫秒) 。当同一个表上有多个并发直接 `SELECT` 查询运行时，每个查询都必须等待消费者变为可用。该超时设置可防止在查询分别持有不同消费者子集时发生死锁。默认值：`30000`。
* `kafka_max_rows_per_message` — 对于基于行的格式，单条 kafka 消息中写入的最大行数。默认值：`1`。
* `kafka_autodetect_client_rack` — 自动为 `librdkafka` 设置 `client.rack` 参数，以优先选择最近的 Kafka 副本。
  支持的来源：
  `AWS_ZONE_ID` 表示 AWS IMDSv2 的可用区 ID，例如 `euc1-az1`；
  `AWS_ZONE_NAME` 表示 AWS IMDSv2 的可用区名称，例如 `eu-central-1a`；
  `GCP_ZONE` 表示 GCP 元数据服务中的可用区，例如 `europe-central2-a`；
  `CLICKHOUSE` 表示使用 ClickHouse 内部检测，这可能依赖云元数据或配置；
  `AWS_ZONE_NAME_THEN_GCP_ZONE` 表示先尝试 `AWS_ZONE_NAME`，再尝试 `GCP_ZONE`。
  默认值：空字符串，即禁用。
  提示：不同环境使用的可用区格式不同。亚马逊 MSK 通常使用可用区 ID，因此优先选择 `AWS_ZONE_ID`。Confluent Cloud 通常使用可用区名称，因此优先选择 `AWS_ZONE_NAME`。如果不确定，可使用 `AWS_ZONE_NAME_THEN_GCP_ZONE`，或检查你的 cluster 上的 `broker.rack` 值。
  注意：Kafka broker 必须配置 `broker.rack` 和 `replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector`。
* `kafka_compression_codec` — 用于生成消息的压缩 codec。支持：空字符串、`none`、`gzip`、`snappy`、`lz4`、`zstd`。当该值为空字符串时，表不会设置压缩 codec，因此将使用配置文件中的值或 `librdkafka` 的默认值。默认值：空字符串。
* `kafka_compression_level` — 由 kafka&#95;compression&#95;codec 选择的算法对应的压缩级别参数。值越高，压缩效果越好，但 CPU 使用率也会更高。可用范围取决于算法：`gzip` 为 `[0-9]`；`lz4` 为 `[0-12]`；`snappy` 仅支持 `0`；`zstd` 为 `[0-12]`；`-1` = codec 相关的默认压缩级别。默认值：`-1`。
* `kafka_map_virtual_columns_on_write` — 如果启用，表 schema 中名称为 `_key`、`_timestamp`、`_headers.name` 和 `_headers.value` 的列会在 `INSERT` 时映射到对应的 Kafka 消息元数据，并从消息载荷中排除。请参见 [将列映射到 Kafka 消息元数据](#mapping-columns-to-kafka-message-metadata)。默认值：`false`。

示例：

```sql
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

  CREATE TABLE queue3 (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1')
              SETTINGS kafka_format = 'JSONEachRow',
                       kafka_num_consumers = 4;
```

<details markdown="1">
  <summary>已弃用的创建表方法</summary>

  :::note
  请勿在新项目中使用此方法。如有可能，请将旧项目切换到上文所述的方法。
  :::

  ```sql
  Kafka(kafka_broker_list, kafka_topic_list, kafka_group_name, kafka_format
        [, kafka_row_delimiter, kafka_schema, kafka_num_consumers, kafka_max_block_size,  kafka_skip_broken_messages, kafka_commit_every_batch, kafka_client_id, kafka_poll_timeout_ms, kafka_poll_max_batch_size, kafka_flush_interval_ms, kafka_consumer_reschedule_ms, kafka_thread_per_consumer, kafka_handle_error_mode, kafka_commit_on_select, kafka_max_rows_per_message]);
  ```
</details>

:::info
Kafka 表引擎不支持带有 [default value](/zh/sql-reference/statements/create/table#default_values) 的列。如果需要带有 default value 的列，可以在 materialized view 层添加它们 (见下文) 。
:::

<div id="description">
  ## 描述
</div>

已投递的消息会被自动跟踪，因此一个组中的每条消息只会被计数一次。如果你想获取两次数据，请使用另一个组名创建该表的副本。

组是灵活的，并且会在集群中同步。例如，如果你有 10 个 topic，以及集群中某张表的 5 个副本，那么每个副本会分配到 2 个 topic。如果副本数量发生变化，这些 topic 会自动在各副本之间重新分配。更多信息请参见 http://kafka.apache.org/intro。

建议每个 Kafka topic 都使用自己专用的消费者组，以确保 topic 与组之间是一对一的独占对应关系，尤其是在 topic 可能被动态创建和删除的环境中 (例如测试或暂存环境) 。

`SELECT` 并不特别适合用于读取消息 (调试除外) ，因为每条消息只能被读取一次。更实用的做法是使用 materialized view 创建实时处理流程。为此：

1. 使用该引擎创建一个 Kafka 消费者，并将其视为一个数据 stream。
2. 创建一个具有所需结构的表。
3. 创建一个 materialized view，将来自该引擎的数据转换后写入前面创建的表中。

当 `MATERIALIZED VIEW` 关联到该引擎后，它就会开始在后台收集数据。这样你就可以持续从 Kafka 接收消息，并使用 `SELECT` 将其转换为所需的 format。
一个 Kafka 表可以拥有任意多个 materialized view；它们不会直接从 Kafka 表中读取数据，而是接收新的记录 (以块的形式) ，这样你就可以将数据写入多个明细级别不同的表中 (带分组聚合和不带分组聚合) 。

示例：

```sql
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
    AS SELECT toDate(toDateTime(timestamp)) AS day, level, count() AS total
    FROM queue GROUP BY day, level;

  SELECT level, sum(total) FROM daily GROUP BY level;
```

为了提高性能，接收到的消息会按 [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size) 的大小分组成块。如果在 [stream&#95;flush&#95;interval&#95;ms](/zh/operations/settings/settings#stream_flush_interval_ms) 毫秒内未形成该块，则无论该块是否完整，数据都会被刷新到表中。

要停止接收 topic 数据或更改转换逻辑，请分离 materialized view：

```sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

如果要使用 `ALTER` 更改目标表，我们建议先禁用materialized view，以避免目标表与视图数据之间出现不一致。

<div id="configuration">
  ## 配置
</div>

与 GraphiteMergeTree 类似，Kafka 引擎支持通过 ClickHouse 配置文件进行扩展配置。你可以使用两类配置项：全局级别 (位于 `<kafka>` 下) 和 topic 级别 (位于 `<kafka><kafka_topic>` 下) 。系统会先应用全局配置，然后再应用 topic 级别的配置 (如果存在) 。

```xml
  <kafka>
    <!-- Global configuration options for all tables of Kafka engine type -->
    <debug>cgrp</debug>
    <statistics_interval_ms>3000</statistics_interval_ms>

    <kafka_topic>
        <name>logs</name>
        <statistics_interval_ms>4000</statistics_interval_ms>
    </kafka_topic>

    <!-- Settings for consumer -->
    <consumer>
        <auto_offset_reset>smallest</auto_offset_reset>
        <kafka_topic>
            <name>logs</name>
            <fetch_min_bytes>100000</fetch_min_bytes>
        </kafka_topic>

        <kafka_topic>
            <name>stats</name>
            <fetch_min_bytes>50000</fetch_min_bytes>
        </kafka_topic>
    </consumer>

    <!-- Settings for producer -->
    <producer>
        <kafka_topic>
            <name>logs</name>
            <retry_backoff_ms>250</retry_backoff_ms>
        </kafka_topic>

        <kafka_topic>
            <name>stats</name>
            <retry_backoff_ms>400</retry_backoff_ms>
        </kafka_topic>
    </producer>
  </kafka>
```

有关可用配置选项的完整列表，请参阅 [librdkafka configuration reference](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)。在 ClickHouse 配置中，请使用下划线 (`_`) 而不是点号 (`.`) 。例如，`check.crcs=true` 应写为 `<check_crcs>true</check_crcs>`。

<div id="kafka-aws-msk-iam">
  ### AWS MSK IAM 身份验证
</div>

:::note
AWS MSK IAM 身份验证要求 ClickHouse 在构建时启用 AWS S3 支持。
:::

AWS MSK 支持基于 IAM 的身份验证，因此可以使用 AWS 凭证连接到 Kafka 集群，无需单独管理用户名和密码。

**基本设置：**

在表设置中将 `kafka_sasl_mechanism` 设为 `AWS_MSK_IAM`：

```sql
CREATE TABLE msk_queue (
    timestamp UInt64,
    level String,
    message String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'b-1.mycluster.kafka.us-east-1.amazonaws.com:9098',
    kafka_topic_list = 'my-topic',
    kafka_group_name = 'my-group',
    kafka_format = 'JSONEachRow',
    kafka_sasl_mechanism = 'AWS_MSK_IAM';
```

AWS 区域会通过模式匹配从 broker 端点中自动提取：

* 预置型 MSK：`b-X.cluster.kafka.<region>.amazonaws.com:9098`
* Serverless MSK：`boot-X.kafka-serverless.<region>.amazonaws.com:9098`
* VPC 端点：`vpce-X.kafka.<region>.vpce.amazonaws.com:9098`

**AWS 凭证：**

如果存在，凭证始终会从 `~/.aws/credentials` 和 `~/.aws/config` (AWS profile 文件) 中加载。若还要启用 EC2 instance profile、环境变量 (`AWS_ACCESS_KEY_ID` 等) 、ECS task role 以及其他自动凭证来源，请将以下内容添加到你的服务器配置中：

```xml
<kafka>
  <use_environment_credentials>true</use_environment_credentials>
</kafka>
```

此设置只能由服务器管理员进行配置。默认值：`false`。

**PrivateLink 和自定义 DNS：**

使用不包含区域信息的 PrivateLink 别名或自定义 DNS 主机名时，请显式指定 AWS 区域：

```sql
CREATE TABLE msk_privatelink_queue (
    timestamp UInt64,
    level String,
    message String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'my-privatelink-alias.internal.example.com:9098',
    kafka_topic_list = 'my-topic',
    kafka_group_name = 'my-group',
    kafka_format = 'JSONEachRow',
    kafka_sasl_mechanism = 'AWS_MSK_IAM',
    kafka_aws_region = 'us-east-1';
```

**IAM 权限：**

消费者权限 (用于读取消息) ：

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:ReadData",
      "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeGroup"
    ],
    "Resource": [
      "arn:aws:kafka:REGION:ACCOUNT:cluster/CLUSTER_NAME/*",
      "arn:aws:kafka:REGION:ACCOUNT:topic/CLUSTER_NAME/TOPIC_NAME/*",
      "arn:aws:kafka:REGION:ACCOUNT:group/CLUSTER_NAME/CONSUMER_GROUP/*"
    ]
  }]
}
```

生产者权限 (用于写入消息) ：

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:WriteData"
    ],
    "Resource": [
      "arn:aws:kafka:REGION:ACCOUNT:cluster/CLUSTER_NAME/*",
      "arn:aws:kafka:REGION:ACCOUNT:topic/CLUSTER_NAME/TOPIC_NAME/*"
    ]
  }]
}
```

<div id="kafka-kerberos-support">
  ### Kerberos 支持
</div>

要连接到启用了 Kerberos 的 Kafka，请添加值为 `sasl_plaintext` 的 `security_protocol` 子元素。只要已通过操作系统机制获取并缓存 Kerberos ticket-granting ticket 即可。
ClickHouse 可以使用 keytab 文件维护 Kerberos 凭据。请考虑添加 `sasl_kerberos_service_name`、`sasl_kerberos_keytab` 和 `sasl_kerberos_principal` 子元素。

示例：

```xml
<!-- Kerberos-aware Kafka -->
<kafka>
  <security_protocol>SASL_PLAINTEXT</security_protocol>
  <sasl_kerberos_keytab>/home/kafkauser/kafkauser.keytab</sasl_kerberos_keytab>
  <sasl_kerberos_principal>kafkauser/kafkahost@EXAMPLE.COM</sasl_kerberos_principal>
</kafka>
```

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_topic` — Kafka topic。数据类型：`LowCardinality(String)`。
* `_key` — 消息的键。数据类型：`String`。
* `_offset` — 消息的偏移量。数据类型：`UInt64`。
* `_timestamp` — 消息的时间戳。数据类型：`Nullable(DateTime)`。
* `_timestamp_ms` — 消息的毫秒级时间戳。数据类型：`Nullable(DateTime64(3))`。
* `_partition` — Kafka topic 的分区。数据类型：`UInt64`。
* `_headers.name` — 消息请求头键的数组。数据类型：`Array(String)`。
* `_headers.value` — 消息请求头值的数组。数据类型：`Array(String)`。

当 `kafka_handle_error_mode='stream'` 时，还会附加以下虚拟列：

* `_raw_message` - 无法成功解析的原始消息。数据类型：`String`。
* `_error` - 解析失败时产生的异常消息。数据类型：`String`。

注意：`_raw_message` 和 `_error` 这两个虚拟列仅会在解析过程中发生异常时填充；消息成功解析时，它们始终为空。

<div id="mapping-columns-to-kafka-message-metadata">
  ## 将列映射到 Kafka 消息元数据
</div>

使用 `INSERT INTO` 生成消息时，如果表中存在名为 `_key` 的列 (类型为 `String`) 和名为 `_timestamp` 的列 (类型为 `DateTime`) ，Kafka 引擎始终会分别将它们用作 Kafka 消息键和 Kafka 消息时间戳。默认情况下，这些列也会和其他列一起出现在生成的消息载荷中。

设置 `kafka_map_virtual_columns_on_write = 1` 后，行为会发生变化：

* `_key` (类型为 `String`) — 映射为 Kafka 消息键。
* `_timestamp` (类型为 `DateTime`) — 映射为 Kafka 消息时间戳。
* `_headers.name` (类型为 `Array(String)`) 和 `_headers.value` (类型为 `Array(String)`) — 映射为 Kafka 消息请求头。每一对 `(_headers.name[i], _headers.value[i])` 都会成为一个 Kafka 请求头。由于 `_headers.name` 和 `_headers.value` 共享 `_headers` 这个 Nested 前缀，ClickHouse 要求这两个数组在每一行中的长度都相同。

只有当具有这些名称的列其类型与上面列出的类型一致时，它们才会**从消息载荷中排除**；否则，它们仍会保留在载荷中，因此，对于恰好复用了这些名称来存储无关数据的 schema，依然可以正常工作。

示例：

```sql
CREATE TABLE kafka_out
(
    event_json String,
    `_key` String,
    `_timestamp` DateTime,
    `_headers.name` Array(String),
    `_headers.value` Array(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'broker:9092',
    kafka_topic_list = 'events',
    kafka_group_name = 'events-producer',
    kafka_format = 'JSONEachRow',
    kafka_map_virtual_columns_on_write = 1;

INSERT INTO kafka_out VALUES
    ('{"a":1}', 'session-42', now(), ['source', 'trace_id'], ['api', 'abc-123']);
```

生成的 Kafka 消息包含载荷 `{"event_json":"{\"a\":1}"}`、消息键 `session-42`、当前时间戳，以及两个请求头 `source=api` 和 `trace_id=abc-123`。

<div id="data-formats-support">
  ## 数据格式支持
</div>

Kafka 引擎支持 ClickHouse 支持的所有[格式](../../../interfaces/formats.md)。
单条 Kafka 消息中的行数取决于所用格式是按行还是按块：

* 对于按行的格式，单条 Kafka 消息中的行数可以通过设置 `kafka_max_rows_per_message` 来控制。
* 对于按块的格式，我们无法将一个块再拆分成更小的部分，但单个块中的行数可以通过通用设置 [max&#95;block&#95;size](/zh/operations/settings/settings#max_block_size) 来控制。

<div id="engine-to-store-committed-offsets-in-clickhouse-keeper">
  ## 在 ClickHouse Keeper 中存储已提交偏移量的引擎
</div>

<ExperimentalBadge />

如果启用了 `allow_experimental_kafka_offsets_storage_in_keeper`，则可以为 Kafka 表引擎额外指定两个设置：

* `kafka_keeper_path`：指定 ClickHouse Keeper 中该表的路径
* `kafka_replica_name`：指定 ClickHouse Keeper 中的副本名称

这两个设置必须同时指定，或者都不指定。当这两个设置都已指定时，将使用一个新的 Experimental Kafka 引擎。这个新引擎不再依赖将已提交偏移量存储在 Kafka 中，而是将其存储在 ClickHouse Keeper 中。它仍会尝试将偏移量提交到 Kafka，但只有在创建表时才依赖这些偏移量。其他情况下 (例如表重启，或在发生错误后恢复) ，都会使用存储在 ClickHouse Keeper 中的偏移量继续消费消息。除了已提交偏移量外，它还会存储上一批次消费的消息数量，因此如果 insert 失败，就会再次消费相同数量的消息，从而在需要时支持去重。

示例：

```sql
CREATE TABLE experimental_kafka (key UInt64, value UInt64)
ENGINE = Kafka('localhost:19092', 'my-topic', 'my-consumer', 'JSONEachRow')
SETTINGS
  kafka_keeper_path = '/clickhouse/{database}/{uuid}',
  kafka_replica_name = '{replica}'
SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;
```

<div id="known-limitations">
  ### 已知限制
</div>

由于这个新引擎仍处于 Experimental 阶段，因此尚未达到可用于生产环境的程度。目前该实现已知存在以下限制：

* 如果快速删除并重新创建该表，或者为不同引擎指定相同的 ClickHouse Keeper 路径，可能会导致问题。最佳实践是在 `kafka_keeper_path` 中使用 `{uuid}`，以避免路径冲突。
* 为了实现可重复读取，单个线程不能消费来自多个分区的消息。另一方面，Kafka 消费者又必须定期执行 poll 才能保持存活。基于这两个要求，我们决定仅在启用 `kafka_thread_per_consumer` 时才允许创建多个消费者，否则很难避免因需要定期 poll 消费者而引发的问题。

**另请参阅**

* [虚拟列](../../../engines/table-engines/index.md#table_engines-virtual_columns)
* [background&#95;message&#95;broker&#95;schedule&#95;pool&#95;size](/zh/operations/server-configuration-parameters/settings#background_message_broker_schedule_pool_size)
* [system.kafka&#95;consumers](../../../operations/system-tables/kafka_consumers.md)