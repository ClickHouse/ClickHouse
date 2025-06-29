---
description: 'The Kafka Table Engine can be used to publish works with Apache Kafka and lets you publish or subscribe
  to data flows, organize fault-tolerant storage, and process streams as they become
  available.'
sidebar_label: 'Kafka'
sidebar_position: 110
slug: /engines/table-engines/integrations/kafka
title: 'Kafka'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# Kafka

:::note
If you're on ClickHouse Cloud, we recommend using [ClickPipes](/integrations/clickpipes) instead. ClickPipes natively supports private network connections, scaling ingestion and cluster resources independently, and comprehensive monitoring for streaming Kafka data into ClickHouse.
:::

- Publish or subscribe to data flows.
- Organize fault-tolerant storage.
- Process streams as they become available.

## Creating a Table {#creating-a-table}

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
    [kafka_schema = '',]
    [kafka_num_consumers = N,]
    [kafka_max_block_size = 0,]
    [kafka_skip_broken_messages = N,]
    [kafka_commit_every_batch = 0,]
    [kafka_client_id = '',]
    [kafka_poll_timeout_ms = 0,]
    [kafka_poll_max_batch_size = 0,]
    [kafka_flush_interval_ms = 0,]
    [kafka_thread_per_consumer = 0,]
    [kafka_handle_error_mode = 'default',]
    [kafka_commit_on_select = false,]
    [kafka_max_rows_per_message = 1];
```

Required parameters:

- `kafka_broker_list` — A comma-separated list of brokers (for example, `localhost:9092`).
- `kafka_topic_list` — A list of Kafka topics.
- `kafka_group_name` — A group of Kafka consumers. Reading margins are tracked for each group separately. If you do not want messages to be duplicated in the cluster, use the same group name everywhere.
- `kafka_format` — Message format. Uses the same notation as the SQL `FORMAT` function, such as `JSONEachRow`. For more information, see the [Formats](../../../interfaces/formats.md) section.

Optional parameters:

- `kafka_security_protocol` - Protocol used to communicate with brokers. Possible values: `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl`.
- `kafka_sasl_mechanism` - SASL mechanism to use for authentication. Possible values: `GSSAPI`, `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`.
- `kafka_sasl_username` - SASL username for use with the `PLAIN` and `SASL-SCRAM-..` mechanisms.
- `kafka_sasl_password` - SASL password for use with the `PLAIN` and `SASL-SCRAM-..` mechanisms.
- `kafka_schema` — Parameter that must be used if the format requires a schema definition. For example, [Cap'n Proto](https://capnproto.org/) requires the path to the schema file and the name of the root `schema.capnp:Message` object.
- `kafka_num_consumers` — The number of consumers per table. Specify more consumers if the throughput of one consumer is insufficient. The total number of consumers should not exceed the number of partitions in the topic, since only one consumer can be assigned per partition, and must not be greater than the number of physical cores on the server where ClickHouse is deployed. Default: `1`.
- `kafka_max_block_size` — The maximum batch size (in messages) for poll. Default: [max_insert_block_size](../../../operations/settings/settings.md#max_insert_block_size).
- `kafka_skip_broken_messages` — Kafka message parser tolerance to schema-incompatible messages per block. If `kafka_skip_broken_messages = N` then the engine skips *N* Kafka messages that cannot be parsed (a message equals a row of data). Default: `0`.
- `kafka_commit_every_batch` — Commit every consumed and handled batch instead of a single commit after writing a whole block. Default: `0`.
- `kafka_client_id` — Client identifier. Empty by default.
- `kafka_poll_timeout_ms` — Timeout for single poll from Kafka. Default: [stream_poll_timeout_ms](../../../operations/settings/settings.md#stream_poll_timeout_ms).
- `kafka_poll_max_batch_size` — Maximum amount of messages to be polled in a single Kafka poll. Default: [max_block_size](/operations/settings/settings#max_block_size).
- `kafka_flush_interval_ms` — Timeout for flushing data from Kafka. Default: [stream_flush_interval_ms](/operations/settings/settings#stream_flush_interval_ms).
- `kafka_thread_per_consumer` — Provide independent thread for each consumer. When enabled, every consumer flush the data independently, in parallel (otherwise — rows from several consumers squashed to form one block). Default: `0`.
- `kafka_handle_error_mode` — How to handle errors for Kafka engine. Possible values: default (the exception will be thrown if we fail to parse a message), stream (the exception message and raw message will be saved in virtual columns `_error` and `_raw_message`).
- `kafka_commit_on_select` —  Commit messages when select query is made. Default: `false`.
- `kafka_max_rows_per_message` — The maximum number of rows written in one kafka message for row-based formats. Default : `1`.

Examples:

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

<summary>Deprecated Method for Creating a Table</summary>

:::note
Do not use this method in new projects. If possible, switch old projects to the method described above.
:::

```sql
Kafka(kafka_broker_list, kafka_topic_list, kafka_group_name, kafka_format
      [, kafka_row_delimiter, kafka_schema, kafka_num_consumers, kafka_max_block_size,  kafka_skip_broken_messages, kafka_commit_every_batch, kafka_client_id, kafka_poll_timeout_ms, kafka_poll_max_batch_size, kafka_flush_interval_ms, kafka_thread_per_consumer, kafka_handle_error_mode, kafka_commit_on_select, kafka_max_rows_per_message]);
```

</details>

:::info
The Kafka table engine doesn't support columns with [default value](/sql-reference/statements/create/table#default_values). If you need columns with default value, you can add them at materialized view level (see below).
:::

## Description {#description}

The delivered messages are tracked automatically, so each message in a group is only counted once. If you want to get the data twice, then create a copy of the table with another group name.

Groups are flexible and synced on the cluster. For instance, if you have 10 topics and 5 copies of a table in a cluster, then each copy gets 2 topics. If the number of copies changes, the topics are redistributed across the copies automatically. Read more about this at http://kafka.apache.org/intro.

`SELECT` is not particularly useful for reading messages (except for debugging), because each message can be read only once. It is more practical to create real-time threads using materialized views. To do this:

1.  Use the engine to create a Kafka consumer and consider it a data stream.
2.  Create a table with the desired structure.
3.  Create a materialized view that converts data from the engine and puts it into a previously created table.

When the `MATERIALIZED VIEW` joins the engine, it starts collecting data in the background. This allows you to continually receive messages from Kafka and convert them to the required format using `SELECT`.
One kafka table can have as many materialized views as you like, they do not read data from the kafka table directly, but receive new records (in blocks), this way you can write to several tables with different detail level (with grouping - aggregation and without).

Example:

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
To improve performance, received messages are grouped into blocks the size of [max_insert_block_size](../../../operations/settings/settings.md#max_insert_block_size). If the block wasn't formed within [stream_flush_interval_ms](/operations/settings/settings#stream_flush_interval_ms) milliseconds, the data will be flushed to the table regardless of the completeness of the block.

To stop receiving topic data or to change the conversion logic, detach the materialized view:

```sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

If you want to change the target table by using `ALTER`, we recommend disabling the material view to avoid discrepancies between the target table and the data from the view.

## Configuration {#configuration}

Similar to GraphiteMergeTree, the Kafka engine supports extended configuration using the ClickHouse config file. There are two configuration keys that you can use: global (below `<kafka>`) and topic-level (below `<kafka><kafka_topic>`). The global configuration is applied first, and then the topic-level configuration is applied (if it exists).

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


For a list of possible configuration options, see the [librdkafka configuration reference](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md). Use the underscore (`_`) instead of a dot in the ClickHouse configuration. For example, `check.crcs=true` will be `<check_crcs>true</check_crcs>`.

### Kerberos support {#kafka-kerberos-support}

To deal with Kerberos-aware Kafka, add `security_protocol` child element with `sasl_plaintext` value. It is enough if Kerberos ticket-granting ticket is obtained and cached by OS facilities.
ClickHouse is able to maintain Kerberos credentials using a keytab file. Consider `sasl_kerberos_service_name`, `sasl_kerberos_keytab` and `sasl_kerberos_principal` child elements.

Example:

```xml
<!-- Kerberos-aware Kafka -->
<kafka>
  <security_protocol>SASL_PLAINTEXT</security_protocol>
  <sasl_kerberos_keytab>/home/kafkauser/kafkauser.keytab</sasl_kerberos_keytab>
  <sasl_kerberos_principal>kafkauser/kafkahost@EXAMPLE.COM</sasl_kerberos_principal>
</kafka>
```

## Virtual Columns {#virtual-columns}

- `_topic` — Kafka topic. Data type: `LowCardinality(String)`.
- `_key` — Key of the message. Data type: `String`.
- `_offset` — Offset of the message. Data type: `UInt64`.
- `_timestamp` — Timestamp of the message Data type: `Nullable(DateTime)`.
- `_timestamp_ms` — Timestamp in milliseconds of the message. Data type: `Nullable(DateTime64(3))`.
- `_partition` — Partition of Kafka topic. Data type: `UInt64`.
- `_headers.name` — Array of message's headers keys. Data type: `Array(String)`.
- `_headers.value` — Array of message's headers values. Data type: `Array(String)`.

Additional virtual columns when `kafka_handle_error_mode='stream'`:

- `_raw_message` - Raw message that couldn't be parsed successfully. Data type: `String`.
- `_error` - Exception message happened during failed parsing. Data type: `String`.

Note: `_raw_message` and `_error` virtual columns are filled only in case of exception during parsing, they are always empty when message was parsed successfully.

## Data formats support {#data-formats-support}

Kafka engine supports all [formats](../../../interfaces/formats.md) supported in ClickHouse.
The number of rows in one Kafka message depends on whether the format is row-based or block-based:

- For row-based formats the number of rows in one Kafka message can be controlled by setting `kafka_max_rows_per_message`.
- For block-based formats we cannot divide block into smaller parts, but the number of rows in one block can be controlled by general setting [max_block_size](/operations/settings/settings#max_block_size).

## Engine to store committed offsets in ClickHouse Keeper {#engine-to-store-committed-offsets-in-clickhouse-keeper}

<ExperimentalBadge/>

If `allow_experimental_kafka_offsets_storage_in_keeper` is enabled, then two more settings can be specified to the Kafka table engine:
 - `kafka_keeper_path` specifies the path to the table in ClickHouse Keeper
 - `kafka_replica_name` specifies the replica name in ClickHouse Keeper

Either both of the settings must be specified or neither of them. When both of them are specified, then a new, experimental Kafka engine will be used. The new engine doesn't depend on storing the committed offsets in Kafka, but stores them in ClickHouse Keeper. It still tries to commit the offsets to Kafka, but it only depends on those offsets when the table is created. In any other circumstances (table is restarted, or recovered after some error) the offsets stored in ClickHouse Keeper will be used as an offset to continue consuming messages from. Apart from the committed offset, it also stores how many messages were consumed in the last batch, so if the insert fails, the same amount of messages will be consumed, thus enabling deduplication if necessary.

Example:

```sql
CREATE TABLE experimental_kafka (key UInt64, value UInt64)
ENGINE = Kafka('localhost:19092', 'my-topic', 'my-consumer', 'JSONEachRow')
SETTINGS
  kafka_keeper_path = '/clickhouse/{database}/experimental_kafka',
  kafka_replica_name = 'r1'
SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;
```

Or to utilize the `uuid` and `replica` macros similarly to ReplicatedMergeTree:

```sql
CREATE TABLE experimental_kafka (key UInt64, value UInt64)
ENGINE = Kafka('localhost:19092', 'my-topic', 'my-consumer', 'JSONEachRow')
SETTINGS
  kafka_keeper_path = '/clickhouse/{database}/{uuid}',
  kafka_replica_name = '{replica}'
SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;
```

### Known limitations {#known-limitations}

As the new engine is experimental, it is not production ready yet. There are few known limitations of the implementation:
 - The biggest limitation is the engine doesn't support direct reading. Reading from the engine using materialized views and writing to the engine work, but direct reading doesn't. As a result, all direct `SELECT` queries will fail.
 - Rapidly dropping and recreating the table or specifying the same ClickHouse Keeper path to different engines might cause issues. As best practice you can use the `{uuid}` in `kafka_keeper_path` to avoid clashing paths.
 - To make repeatable reads, messages cannot be consumed from multiple partitions on a single thread. On the other hand, the Kafka consumers have to be polled regularly to keep them alive. As a result of these two objectives, we decided to only allow creating multiple consumers if `kafka_thread_per_consumer` is enabled, otherwise it is too complicated to avoid issues regarding polling consumers regularly.
 - Consumers created by the new storage engine do not show up in [`system.kafka_consumers`](../../../operations/system-tables/kafka_consumers.md) table.

**See Also**

- [Virtual columns](../../../engines/table-engines/index.md#table_engines-virtual_columns)
- [background_message_broker_schedule_pool_size](/operations/server-configuration-parameters/settings#background_message_broker_schedule_pool_size)
- [system.kafka_consumers](../../../operations/system-tables/kafka_consumers.md)
