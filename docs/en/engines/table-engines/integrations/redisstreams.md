---
sidebar_position: 15
sidebar_label: Redis Streams
---

# Redis Streams Engine {#redisstreams-engine}

This engine allows integrating ClickHouse with [Redis Streams](https://redis.io/docs/manual/data-types/streams/).

`Redis Streams` lets you:

- Add messages to streams.
- Process streams at any point in time 
- Process new messages as they become available.

## Creating a Table {#table_engine-redisstreams-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Redis Streams SETTINGS
    redis_broker = 'host:port',
    redis_stream_list = 'stream_foo,stream_boo',
    redis_group_name = 'my_group'[,]
    [redis_common_consumer_id = 'my_consumer',]
    [redis_num_consumers = N,]
    [redis_manage_consumer_groups = true,]
    [redis_consumer_groups_start_id = '0-0',]
    [redis_ack_every_batch = false,]
    [redis_ack_on_select = true,]
    [redis_poll_timeout_ms = N,]
    [redis_poll_max_batch_size = N,]
    [redis_claim_max_batch_size = N,]
    [redis_min_time_for_claim = N,]
    [redis_max_block_size = N,]
    [redis_flush_interval_ms = N,]
    [redis_thread_per_consumer = true]
    [redis_password = 'clickhouse']
```

Required parameters:

-   `redis_broker` – host:port (for example, `localhost:5672`).
-   `redis_stream_list` – A list of Redis Streams streams.
-   `redis_group_name` – Name of the consumer group, which engine will use to read messages.

Optional parameters:

- `redis_common_consumer_id` – Common identifier for consumers. Must be unique within group. They will be enumerated within the engine.
- `redis_num_consumers` – The number of consumers per table. Default: `1`. Specify more consumers if the throughput of one consumer is insufficient.
- `redis_manage_consumer_groups` – Create consumer groups on engine startup and delete them at the end. Default: `false`.
- `redis_consumer_groups_start_id` – The message id from which the consumer groups will start to read. Default: `$`. This means it will read only messages which appear after table creation.
- `redis_ack_every_batch` – Ack every consumed and handled batch instead of a single commit after writing a whole block. Default: `false`.
- `redis_ack_on_select` – Ack messages after select query. Default: `true`.
- `redis_poll_timeout_ms` - Timeout for single poll from Redis.
- `redis_poll_max_batch_size` - Maximum amount of messages to be read in a single Redis poll.
- `redis_claim_max_batch_size` - Maximum amount of messages to be claimed in a single Redis poll.
- `redis_min_time_for_claim` – Minimum time in milliseconds after which consumers will start to claim messages. Default: `10000`.
- `rabbitmq_max_block_size` - Number of row collected by poll(s) for flushing data from Redis.
- `rabbitmq_flush_interval_ms` - Timeout for flushing data read from Redis.
- `redis_thread_per_consumer` - Provide independent thread for each consumer. Default: `false`.
- `redis_password`

All messages in Redis Streams are already key-value.
That is why our implementation uses JSONEachRow format internally.
So JSONEachRow settings can be added along with rabbitmq-related settings.

Example:

``` sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64,
    date DateTime
  ) ENGINE = Redis Streams 
    SETTINGS
        redis_broker = 'host:port',
        redis_stream_list = 'stream,
        redis_group_name = 'my_group',
        date_time_input_format = 'best_effort';
```

The Redis Streams server configuration can be added using the ClickHouse config file.
 More specifically you can add Redis password for Redis Streams engine:

``` xml
<redis>
    <password>clickhouse</password>
</redis>
```

## Description {#description}

`SELECT` is not particularly useful for reading messages (except for debugging), because each message can be read only once. It is more practical to create real-time threads using [materialized views](../../../sql-reference/statements/create/view.md). To do this:

1.  Use the engine to create a Redis Streams consumer and consider it a data stream.
2.  Create a table with the desired structure.
3.  Create a materialized view that converts data from the engine and puts it into a previously created table.

When the `MATERIALIZED VIEW` joins the engine, it starts collecting data in the background. This allows you to continually receive messages from Redis Streams and convert them to the required format using `SELECT`.
One Redis Streams table can have as many materialized views as you like, they do not read data from the table directly, but receive new records (in blocks), this way you can write to several tables with different detail level (with grouping - aggregation and without).

Example:

``` sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = Redis Streams 
    SETTINGS
        redis_broker = 'localhost:6379',
        redis_stream_list = 'stream,
        redis_group_name = 'my_group',

  CREATE TABLE daily (key UInt64, value UInt64)
    ENGINE = MergeTree() ORDER BY key;

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT key, value FROM queue;

  SELECT key, value FROM daily ORDER BY key;
```

To stop receiving streams data or to change the conversion logic, detach the materialized view:

``` sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

If you want to change the target table by using `ALTER`, we recommend disabling the material view to avoid discrepancies between the target table and the data from the view.

## Virtual Columns {#virtual-columns}

-   `_stream` - Redis Streams stream name.
-   `_key` - Message's ID.
-   `_timestamp` - Message's timestamp (first part of ID).
-   `_sequence_number` - Message's sequence number (second part of ID).
-   `_group` - Name of consumer group which read this message.
-   `_consumer` - Name of consumer which read this message.

[Original article](https://clickhouse.com/docs/en/engines/table-engines/integrations/redisstreams/) <!--hide-->
