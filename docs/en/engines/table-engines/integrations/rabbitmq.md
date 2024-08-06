---
slug: /en/engines/table-engines/integrations/rabbitmq
sidebar_position: 170
sidebar_label: RabbitMQ
---

# RabbitMQ Engine

This engine allows integrating ClickHouse with [RabbitMQ](https://www.rabbitmq.com).

`RabbitMQ` lets you:

- Publish or subscribe to data flows.
- Process streams as they become available.

## Creating a Table {#creating-a-table}

``` sql
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
    [rabbitmq_deadletter_exchange = 'dl-exchange',]
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

Required parameters:

- `rabbitmq_host_port` – host:port (for example, `localhost:5672`).
- `rabbitmq_exchange_name` – RabbitMQ exchange name.
- `rabbitmq_format` – Message format. Uses the same notation as the SQL `FORMAT` function, such as `JSONEachRow`. For more information, see the [Formats](../../../interfaces/formats.md) section.

Optional parameters:

- `rabbitmq_exchange_type` – The type of RabbitMQ exchange: `direct`, `fanout`, `topic`, `headers`, `consistent_hash`. Default: `fanout`.
- `rabbitmq_routing_key_list` – A comma-separated list of routing keys.
- `rabbitmq_schema` – Parameter that must be used if the format requires a schema definition. For example, [Cap’n Proto](https://capnproto.org/) requires the path to the schema file and the name of the root `schema.capnp:Message` object.
- `rabbitmq_num_consumers` – The number of consumers per table. Specify more consumers if the throughput of one consumer is insufficient. Default: `1`
- `rabbitmq_num_queues` – Total number of queues. Increasing this number can significantly improve performance. Default: `1`.
- `rabbitmq_queue_base` - Specify a hint for queue names. Use cases of this setting are described below.
- `rabbitmq_deadletter_exchange` - Specify name for a [dead letter exchange](https://www.rabbitmq.com/dlx.html). You can create another table with this exchange name and collect messages in cases when they are republished to dead letter exchange. By default dead letter exchange is not specified.
- `rabbitmq_persistent` - If set to 1 (true), in insert query delivery mode will be set to 2 (marks messages as 'persistent'). Default: `0`.
- `rabbitmq_skip_broken_messages` – RabbitMQ message parser tolerance to schema-incompatible messages per block. If `rabbitmq_skip_broken_messages = N` then the engine skips *N* RabbitMQ messages that cannot be parsed (a message equals a row of data). Default: `0`.
- `rabbitmq_max_block_size` - Number of row collected before flushing data from RabbitMQ. Default: [max_insert_block_size](../../../operations/settings/settings.md#max_insert_block_size).
- `rabbitmq_flush_interval_ms` - Timeout for flushing data from RabbitMQ. Default: [stream_flush_interval_ms](../../../operations/settings/settings.md#stream-flush-interval-ms).
- `rabbitmq_queue_settings_list` - allows to set RabbitMQ settings when creating a queue. Available settings: `x-max-length`, `x-max-length-bytes`, `x-message-ttl`, `x-expires`, `x-priority`, `x-max-priority`, `x-overflow`, `x-dead-letter-exchange`, `x-queue-type`. The `durable` setting is enabled automatically for the queue.
- `rabbitmq_address` - Address for connection. Use ether this setting or `rabbitmq_host_port`.
- `rabbitmq_vhost` - RabbitMQ vhost. Default: `'\'`.
- `rabbitmq_queue_consume` - Use user-defined queues and do not make any RabbitMQ setup: declaring exchanges, queues, bindings. Default: `false`.
- `rabbitmq_username` - RabbitMQ username.
- `rabbitmq_password` - RabbitMQ password.
- `reject_unhandled_messages` - Reject messages (send RabbitMQ negative acknowledgement) in case of errors. This setting is automatically enabled if there is a `x-dead-letter-exchange` defined in `rabbitmq_queue_settings_list`.
- `rabbitmq_commit_on_select` - Commit messages when select query is made. Default: `false`.
- `rabbitmq_max_rows_per_message` — The maximum number of rows written in one RabbitMQ message for row-based formats. Default : `1`.
- `rabbitmq_empty_queue_backoff_start` — A start backoff point to reschedule read if the rabbitmq queue is empty.
- `rabbitmq_empty_queue_backoff_end` — An end backoff point to reschedule read if the rabbitmq queue is empty.
- `rabbitmq_handle_error_mode` — How to handle errors for RabbitMQ engine. Possible values: default (the exception will be thrown if we fail to parse a message), stream (the exception message and raw message will be saved in virtual columns `_error` and `_raw_message`).



  * [ ] SSL connection:

Use either `rabbitmq_secure = 1` or `amqps` in connection address: `rabbitmq_address = 'amqps://guest:guest@localhost/vhost'`.
The default behaviour of the used library is not to check if the created TLS connection is sufficiently secure. Whether the certificate is expired, self-signed, missing or invalid: the connection is simply permitted. More strict checking of certificates can possibly be implemented in the future.

Also format settings can be added along with rabbitmq-related settings.

Example:

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

The RabbitMQ server configuration should be added using the ClickHouse config file.

Required configuration:

``` xml
 <rabbitmq>
    <username>root</username>
    <password>clickhouse</password>
 </rabbitmq>
```

Additional configuration:

``` xml
 <rabbitmq>
    <vhost>clickhouse</vhost>
 </rabbitmq>
```

## Description {#description}

`SELECT` is not particularly useful for reading messages (except for debugging), because each message can be read only once. It is more practical to create real-time threads using [materialized views](../../../sql-reference/statements/create/view.md). To do this:

1.  Use the engine to create a RabbitMQ consumer and consider it a data stream.
2.  Create a table with the desired structure.
3.  Create a materialized view that converts data from the engine and puts it into a previously created table.

When the `MATERIALIZED VIEW` joins the engine, it starts collecting data in the background. This allows you to continually receive messages from RabbitMQ and convert them to the required format using `SELECT`.
One RabbitMQ table can have as many materialized views as you like.

Data can be channeled based on `rabbitmq_exchange_type` and the specified `rabbitmq_routing_key_list`.
There can be no more than one exchange per table. One exchange can be shared between multiple tables - it enables routing into multiple tables at the same time.

Exchange type options:

- `direct` - Routing is based on the exact matching of keys. Example table key list: `key1,key2,key3,key4,key5`, message key can equal any of them.
- `fanout` - Routing to all tables (where exchange name is the same) regardless of the keys.
- `topic` - Routing is based on patterns with dot-separated keys. Examples: `*.logs`, `records.*.*.2020`, `*.2018,*.2019,*.2020`.
- `headers` - Routing is based on `key=value` matches with a setting `x-match=all` or `x-match=any`. Example table key list: `x-match=all,format=logs,type=report,year=2020`.
- `consistent_hash` - Data is evenly distributed between all bound tables (where the exchange name is the same). Note that this exchange type must be enabled with RabbitMQ plugin: `rabbitmq-plugins enable rabbitmq_consistent_hash_exchange`.

Setting `rabbitmq_queue_base` may be used for the following cases:

- to let different tables share queues, so that multiple consumers could be registered for the same queues, which makes a better performance. If using `rabbitmq_num_consumers` and/or `rabbitmq_num_queues` settings, the exact match of queues is achieved in case these parameters are the same.
- to be able to restore reading from certain durable queues when not all messages were successfully consumed. To resume consumption from one specific queue - set its name in `rabbitmq_queue_base` setting and do not specify `rabbitmq_num_consumers` and `rabbitmq_num_queues` (defaults to 1). To resume consumption from all queues, which were declared for a specific table - just specify the same settings: `rabbitmq_queue_base`, `rabbitmq_num_consumers`, `rabbitmq_num_queues`. By default, queue names will be unique to tables.
- to reuse queues as they are declared durable and not auto-deleted. (Can be deleted via any of RabbitMQ CLI tools.)

To improve performance, received messages are grouped into blocks the size of [max_insert_block_size](../../../operations/server-configuration-parameters/settings.md#settings-max_insert_block_size). If the block wasn’t formed within [stream_flush_interval_ms](../../../operations/server-configuration-parameters/settings.md) milliseconds, the data will be flushed to the table regardless of the completeness of the block.

If `rabbitmq_num_consumers` and/or `rabbitmq_num_queues` settings are specified along with `rabbitmq_exchange_type`, then:

- `rabbitmq-consistent-hash-exchange` plugin must be enabled.
- `message_id` property of the published messages must be specified (unique for each message/batch).

For insert query there is message metadata, which is added for each published message: `messageID` and `republished` flag (true, if published more than once) - can be accessed via message headers.

Do not use the same table for inserts and materialized views.

Example:

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

## Virtual Columns {#virtual-columns}

- `_exchange_name` - RabbitMQ exchange name. Data type: `String`.
- `_channel_id` - ChannelID, on which consumer, who received the message, was declared. Data type: `String`.
- `_delivery_tag` - DeliveryTag of the received message. Scoped per channel. Data type: `UInt64`.
- `_redelivered` - `redelivered` flag of the message. Data type: `UInt8`.
- `_message_id` - messageID of the received message; non-empty if was set, when message was published. Data type: `String`.
- `_timestamp` - timestamp of the received message; non-empty if was set, when message was published. Data type: `UInt64`.

Additional virtual columns when `kafka_handle_error_mode='stream'`:

- `_raw_message` - Raw message that couldn't be parsed successfully. Data type: `Nullable(String)`.
- `_error` - Exception message happened during failed parsing. Data type: `Nullable(String)`.

Note: `_raw_message` and `_error` virtual columns are filled only in case of exception during parsing, they are always `NULL` when message was parsed successfully.

## Caveats {#caveats}

Even though you may specify [default column expressions](/docs/en/sql-reference/statements/create/table.md/#default_values) (such as `DEFAULT`, `MATERIALIZED`, `ALIAS`) in the table definition, these will be ignored. Instead, the columns will be filled with their respective default values for their types.

## Data formats support {#data-formats-support}

RabbitMQ engine supports all [formats](../../../interfaces/formats.md) supported in ClickHouse.
The number of rows in one RabbitMQ message depends on whether the format is row-based or block-based:

- For row-based formats the number of rows in one RabbitMQ message can be controlled by setting `rabbitmq_max_rows_per_message`.
- For block-based formats we cannot divide block into smaller parts, but the number of rows in one block can be controlled by general setting [max_block_size](../../../operations/settings/settings.md#setting-max_block_size).
