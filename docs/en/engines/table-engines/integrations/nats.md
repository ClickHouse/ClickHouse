---
sidebar_position: 14
sidebar_label: NATS
---

# NATS Engine {#redisstreams-engine}

This engine allows integrating ClickHouse with [NATS](https://nats.io/).

`NATS` lets you:

- Publish or subcribe to message subjects.
- Process new messages as they become available.

## Creating a Table {#table_engine-redisstreams-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = NATS SETTINGS
    nats_url = 'host:port',
    nats_subjects = 'subject1,subject2,...',
    nats_format = 'data_format'[,]
    [nats_row_delimiter = 'delimiter_symbol',]
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
    [nats_password = 'password']
    [redis_password = 'clickhouse']
```

Required parameters:

-   `nats_url` – host:port (for example, `localhost:5672`)..
-   `nats_subjects` – List of subject for NATS table to subscribe/publsh to. Supports wildcard subjects like `foo.*.bar` or `baz.>`
-   `nats_format` – Message format. Uses the same notation as the SQL `FORMAT` function, such as `JSONEachRow`. For more information, see the [Formats](../../../interfaces/formats.md) section.

Optional parameters:

- `nats_row_delimiter` – Delimiter character, which ends the message.
- `nats_schema` – Parameter that must be used if the format requires a schema definition. For example, [Cap’n Proto](https://capnproto.org/) requires the path to the schema file and the name of the root `schema.capnp:Message` object.
- `nats_num_consumers` – The number of consumers per table. Default: `1`. Specify more consumers if the throughput of one consumer is insufficient.
- `nats_queue_group` – Name for queue group of NATS subscribers. Default is the table name.
- `nats_max_reconnect` – Maximum amount of reconnection attempts per try to connect to NATS. Default: `5`.
- `nats_reconnect_wait` – Amount of time in milliseconds to sleep between each reconnect attempt. Default: `5000`.
- `nats_server_list` - Server list for connection. Can be specified to connect to NATS cluster.
- `nats_skip_broken_messages` - NATS message parser tolerance to schema-incompatible messages per block. Default: `0`. If `nats_skip_broken_messages = N` then the engine skips *N* RabbitMQ messages that cannot be parsed (a message equals a row of data).
- `nats_max_block_size` - Number of row collected by poll(s) for flushing data from NATS.
- `nats_flush_interval_ms` - Timeout for flushing data read from NATS.
- `nats_username` - NATS username.
- `nats_password` - NATS password.
- `nats_token` - NATS auth token.

SSL connection:

For secure connection use `nats_secure = 1`.
The default behaviour of the used library is not to check if the created TLS connection is sufficiently secure. Whether the certificate is expired, self-signed, missing or invalid: the connection is simply permitted. More strict checking of certificates can possibly be implemented in the future.

Writing to NATS table:

If table reads only from one subject, any insert will publish to the same subject.
However, if table reads from multiple subjects, we need to specify which subject we want to publish to.
That is why whenever inserting into table with multiple subjects, setting `stream_like_engine_insert_queue` is needed.
You can select one of the subjects the table reads from and publish your data there. For example:

``` sql
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

Also format settings can be added along with nats-related settings.

Example:

``` sql
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

The NATS server configuration can be added using the ClickHouse config file.
 More specifically you can add Redis password for NATS engine:

``` xml
<nats>
    <user>click</user>
    <password>house</password>
    <token>clickhouse</token>
</nats>
```

## Description {#description}

`SELECT` is not particularly useful for reading messages (except for debugging), because each message can be read only once. It is more practical to create real-time threads using [materialized views](../../../sql-reference/statements/create/view.md). To do this:

1.  Use the engine to create a NATS consumer and consider it a data stream.
2.  Create a table with the desired structure.
3.  Create a materialized view that converts data from the engine and puts it into a previously created table.

When the `MATERIALIZED VIEW` joins the engine, it starts collecting data in the background. This allows you to continually receive messages from NATS and convert them to the required format using `SELECT`.
One NATS table can have as many materialized views as you like, they do not read data from the table directly, but receive new records (in blocks), this way you can write to several tables with different detail level (with grouping - aggregation and without).

Example:

``` sql
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

To stop receiving streams data or to change the conversion logic, detach the materialized view:

``` sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

If you want to change the target table by using `ALTER`, we recommend disabling the material view to avoid discrepancies between the target table and the data from the view.

## Virtual Columns {#virtual-columns}

-   `_subject` - NATS message subject.

[Original article](https://clickhouse.com/docs/en/engines/table-engines/integrations/nats/) <!--hide-->
