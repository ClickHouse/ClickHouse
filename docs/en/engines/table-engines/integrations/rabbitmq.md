---
toc_priority: 6
toc_title: RabbitMQ
---

# RabbitMQ Engine {#rabbitmq-engine}

This engine allows integrating ClickHouse with [RabbitMQ](https://www.rabbitmq.com).

RabbitMQ lets you:

-   Publish or subscribe to data flows.
-   Process streams as they become available.

## Creating a Table {#table_engine-rabbitmq-creating-a-table}

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
    [rabbitmq_num_consumers = N,]
    [rabbitmq_num_queues = N,]
    [rabbitmq_transactional_channel = 0]
```

Required parameters:

-   `rabbitmq_host_port` – host:port (for example, `localhost:5672`).
-   `rabbitmq_exchange_name` – RabbitMQ exchange name.
-   `rabbitmq_format` – Message format. Uses the same notation as the SQL `FORMAT` function, such as `JSONEachRow`. For more information, see the [Formats](../../../interfaces/formats.md) section.

Optional parameters:

-   `rabbitmq_exchange_type` – The type of RabbitMQ exchange: `direct`, `fanout`, `topic`, `headers`, `consistent-hash`. Default: `fanout`.
-   `rabbitmq_routing_key_list` – A comma-separated list of routing keys.
-   `rabbitmq_row_delimiter` – Delimiter character, which ends the message.
-   `rabbitmq_num_consumers` – The number of consumers per table. Default: `1`. Specify more consumers if the throughput of one consumer is insufficient.
-   `rabbitmq_num_queues` – The number of queues per consumer. Default: `1`. Specify more queues if the capacity of one queue per consumer is insufficient. Single queue can contain up to 50K messages at the same time.
-   `rabbitmq_transactional_channel` – Wrap insert queries in transactions. Default: `0`.

Required configuration:

The RabbitMQ server configuration should be added using the ClickHouse config file.

``` xml
 <rabbitmq>
    <username>root</username>
    <password>clickhouse</password>
 </rabbitmq>
```

Example:

``` sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = 'localhost:5672',
                            rabbitmq_exchange_name = 'exchange1',
                            rabbitmq_format = 'JSONEachRow',
                            rabbitmq_num_consumers = 5;
```

## Description {#description}

`SELECT` is not particularly useful for reading messages (except for debugging), because each message can be read only once. It is more practical to create real-time threads using materialized views. To do this:

1.  Use the engine to create a RabbitMQ consumer and consider it a data stream.
2.  Create a table with the desired structure.
3.  Create a materialized view that converts data from the engine and puts it into a previously created table.

When the `MATERIALIZED VIEW` joins the engine, it starts collecting data in the background. This allows you to continually receive messages from RabbitMQ and convert them to the required format using `SELECT`.
One RabbitMQ table can have as many materialized views as you like.

Data can be channeled based on `rabbitmq_exchange_type` and the specified `rabbitmq_routing_key_list`.
There can be no more than one exchange per table. One exchange can be shared between multiple tables - it enables routing into multiple tables at the same time.

Exchange type options:

-   `direct` - Routing is based on exact matching of keys. Example table key list: `key1,key2,key3,key4,key5`, message key can eqaul any of them.
-   `fanout` - Routing to all tables (where exchange name is the same) regardless of the keys.
-   `topic` - Routing is based on patterns with dot-separated keys. Examples: `*.logs`, `records.*.*.2020`, `*.2018,*.2019,*.2020`.
-   `headers` - Routing is based on `key=value` matches with a setting `x-match=all` or `x-match=any`. Example table key list: `x-match=all,format=logs,type=report,year=2020`.
-   `consistent-hash` - Data is evenly distributed between all bound tables (where exchange name is the same). Note that this exchange type must be enabled with RabbitMQ plugin: `rabbitmq-plugins enable rabbitmq_consistent_hash_exchange`.

If exchange type is not specified, then default is `fanout` and routing keys for data publishing must be randomized in range `[1, num_consumers]` for every message/batch (or in range `[1, num_consumers * num_queues]` if `rabbitmq_num_queues` is set). This table configuration works quicker then any other, especially when `rabbitmq_num_consumers` and/or `rabbitmq_num_queues` parameters are set.

If `rabbitmq_num_consumers` and/or `rabbitmq_num_queues` parameters are specified along with `rabbitmq_exchange_type`, then:

-   `rabbitmq-consistent-hash-exchange` plugin must be enabled.
-   `message_id` property of the published messages must be specified (unique for each message/batch).

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
    ENGINE = MergeTree();

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT key, value FROM queue;

  SELECT key, value FROM daily ORDER BY key;
```
