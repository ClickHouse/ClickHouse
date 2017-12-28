# Kafka

The engine works with [Apache Kafka](http://kafka.apache.org/).

Kafka lets you:

- Publish or subscribe to data flows.
- Organize fault-tolerant storage.
- Process streams as they become available.

```
Kafka(broker_list, topic_list, group_name, format[, schema])
```

Parameters:

- `broker_list` – A comma-separated list of brokers (`localhost:9092`).
- `topic_list` – A list of Kafka topics (`my_topic`).
- `group_name` – A group of Kafka consumers (`group1`). Reading margins are tracked for each group separately. If you don't want messages to be duplicated in the cluster, use the same group name everywhere.
- `--format` – Message format. Uses the same notation as the SQL ` FORMAT` function, such as ` JSONEachRow`.
- `schema` – An optional parameter that must be used if the format requires a schema definition. For example, [Cap'n Proto](https://capnproto.org/)  requires the path to the schema file and the name of the root ` schema.capnp:Message` object.

Example:

```sql
CREATE TABLE queue (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1', 'JSONEachRow');

  SELECT * FROM queue LIMIT 5;
```

The delivered messages are tracked automatically, so each message in a group is only counted once. If you want to get the data twice, then create a copy of the table with another group name.

Groups are flexible and synced on the cluster. For instance, if you have 10 topics and 5 copies of a table in a cluster, then each copy gets 2 topics. If the number of copies changes, the topics are redistributed across the copies automatically. For more information, see [http://kafka.apache.org/intro](http://kafka.apache.org/intro).

`SELECT` is not particularly useful for reading messages (except for debugging), because each message can be read only once. It is more practical to create real-time threads using materialized views. For this purpose, the following was done:

1. Use the engine to create a Kafka consumer and consider it a data stream.
2. Create a table with the desired structure.
3. Create a materialized view that converts data from the engine and puts it into a previously created table.

When the `MATERIALIZED VIEW` joins the engine, it starts collecting data in the background. This allows you to continually receive messages from Kafka and convert them to the required format using `SELECT`

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
    AS SELECT toDate(toDateTime(timestamp)) AS day, level, count() as total
    FROM queue GROUP BY day, level;

SELECT level, sum(total) FROM daily GROUP BY level;
```

To improve performance, received messages are grouped into blocks the size of [max_block_size](../operations/settings/settings.md#settings-settings-max_insert_block_size). If the block wasn't formed within [ stream_flush_interval_ms](../operations/settings/settings.md#settings-settings_stream_flush_interval_ms) milliseconds, the data will be flushed to the table regardless of the completeness of the block.

To stop receiving topic data or to change the conversion logic, detach the materialized view:

```
DETACH TABLE consumer;
ATTACH MATERIALIZED VIEW consumer;
```

If you want to change the target table by using ` ALTER`materialized view, we recommend disabling the material view to avoid discrepancies between the target table and the data from the view.

