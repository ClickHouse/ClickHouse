Kafka
-----

A table engine backed by Apache Kafka, a streaming platform having three key capabilities:

1. It lets you publish and subscribe to streams of records. In this respect it is similar to a message queue or enterprise messaging system.
2. It lets you store streams of records in a fault-tolerant way.
3. It lets you process streams of records as they occur.

.. code-block:: text

  Kafka(broker_list, topic_list, group_name, format[, schema])

Engine parameters:

broker_list
  A comma-separated list of brokers (``localhost:9092``).

topic_list
  List of Kafka topics to consume (``my_topic``).

group_name
  Kafka consumer group name (``group1``). Read offsets are tracked for each consumer group, if you want to consume messages exactly once across cluster, you should use the same group name.

format
  Name of the format used to deserialize messages. It accepts the same values as the ``FORMAT`` SQL statement, for example ``JSONEachRow``.

schema
  Optional schema value for formats that require a schema to interpret consumed messages, for example Cap'n Proto format requires
  a path to schema file and root object - ``schema:Message``. Self-describing formats such as JSON don't require any schema.

Example:

.. code-block:: sql

  CREATE TABLE queue (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1', 'JSONEachRow');

  SELECT * FROM queue LIMIT 5;

The consumed messages are tracked automatically in the background, so each message will be read exactly once in a single consumer group. If you want to consume the same set of messages twice, you can create a copy of the table with a different ``group_name``. The consumer group is elastic and synchronised across the cluster. For example, if you have 10 topic/partitions and 5 instances of the table across cluster, it will automatically assign 2 topic/partitions per instace. If you detach a Kafka engine table (or create new), it will rebalance topic/partition assignments automatically. See `Kafka Introduction <https://kafka.apache.org/intro>`_ for more information about how this works.

Reading messages using SELECT is not very useful (except for troubleshooting), because each message can be read only once.
The table engine is typically used to build real-time ingestion pipelines using MATERIALIZED VIEW. It works like this:

1. You create a Kafka consumer with a Kafka engine. This is the data stream.
2. You create an arbitrary table with the desired data schema.
3. You create a MATERIALIZED VIEW that transforms the data from Kafka, and materializes it into your desired table.


When a MATERIALIZED VIEW is attached to a Kafka table engine, it will start automatically consuming messages in the background, and push them into the attached views. This allows you to continuously ingest messages from Kafka and transform them using the SELECT statement to describe transformation.

Example:

.. code-block:: sql

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

The messages are streamed into the attached view immediately in the same way a continuous stream of INSERT statement would. To improve performance, consumed messages are squashed into batches of ``max_insert_block_size``. If the message batch cannot be completed within ``stream_flush_interval_ms`` period (by default 7500ms), it will be flushed to ensure time bounded insertion time.

In order to stop topic consumption, or alter the transformation logc, you simply detach the MATERIALIZED VIEW:

.. code-block:: sql

  DETACH TABLE consumer;
  ATTACH MATERIALIZED VIEW consumer;

Note: When you're performing ALTERs on target table, it's recommended to detach materializing views to prevent a mismatch between the current schema and the result of MATERIALIZED VIEWS.
