---
title : AvroConfluent
slug : /en/interfaces/formats/AvroConfluent
keywords : [AvroConfluent]
---

## Description

AvroConfluent supports decoding single-object Avro messages commonly used with [Kafka](https://kafka.apache.org/) and [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html).
Each Avro message embeds a schema ID that can be resolved to the actual schema with the help of the Schema Registry.
Schemas are cached once resolved.

## Data Types Matching {#data_types-matching-1}

The data types matching for the ClickHouse format are the same as for [Avro](/docs/en/interfaces/formats/Avro).

## Example Usage

To quickly verify schema resolution you can use [kafkacat](https://github.com/edenhill/kafkacat) with [clickhouse-local](/docs/en/operations/utilities/clickhouse-local.md):

``` bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local   --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select *  from table'
1 a
2 b
3 c
```

To use `AvroConfluent` with [Kafka](/docs/en/engines/table-engines/integrations/kafka.md):

``` sql
CREATE TABLE topic1_stream
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_group_name = 'group1',
kafka_format = 'AvroConfluent';

-- for debug purposes you can set format_avro_schema_registry_url in a session.
-- this way cannot be used in production
SET format_avro_schema_registry_url = 'http://schema-registry';

SELECT * FROM topic1_stream;
```

## Format Settings

The Schema Registry URL is configured with [format_avro_schema_registry_url](/docs/en/operations/settings/settings-formats.md/#format_avro_schema_registry_url).

:::note
Setting `format_avro_schema_registry_url` needs to be configured in `users.xml` to maintain it’s value after a restart. Also you can use the `format_avro_schema_registry_url` setting of the `Kafka` table engine.
:::