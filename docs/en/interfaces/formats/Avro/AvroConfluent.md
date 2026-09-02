---
alias: []
description: 'Documentation for the AvroConfluent format'
input_format: true
keywords: ['AvroConfluent']
output_format: true
slug: /interfaces/formats/AvroConfluent
title: 'AvroConfluent'
doc_type: 'reference'
---

import DataTypesMatching from './_snippets/data-types-matching.md'

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[Apache Avro](https://avro.apache.org/) is a row-oriented serialization format that uses binary encoding for efficient data processing. The `AvroConfluent` format supports reading and writing Avro-encoded messages using the [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html) (or API-compatible services).

Each message uses the Confluent wire format: a magic byte (`0x00`) followed by a 4-byte big-endian schema ID, followed by the Avro binary datum. When reading, ClickHouse resolves the schema ID by querying the registry. When writing, ClickHouse registers the schema derived from the output columns and prepends the resulting ID to each row. Schemas are cached for optimal performance.

<a id="data-types-matching"></a>
## Data type mapping {#data-type-mapping}

<DataTypesMatching/>

## Format settings {#format-settings}

[//]: # "NOTE These settings can be set at a session-level, but this isn't common and documenting it too prominently can be confusing to users."

| Setting                                              | Description                                                                                         | Default |
|------------------------------------------------------|-----------------------------------------------------------------------------------------------------|---------|
| `input_format_avro_allow_missing_fields`             | Whether to use a default value instead of throwing an error when a field is not found in the schema. | `0`     |
| `input_format_avro_null_as_default`                  | Whether to use a default value instead of throwing an error when inserting a `null` value into a non-nullable column. |   `0`   |
| `format_avro_schema_registry_url`                    | The Confluent Schema Registry URL. For basic authentication, URL-encoded credentials can be included directly in the URL path. |         |
| `format_avro_schema_registry_connection_timeout`     | Connection timeout in seconds for the Schema Registry HTTP client (used for both schema fetch and registration). Must be greater than 0 and less than 600 (10 minutes). | `1`     |
| `format_avro_schema_registry_send_timeout`           | Send timeout in seconds for the Schema Registry HTTP client. Must be greater than 0 and less than 600 (10 minutes). | `1`     |
| `format_avro_schema_registry_receive_timeout`        | Receive timeout in seconds for the Schema Registry HTTP client. Must be greater than 0 and less than 600 (10 minutes). | `1`     |
| `output_format_avro_confluent_subject`               | For output: the subject name under which the schema is registered in the Schema Registry. Required when writing. |         |
| `output_format_avro_string_column_pattern`           | For output: regexp of String columns to serialize as Avro `string` (default is `bytes`). |         |

## Examples {#examples}

### Reading from Kafka {#reading-from-kafka}

To read an Avro-encoded Kafka topic using the [Kafka table engine](/engines/table-engines/integrations/kafka.md), use the `format_avro_schema_registry_url` setting to provide the URL of the schema registry.

```sql
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
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url = 'http://schema-registry-url';

SELECT * FROM topic1_stream;
```

### Writing to Kafka {#writing-to-kafka}

To write AvroConfluent messages to a Kafka topic, set both the schema registry URL and the subject name. The schema is automatically registered with the registry on the first write.

```sql
CREATE TABLE topic1_sink
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url = 'http://schema-registry-url',
output_format_avro_confluent_subject = 'topic1-value';

INSERT INTO topic1_sink VALUES ('hello', 'world');
```

#### Using basic authentication {#using-basic-authentication}

If your schema registry requires basic authentication (e.g., if you're using Confluent Cloud), you can provide URL-encoded credentials in the `format_avro_schema_registry_url` setting.

```sql
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
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url = 'https://<username>:<password>@schema-registry-url';
```

## Troubleshooting {#troubleshooting}

To monitor ingestion progress and debug errors with the Kafka consumer, you can query the [`system.kafka_consumers` system table](../../../operations/system-tables/kafka_consumers.md). If your deployment has multiple replicas (e.g., ClickHouse Cloud), you must use the [`clusterAllReplicas`](../../../sql-reference/table-functions/cluster.md) table function.

```sql
SELECT * FROM clusterAllReplicas('default',system.kafka_consumers)
ORDER BY assignments.partition_id ASC;
```

If you run into schema resolution issues, you can use [kafkacat](https://github.com/edenhill/kafkacat) with [clickhouse-local](/operations/utilities/clickhouse-local.md) to troubleshoot:

```bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local   --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select *  from table'
1 a
2 b
3 c
```
