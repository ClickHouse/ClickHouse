---
alias: []
description: 'Documentation for the AvroConfluent format'
input_format: true
keywords: ['AvroConfluent']
output_format: false
slug: /interfaces/formats/AvroConfluent
title: 'AvroConfluent'
doc_type: 'reference'
---

import DataTypesMatching from './_snippets/data-types-matching.md'

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✗      |       |

## Description {#description}

[Apache Avro](https://avro.apache.org/) is a row-oriented serialization format that uses binary encoding for efficient data processing. The `AvroConfluent` format supports decoding single-object, Avro-encoded Kafka messages serialized using the [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html) (or API-compatible services).

Each Avro message embeds a schema ID that ClickHouse automatically resolves by querying the configured schema registry. Once resolved, schemas are cached for optimal performance.

<a id="data-types-matching"></a>
## Data type mapping {#data-type-mapping}

<DataTypesMatching/>

## Format settings {#format-settings}

[//]: # "NOTE These settings can be set at a session-level, but this isn't common and documenting it too prominently can be confusing to users."

| Setting                                     | Description                                                                                         | Default |
|---------------------------------------------|-----------------------------------------------------------------------------------------------------|---------|
| `input_format_avro_allow_missing_fields`    | Whether to use a default value instead of throwing an error when a field is not found in the schema. | `0`     |
| `input_format_avro_null_as_default`         | Whether to use a default value instead of throwing an error when inserting a `null` value into a non-nullable column. |   `0`   |
| `format_avro_schema_registry_url`           | The Confluent Schema Registry URL. For basic authentication, URL-encoded credentials can be included directly in the URL path. |         |

## Examples {#examples}

### Using a schema registry {#using-a-schema-registry}

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
