---
alias: []
description: 'Documentation for the ProtobufConfluent format'
input_format: true
keywords: ['ProtobufConfluent']
output_format: false
slug: /interfaces/formats/ProtobufConfluent
title: 'ProtobufConfluent'
---

import DataTypesMatching from './_snippets/data-types-matching.md'

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✗      |       |

## Description {#description}

[Protocol Buffers (Protobuf)](https://developers.google.com/protocol-buffers) is a compact, efficient, and extensible binary serialization format developed by Google. The `ProtobufConfluent` format supports decoding messages serialized with Protobuf using the [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html) (or compatible services), following the [Confluent wire format](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format).

Each Protobuf message includes a schema ID prefix that ClickHouse uses to automatically retrieve the corresponding schema from the configured registry. Retrieved schemas are cached to improve performance.

<a id="data-types-matching"></a>
## Data type mapping {#data-type-mapping}

<DataTypesMatching/>

## Format settings {#format-settings}

| Setting                                        | Description                                                                                         | Default |
|-----------------------------------------------|-----------------------------------------------------------------------------------------------------|---------|
| `format_protobuf_schema_registry_url`         | The URL of the Confluent Schema Registry. Supports embedded basic authentication via URL-encoded credentials. |         |

## Examples {#examples}

### Using a schema registry {#using-a-schema-registry}

To read Protobuf-encoded messages from a Kafka topic using the [Kafka table engine](/engines/table-engines/integrations/kafka.md), specify the registry URL using the `format_protobuf_schema_registry_url` setting.

```sql
CREATE TABLE topic_protobuf
(
    value Int64,
    value2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_group_name = 'group1',
kafka_format = 'ProtobufConfluent',
format_protobuf_schema_registry_url = 'http://schema-registry-url';

SELECT * FROM topic_protobuf;
```

## Using basic authentication {#using-basic-authentication}

If your Schema Registry requires authentication (e.g. Confluent Cloud), provide credentials in the URL:

```sql
CREATE TABLE topic_protobuf
(
    value Int64,
    value2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_group_name = 'group1',
kafka_format = 'ProtobufConfluent',
format_protobuf_schema_registry_url = 'https://username:password@schema-registry-url';
```
