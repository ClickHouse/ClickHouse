---
description: 'System table containing information about messages
  received via a streaming engine and parsed with errors.'
keywords: ['system table', 'dead_letter']
slug: /en/operations/system-tables/dead_letter
title: 'system.dead_letter'
---

Contains information about messages received via a streaming engine and parsed with errors. Currently implemented for Kafka and RabbitMQ.

Logging is enabled by specifying `dead_letter` for the engine specific `handle_error_mode` setting.

The flushing period of data is set in `flush_interval_milliseconds` parameter of the [dead_letter](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dead_letter) server settings section. To force flushing, use the [SYSTEM FLUSH LOGS](../../sql-reference/statements/system.md#query_language-system-flush_logs) query.

ClickHouse does not delete data from the table automatically. See [Introduction](../../operations/system-tables/index.md#system-tables-introduction) for more details.

Columns:

- `stream_type` ([Enum8](../../sql-reference/data-types/enum.md)) - Stream type. Possible values: `Kafka` and `RabbitMQ`.
- `event_date` ([Date](../../sql-reference/data-types/date.md)) - Message consuming date.
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) - Message consuming date and time.
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) - Message consuming time with microseconds precision.
- `database_name` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) - ClickHouse database the streaming table belongs to.
- `table_name` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) - ClickHouse table name.
- `error` ([String](../../sql-reference/data-types/string.md)) - Error text.
- `raw_message` ([String](../../sql-reference/data-types/string.md)) - Message body.
- `kafka_topic_name` ([String](../../sql-reference/data-types/string.md)) - Kafka topic name.
- `kafka_partition` ([UInt64](../../sql-reference/data-types/int-uint.md)) - Kafka partition of the topic.
- `kafka_offset` ([UInt64](../../sql-reference/data-types/int-uint.md)) - Kafka offset of the message.
- `kafka_key` ([String](../../sql-reference/data-types/string.md)) - Kafka key of the message.
- `rabbitmq_exchange_name` ([String](../../sql-reference/data-types/string.md)) - RabbitMQ exchange name.
- `rabbitmq_message_id` ([String](../../sql-reference/data-types/string.md)) - RabbitMQ message id.
- `rabbitmq_message_timestamp` ([DateTime](../../sql-reference/data-types/datetime.md)) - RabbitMQ message timestamp.
- `rabbitmq_message_redelivered` ([UInt8](../../sql-reference/data-types/int-uint.md)) - RabbitMQ redelivered flag.
- `rabbitmq_message_delivery_tag` ([UInt64](../../sql-reference/data-types/int-uint.md)) - RabbitMQ delivery tag.
- `rabbitmq_channel_id` ([String](../../sql-reference/data-types/string.md)) - RabbitMQ channel id.


**Example**

Query:

``` sql
SELECT * FROM system.dead_letter LIMIT 1 \G;
```

Result:

``` text
Row 1:
──────
stream_type:                   Kafka
event_date:                    2025-05-01
event_time:                    2025-05-01 10:34:53
event_time_microseconds:       2025-05-01 10:34:53.910773
database_name:                 default
table_name:                    kafka
error:                         Cannot parse input: expected '\t' before: 'qwertyuiop': (at row 1)
:
Row 1:
Column 0,   name: key,   type: UInt64, ERROR: text "qwertyuiop" is not like UInt64


raw_message:                   qwertyuiop
kafka_topic_name:              TSV_dead_letter_err_1746095689
kafka_partition:               0
kafka_offset:                  0
kafka_key:
rabbitmq_exchange_name:
rabbitmq_message_id:
rabbitmq_message_timestamp:    1970-01-01 00:00:00
rabbitmq_message_redelivered:  0
rabbitmq_message_delivery_tag: 0
rabbitmq_channel_id:

Row 2:
──────
stream_type:                   Kafka
event_date:                    2025-05-01
event_time:                    2025-05-01 10:34:53
event_time_microseconds:       2025-05-01 10:34:53.910944
database_name:                 default
table_name:                    kafka
error:                         Cannot parse input: expected '\t' before: 'asdfghjkl': (at row 1)
:
Row 1:
Column 0,   name: key,   type: UInt64, ERROR: text "asdfghjkl" is not like UInt64


raw_message:                   asdfghjkl
kafka_topic_name:              TSV_dead_letter_err_1746095689
kafka_partition:               0
kafka_offset:                  0
kafka_key:
rabbitmq_exchange_name:
rabbitmq_message_id:
rabbitmq_message_timestamp:    1970-01-01 00:00:00
rabbitmq_message_redelivered:  0
rabbitmq_message_delivery_tag: 0
rabbitmq_channel_id:

Row 3:
──────
stream_type:                   Kafka
event_date:                    2025-05-01
event_time:                    2025-05-01 10:34:53
event_time_microseconds:       2025-05-01 10:34:53.911092
database_name:                 default
table_name:                    kafka
error:                         Cannot parse input: expected '\t' before: 'zxcvbnm': (at row 1)
:
Row 1:
Column 0,   name: key,   type: UInt64, ERROR: text "zxcvbnm" is not like UInt64


raw_message:                   zxcvbnm
kafka_topic_name:              TSV_dead_letter_err_1746095689
kafka_partition:               0
kafka_offset:                  0
kafka_key:
rabbitmq_exchange_name:
rabbitmq_message_id:
rabbitmq_message_timestamp:    1970-01-01 00:00:00
rabbitmq_message_redelivered:  0
rabbitmq_message_delivery_tag: 0
rabbitmq_channel_id:
 (test.py:78, dead_letter_test)

```

**See Also**

- [Kafka](/engines/table-engines/integrations/kafka.md) - Kafka Engine
- [system.kafka_consumers](/operations/system-tables/kafka_consumers.md#system_tables-kafka_consumers) — Description of the `kafka_consumers` system table which contains information like statistics and errors about Kafka consumers.
