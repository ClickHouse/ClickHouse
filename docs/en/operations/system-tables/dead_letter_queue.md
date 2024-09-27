---
slug: /en/operations/system-tables/dead_letter_queue
---
# dead_letter_queue

Contains information about messages received via a stream engine and parsed with an errors. Currently implemented for Kafka.

Logging is controlled by `dead_letter_queue` of `kafka_handle_error_mode` setting.

The flushing period of data is set in `flush_interval_milliseconds` parameter of the [dead_letter_queue](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dead_letter_queue) server settings section. To force flushing, use the [SYSTEM FLUSH LOGS](../../sql-reference/statements/system.md#query_language-system-flush_logs) query.

ClickHouse does not delete data from the table automatically. See [Introduction](../../operations/system-tables/index.md#system-tables-introduction) for more details.

Columns:

- `stream_type` ([Enum8](../../sql-reference/data-types/enum.md)) - Stream type. Possible values: 'Kafka'.
- `event_date` ([Date](../../sql-reference/data-types/date.md)) - Message consuming date.
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) - Message consuming date and time.
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) - Message consuming time with microseconds precision.
- `database_name` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) - ClickHouse database Kafka table belongs to.
- `table_name` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) - ClickHouse table name.
- `error` ([String](../../sql-reference/data-types/string.md)) - Error text.
- `raw_message` ([String](../../sql-reference/data-types/string.md)) - Message body.
- `topic_name` ([Nullable(String)](../../sql-reference/data-types/nullable.md)) - Kafka topic name.
- `partition` ([Nullable(UInt64)](../../sql-reference/data-types/nullable.md)) - Kafka partition.
- `offset` ([Nullable(UInt64)](../../sql-reference/data-types/nullable.md)) - Kafka offset.
- `exchange_name` ([Nullable(String)](../../sql-reference/data-types/nullable.md)) - RabbitMQ exchange name.
- `message_id` ([Nullable(String)](../../sql-reference/data-types/nullable.md)) - RabbitMQ message id.
- `message_timestamp` ([Nullable(DateTime)](../../sql-reference/data-types/nullable.md)) - RabbitMQ message timestamp.
- `message_redelivered` ([Nullable(UInt8)](../../sql-reference/data-types/nullable.md)) - RabbitMQ redelivered flag.
- `message_delivery_tag` ([Nullable(UInt64)](../../sql-reference/data-types/nullable.md)) - RabbitMQ delivery tag.
- `channel_id` ([Nullable(String)](../../sql-reference/data-types/nullable.md)) - RabbitMQ channel id.


**Example**

Query:

``` sql
SELECT * FROM system.dead_letter_queue LIMIT 1 \G;
```

Result:

``` text
Row 1:
──────
stream_type:             Kafka
event_date:              2024-09-26
event_time:              2024-09-26 20:21:27
event_time_microseconds: 2024-09-26 20:21:27.472622
database_name:           default
table_name:              kafka
error:                   Message has too many segments. Most likely, data was corrupted: (at row 1)

raw_message:             qwertyuiop
topic_name:              CapnProto_dead_letter_queue_err
partition:               0
offset:                  0
exchange_name:           ᴺᵁᴸᴸ
message_id:              ᴺᵁᴸᴸ
message_timestamp:       ᴺᵁᴸᴸ
message_redelivered:     ᴺᵁᴸᴸ
message_delivery_tag:    ᴺᵁᴸᴸ
channel_id:              ᴺᵁᴸᴸ

Row 2:
──────
stream_type:             Kafka
event_date:              2024-09-26
event_time:              2024-09-26 20:21:27
event_time_microseconds: 2024-09-26 20:21:27.472838
database_name:           default
table_name:              kafka
error:                   Message has too many segments. Most likely, data was corrupted: (at row 1)

raw_message:             asdfghjkl
topic_name:              CapnProto_dead_letter_queue_err
partition:               0
offset:                  0
exchange_name:           ᴺᵁᴸᴸ
message_id:              ᴺᵁᴸᴸ
message_timestamp:       ᴺᵁᴸᴸ
message_redelivered:     ᴺᵁᴸᴸ
message_delivery_tag:    ᴺᵁᴸᴸ
channel_id:              ᴺᵁᴸᴸ

Row 3:
──────
stream_type:             Kafka
event_date:              2024-09-26
event_time:              2024-09-26 20:21:27
event_time_microseconds: 2024-09-26 20:21:27.473047
database_name:           default
table_name:              kafka
error:                   Message has too many segments. Most likely, data was corrupted: (at row 1)

raw_message:             zxcvbnm
topic_name:              CapnProto_dead_letter_queue_err
partition:               0
offset:                  0
exchange_name:           ᴺᵁᴸᴸ
message_id:              ᴺᵁᴸᴸ
message_timestamp:       ᴺᵁᴸᴸ
message_redelivered:     ᴺᵁᴸᴸ
message_delivery_tag:    ᴺᵁᴸᴸ
channel_id:              ᴺᵁᴸᴸ
```

**See Also**

- [Kafka](../../engines/table-engines/integrations/kafka) - Kafka Engine
- [system.kafka_consumers](../../operations/system-tables/kafka_consumers.md#system_tables-kafka_consumers) — Description of the `kafka_consumers` system table which contains information like statistics and errors about Kafka consumers.
