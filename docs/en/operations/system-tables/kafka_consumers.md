---
slug: /en/operations/system-tables/kafka_consumers
---
# kafka_consumers

Contains information about Kafka consumers.
Applicable for [Kafka table engine](../../engines/table-engines/integrations/kafka) (native ClickHouse integration)

Columns:

- `database` (String) - database of the table with Kafka Engine.
- `table` (String) - name of the table with Kafka Engine.
- `consumer_id` (String) - Kafka consumer identifier. Note, that a table can have many consumers. Specified by `kafka_num_consumers` parameter.
- `assignments.topic` (Array(String)) - Kafka topic.
- `assignments.partition_id` (Array(Int32)) - Kafka partition id. Note, that only one consumer can be assigned to a partition.
- `assignments.current_offset` (Array(Int64)) - current offset.
- `last_exception_time`, (DateTime) - timestamp when the most recent exception was generated.
- `last_exception`, (String) - text of the most recent exception.
- `last_poll_time`, (DateTime) - timestamp of the most recent poll.
- `num_messages_read`, (UInt64) - number of messages read by the consumer.
- `last_commit_time`, (DateTime) - timestamp of the most recent poll.
- `num_commits`, (UInt64) - total number of commits for the consumer.
- `last_rebalance_time`, (DateTime) - timestamp of the most recent Kafka rebalance
- `num_rebalance_revocations`, (UInt64) - number of times the consumer was revoked its partitions
- `num_rebalance_assignments`, (UInt64) - number of times the consumer was assigned to Kafka cluster
- `is_currently_used`, (UInt8) - consumer is in use
- `rdkafka_stat` (String) - library internal statistic. See https://github.com/ClickHouse/librdkafka/blob/master/STATISTICS.md . Set `statistics_interval_ms` to disable.

Example:

``` sql
SELECT *
FROM system.kafka_consumers
FORMAT Vertical
```

``` text
Row 1:
──────
database:                   test
table:                      kafka
consumer_id:                ClickHouse-instance-test-kafka-1caddc7f-f917-4bb1-ac55-e28bd103a4a0
assignments.topic:          ['system_kafka_cons']
assignments.partition_id:   [0]
assignments.current_offset: [18446744073709550615]
last_exception_time:        1970-01-01 00:00:00
last_exception:
last_poll_time:             2006-11-09 18:47:47
num_messages_read:          4
last_commit_time:           2006-11-10 04:39:40
num_commits:                1
last_rebalance_time:        1970-01-01 00:00:00
num_rebalance_revocations:  0
num_rebalance_assignments:  1
is_currently_used:          1

```
