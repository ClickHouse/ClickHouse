-- Tags: no-fasttest
-- `kafka_disable_num_consumers_limit` only lifts the limit derived from the CPU count. An absurd
-- `kafka_num_consumers` must still be rejected, instead of failing an allocation inside the storage.

-- Suppress expected Kafka consumer connection errors from reaching client stderr.
SET send_logs_level = 'fatal';

SET kafka_disable_num_consumers_limit = 1;

CREATE TABLE kafka_too_many_consumers (a UInt64)
  ENGINE = Kafka
  SETTINGS
  kafka_broker_list = 'localhost:10000',
  kafka_topic_list = 'foo',
  kafka_group_name = 'foo',
  kafka_format = 'JSONEachRow',
  kafka_num_consumers = 9223372036854775807; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'kafka_too_many_consumers';
