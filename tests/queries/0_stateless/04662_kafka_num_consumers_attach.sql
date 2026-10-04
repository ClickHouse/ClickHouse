-- Tags: no-fasttest
-- The `kafka_num_consumers` limit is derived from the number of CPU cores available to the server,
-- so a stored table definition must not be re-validated against it when the table is loaded back.

-- Suppress expected Kafka consumer connection errors from reaching client stderr.
SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS kafka_many_consumers;

SET kafka_disable_num_consumers_limit = 1;

CREATE TABLE kafka_many_consumers (a UInt64)
  ENGINE = Kafka
  SETTINGS
  kafka_broker_list = 'localhost:10000',
  kafka_topic_list = 'foo',
  kafka_group_name = 'foo',
  kafka_format = 'JSONEachRow',
  kafka_num_consumers = 1000;

SET kafka_disable_num_consumers_limit = 0;

DETACH TABLE kafka_many_consumers;
ATTACH TABLE kafka_many_consumers;

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'kafka_many_consumers';

-- A freshly introduced definition is still validated.
CREATE TABLE kafka_many_consumers_2 (a UInt64)
  ENGINE = Kafka
  SETTINGS
  kafka_broker_list = 'localhost:10000',
  kafka_topic_list = 'foo',
  kafka_group_name = 'foo',
  kafka_format = 'JSONEachRow',
  kafka_num_consumers = 1000; -- { serverError BAD_ARGUMENTS }

DROP TABLE kafka_many_consumers;
