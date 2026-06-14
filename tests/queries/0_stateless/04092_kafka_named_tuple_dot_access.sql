-- Tags: no-fasttest
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/52121
-- Named tuple element access via dot notation should work for Kafka tables.
-- Previously, `foo.a` on a Kafka table with Tuple(a String, b String) column
-- failed with UNKNOWN_IDENTIFIER.

-- Suppress expected Kafka consumer connection errors from reaching client stderr.
-- The background consumer tries to connect to the non-existent broker and produces
-- Error-level log messages that get forwarded via send_logs_level.
SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS mt;
DROP TABLE IF EXISTS users;

CREATE TABLE users (foo Tuple(a String, b String))
  ENGINE = Kafka
  SETTINGS
  kafka_broker_list = 'localhost:10000',
  kafka_topic_list = 'foo',
  kafka_group_name = 'foo',
  kafka_format = 'JSONEachRow';

CREATE TABLE mt (a String)
  ENGINE = MergeTree
  ORDER BY tuple();

CREATE MATERIALIZED VIEW mv TO mt AS
  SELECT foo.a AS a FROM users;

SELECT 'OK';

DROP TABLE mv;
DROP TABLE mt;
DROP TABLE users;
