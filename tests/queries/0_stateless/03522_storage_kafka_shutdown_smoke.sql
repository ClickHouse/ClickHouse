-- Tags: no-fasttest, disabled
-- Tag no-fasttest -- requires Kafka
-- Tag disabled -- data race in cJSON (librdkafka vs aws-c-common) - https://github.com/ClickHouse/ClickHouse/issues/80866

-- Regression test for proper StorageKafka shutdown
-- https://github.com/ClickHouse/ClickHouse/issues/80674

-- librdkafka may print some errors/warnings:
---
-- <Warning> StorageKafka (test_8g8g0dlf.kafka_test): sasl.kerberos.kinit.cmd configuration parameter is ignored.
-- <Error> StorageKafka (test_8g8g0dlf.kafka_test): [client.id:ClickHouse-localhost-test_8g8g0dlf-kafka_test] [rdk:ERROR] [thrd:app]: ClickHouse-localhost-test_8g8g0dlf-kafka_test#consumer-1: 0.0.0.0:9092/bootstrap: Connect to ipv4#0.0.0.0:9092 failed: Connection refused (after 0ms in state CONNECT)
-- <Error> StorageKafka (test_8g8g0dlf.kafka_test): [client.id:ClickHouse-localhost-test_8g8g0dlf-kafka_test] [rdk:ERROR] [thrd:app]: ClickHouse-localhost-test_8g8g0dlf-kafka_test#consumer-1: 0.0.0.0:9092/bootstrap: Connect to ipv4#0.0.0.0:9092 failed: Connection refused (after 0ms in state CONNECT, 1 identical error(s) suppressed)
-- <Warning> StorageKafka (test_8g8g0dlf.kafka_test): Can't get assignment. Will keep trying.
SET send_logs_level='fatal';

DROP TABLE IF EXISTS kafka_test;
CREATE TABLE kafka_test
(
    `raw_message` String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = '0.0.0.0:9092', kafka_group_name = 'test', kafka_topic_list = 'kafka_test', kafka_format = 'RawBLOB', kafka_consumers_pool_ttl_ms=500;

SELECT * FROM kafka_test LIMIT 1 settings stream_like_engine_allow_direct_select=1;
DROP TABLE kafka_test SYNC;
