-- Tags: zookeeper

SET allow_experimental_kafka_offsets_storage_in_keeper = 1;

DROP TABLE IF EXISTS test.kafka2_avro_confluent;
CREATE TABLE test.kafka2_avro_confluent (
    id UInt64,
    name String
) ENGINE = Kafka
SETTINGS
    kafka_keeper_path = '/clickhouse/kafka/offsets',
    kafka_replica_name = 'replica1',
    kafka_broker_list = 'localhost:9092',
    kafka_topic_list = 'test_topic',
    kafka_group_name = 'test_group',
    kafka_format = 'AvroConfluent',
    kafka_format_avro_schema_registry_url = 'http://localhost:8081';
SELECT name, engine FROM system.tables WHERE database = 'test' AND name = 'kafka2_avro_confluent';

DROP TABLE IF EXISTS test.kafka2_avro_confluent_auth;
CREATE TABLE test.kafka2_avro_confluent_auth (
    id UInt64,
    name String
) ENGINE = Kafka
SETTINGS
    kafka_keeper_path = '/clickhouse/kafka/offsets',
    kafka_replica_name = 'replica1',
    kafka_broker_list = 'localhost:9092',
    kafka_topic_list = 'test_topic_auth',
    kafka_group_name = 'test_group_auth',
    kafka_format = 'AvroConfluent',
    kafka_format_avro_schema_registry_url = 'http://user:pass@localhost:8081';
SELECT name, engine FROM system.tables WHERE database = 'test' AND name = 'kafka2_avro_confluent_auth';

DROP TABLE IF EXISTS test.kafka2_avro_confluent_no_url;
CREATE TABLE test.kafka2_avro_confluent_no_url (
    id UInt64,
    name String
) ENGINE = Kafka
SETTINGS
    kafka_keeper_path = '/clickhouse/kafka/offsets',
    kafka_replica_name = 'replica1',
    kafka_broker_list = 'localhost:9092',
    kafka_topic_list = 'test_topic_no_url',
    kafka_group_name = 'test_group_no_url',
    kafka_format = 'AvroConfluent';
SELECT name, engine FROM system.tables WHERE database = 'test' AND name = 'kafka2_avro_confluent_no_url';

DROP TABLE IF EXISTS test.kafka2_avro_confluent;
DROP TABLE IF EXISTS test.kafka2_avro_confluent_auth;
DROP TABLE IF EXISTS test.kafka2_avro_confluent_no_url;
