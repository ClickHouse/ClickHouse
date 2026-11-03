-- Tags: no-fasttest
-- Test validation of kafka_partition_shard_num and kafka_shard_count settings

SET allow_experimental_kafka_offsets_storage_in_keeper = 1;

-- Case 1: Only kafka_partition_shard_num without kafka_shard_count should fail
CREATE TABLE test_kafka_partition_affinity_1 (id UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = '127.0.0.1:9092',
         kafka_topic_list = 'test_topic',
         kafka_group_name = 'test_group',
         kafka_format = 'JSONEachRow',
         kafka_keeper_path = '/clickhouse/test/partition_affinity_1',
         kafka_replica_name = 'r1',
         kafka_partition_shard_num = '0'; -- { serverError BAD_ARGUMENTS }

-- Case 2: Only kafka_shard_count without kafka_partition_shard_num should fail
CREATE TABLE test_kafka_partition_affinity_2 (id UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = '127.0.0.1:9092',
         kafka_topic_list = 'test_topic',
         kafka_group_name = 'test_group',
         kafka_format = 'JSONEachRow',
         kafka_keeper_path = '/clickhouse/test/partition_affinity_2',
         kafka_replica_name = 'r1',
         kafka_shard_count = 3; -- { serverError BAD_ARGUMENTS }

-- Case 3: kafka_partition_shard_num > kafka_shard_count should fail
CREATE TABLE test_kafka_partition_affinity_3 (id UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = '127.0.0.1:9092',
         kafka_topic_list = 'test_topic',
         kafka_group_name = 'test_group',
         kafka_format = 'JSONEachRow',
         kafka_keeper_path = '/clickhouse/test/partition_affinity_3',
         kafka_replica_name = 'r1',
         kafka_partition_shard_num = '4',
         kafka_shard_count = 3; -- { serverError BAD_ARGUMENTS }

-- Case 4: kafka_partition_shard_num is not a valid integer should fail
CREATE TABLE test_kafka_partition_affinity_4 (id UInt64)
ENGINE = Kafka
SETTINGS kafka_broker_list = '127.0.0.1:9092',
         kafka_topic_list = 'test_topic',
         kafka_group_name = 'test_group',
         kafka_format = 'JSONEachRow',
         kafka_keeper_path = '/clickhouse/test/partition_affinity_4',
         kafka_replica_name = 'r1',
         kafka_partition_shard_num = 'abc',
         kafka_shard_count = 3; -- { serverError BAD_ARGUMENTS }

SELECT 'All validation tests passed';
