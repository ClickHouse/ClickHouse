DROP TABLE IF EXISTS test_max_size_drop SYNC;
SET insert_keeper_fault_injection_probability = 0.0;

CREATE TABLE test_max_size_drop (number UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_max_size_drop', '1')
ORDER BY number;

INSERT INTO test_max_size_drop SELECT number FROM numbers(1000);

DROP TABLE test_max_size_drop SETTINGS max_table_size_to_drop = 1; -- { serverError TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT }
DROP TABLE test_max_size_drop SYNC;

CREATE TABLE test_max_size_drop (number UInt64)
Engine = ReplicatedMergeTree('/clickhouse/tables/{database}/test_max_size_drop', '1')
ORDER BY number;

INSERT INTO test_max_size_drop SELECT number FROM numbers(1000);

ALTER TABLE test_max_size_drop DROP PARTITION tuple() SETTINGS max_partition_size_to_drop = 1; -- { serverError TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT }
ALTER TABLE test_max_size_drop DROP PARTITION tuple();
DROP TABLE test_max_size_drop SYNC;
