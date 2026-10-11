-- Tags: long, replica

SET insert_keeper_fault_injection_probability=0; -- disable fault injection; part ids are non-deterministic in case of insert retries
SET replication_alter_partitions_sync=2;

DROP TABLE IF EXISTS test SYNC;
DROP TABLE IF EXISTS test2 SYNC;

CREATE TABLE test (x Enum('hello' = 1, 'world' = 2), y String) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_01346/table', 'r1') PARTITION BY x ORDER BY y;
CREATE TABLE test2 (x Enum('hello' = 1, 'world' = 2), y String) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_01346/table', 'r2') PARTITION BY x ORDER BY y;
INSERT INTO test VALUES ('hello', 'test');

SELECT * FROM test;
SYSTEM SYNC REPLICA test2;
SELECT * FROM test2;
SELECT min_block_number, max_block_number, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test' AND active ORDER BY partition;
SELECT min_block_number, max_block_number, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test2' AND active ORDER BY partition;

ALTER TABLE test MODIFY COLUMN x Enum('hello' = 1, 'world' = 2, 'goodbye' = 3);
INSERT INTO test VALUES ('goodbye', 'test');
OPTIMIZE TABLE test FINAL;
SYSTEM SYNC REPLICA test2;

SELECT * FROM test ORDER BY x;
SYSTEM SYNC REPLICA test2;
SELECT * FROM test2 ORDER BY x;
SELECT min_block_number, max_block_number, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test' AND active ORDER BY partition;
SELECT min_block_number, max_block_number, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test2' AND active ORDER BY partition;

ALTER TABLE test MODIFY COLUMN x Enum('hello' = 1, 'world' = 2); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test MODIFY COLUMN x Enum('hello' = 1, 'world' = 2, 'test' = 3);

ALTER TABLE test MODIFY COLUMN x Enum('hello' = 1, 'world' = 2, 'goodbye' = 4); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

ALTER TABLE test MODIFY COLUMN x Int8;
INSERT INTO test VALUES (111, 'abc');
OPTIMIZE TABLE test FINAL;
SYSTEM SYNC REPLICA test2;

SELECT * FROM test ORDER BY x;
SYSTEM SYNC REPLICA test2;
SELECT * FROM test2 ORDER BY x;
SELECT min_block_number, max_block_number, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test' AND active ORDER BY partition;
SELECT min_block_number, max_block_number, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test2' AND active ORDER BY partition;

ALTER TABLE test MODIFY COLUMN x Enum8('' = 1); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test MODIFY COLUMN x Enum16('' = 1); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

ALTER TABLE test MODIFY COLUMN x UInt64; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test MODIFY COLUMN x String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test MODIFY COLUMN x Nullable(Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

ALTER TABLE test RENAME COLUMN x TO z; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test RENAME COLUMN y TO z; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test DROP COLUMN x; -- { serverError UNKNOWN_IDENTIFIER }
ALTER TABLE test DROP COLUMN y; -- { serverError UNKNOWN_IDENTIFIER }

DROP TABLE test SYNC;
DROP TABLE test2 SYNC;
