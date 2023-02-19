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
SELECT name, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test' AND active ORDER BY partition;
SELECT name, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test2' AND active ORDER BY partition;

ALTER TABLE test MODIFY COLUMN x Enum('hello' = 1, 'world' = 2, 'goodbye' = 3);
INSERT INTO test VALUES ('goodbye', 'test');
OPTIMIZE TABLE test FINAL;
SELECT * FROM test ORDER BY x;
SELECT * FROM test2 ORDER BY x;
SELECT name, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test' AND active ORDER BY partition;
SELECT name, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test2' AND active ORDER BY partition;

ALTER TABLE test MODIFY COLUMN x Enum('hello' = 1, 'world' = 2); -- { serverError 524 }
ALTER TABLE test MODIFY COLUMN x Enum('hello' = 1, 'world' = 2, 'test' = 3);

ALTER TABLE test MODIFY COLUMN x Enum('hello' = 1, 'world' = 2, 'goodbye' = 4); -- { serverError 524 }

ALTER TABLE test MODIFY COLUMN x Int8;
INSERT INTO test VALUES (111, 'abc');
OPTIMIZE TABLE test FINAL;
SELECT * FROM test ORDER BY x;
SELECT * FROM test2 ORDER BY x;
SELECT name, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test' AND active ORDER BY partition;
SELECT name, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test2' AND active ORDER BY partition;

ALTER TABLE test MODIFY COLUMN x Enum8('' = 1); -- { serverError 524 }
ALTER TABLE test MODIFY COLUMN x Enum16('' = 1); -- { serverError 524 }

ALTER TABLE test MODIFY COLUMN x UInt64; -- { serverError 524 }
ALTER TABLE test MODIFY COLUMN x String; -- { serverError 524 }
ALTER TABLE test MODIFY COLUMN x Nullable(Int64); -- { serverError 524 }

ALTER TABLE test RENAME COLUMN x TO z; -- { serverError 524 }
ALTER TABLE test RENAME COLUMN y TO z; -- { serverError 524 }
ALTER TABLE test DROP COLUMN x; -- { serverError 47 }
ALTER TABLE test DROP COLUMN y; -- { serverError 47 }

DROP TABLE test SYNC;
DROP TABLE test2 SYNC;
