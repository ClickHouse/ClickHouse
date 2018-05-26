DROP TABLE IF EXISTS test.src;
DROP TABLE IF EXISTS test.dst_r1;
DROP TABLE IF EXISTS test.dst_r2;

CREATE TABLE test.src (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
CREATE TABLE test.dst_r1 (p UInt64, k String, d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/test/dst_1', '1') PARTITION BY p ORDER BY k SETTINGS old_parts_lifetime=1, cleanup_delay_period=1, cleanup_delay_period_random_add=0;
CREATE TABLE test.dst_r2 (p UInt64, k String, d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/test/dst_1', '2') PARTITION BY p ORDER BY k SETTINGS old_parts_lifetime=1, cleanup_delay_period=1, cleanup_delay_period_random_add=0;

INSERT INTO test.src VALUES (0, '0', 1);
INSERT INTO test.src VALUES (1, '0', 1);
INSERT INTO test.src VALUES (1, '1', 1);
INSERT INTO test.src VALUES (2, '0', 1);

SELECT 'Initial';
INSERT INTO test.dst_r1 VALUES (0, '1', 2);
INSERT INTO test.dst_r1 VALUES (1, '1', 2), (1, '2', 2);
INSERT INTO test.dst_r1 VALUES (2, '1', 2);

SYSTEM SYNC REPLICA test.dst_r2;
SELECT count(), sum(d) FROM test.src;
SELECT count(), sum(d) FROM test.dst_r1;
SELECT count(), sum(d) FROM test.dst_r2;


SELECT 'REPLACE simple';
ALTER TABLE test.dst_r1 REPLACE PARTITION 1 FROM test.src;
ALTER TABLE test.src DROP PARTITION 1;

SYSTEM SYNC REPLICA test.dst_r2;
SELECT count(), sum(d) FROM test.src;
SELECT count(), sum(d) FROM test.dst_r1;
SELECT count(), sum(d) FROM test.dst_r2;


SELECT 'REPLACE empty';
ALTER TABLE test.src DROP PARTITION 1;
ALTER TABLE test.dst_r1 REPLACE PARTITION 1 FROM test.src;

SYSTEM SYNC REPLICA test.dst_r2;
SELECT count(), sum(d) FROM test.dst_r1;
SELECT count(), sum(d) FROM test.dst_r2;


SELECT 'REPLACE recursive';
ALTER TABLE test.dst_r1 DROP PARTITION 1;
INSERT INTO test.dst_r1 VALUES (1, '1', 2), (1, '2', 2);

CREATE TEMPORARY table test_block_numbers (m UInt64);
INSERT INTO test_block_numbers SELECT max(max_block_number) AS m FROM system.parts WHERE database='test' AND  table='dst_r1' AND active AND name LIKE '1_%';

ALTER TABLE test.dst_r1 REPLACE PARTITION 1 FROM test.dst_r1;
SYSTEM SYNC REPLICA test.dst_r2;
SELECT count(), sum(d) FROM test.dst_r1;
SELECT count(), sum(d) FROM test.dst_r2;

INSERT INTO test_block_numbers SELECT max(max_block_number) AS m FROM system.parts WHERE database='test' AND  table='dst_r1' AND active AND name LIKE '1_%';
SELECT (max(m) - min(m) > 1) AS new_block_is_generated FROM test_block_numbers;
DROP TABLE test_block_numbers;


SELECT 'ATTACH FROM';
ALTER TABLE test.dst_r1 DROP PARTITION 1;
DROP TABLE test.src;

CREATE TABLE test.src (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
INSERT INTO test.src VALUES (1, '0', 1);
INSERT INTO test.src VALUES (1, '1', 1);

INSERT INTO test.dst_r2 VALUES (1, '1', 2);
ALTER TABLE test.dst_r2 ATTACH PARTITION 1 FROM test.src;

SYSTEM SYNC REPLICA test.dst_r1;
SELECT count(), sum(d) FROM test.dst_r1;
SELECT count(), sum(d) FROM test.dst_r2;


SELECT 'REPLACE with fetch';
DROP TABLE test.src;
CREATE TABLE test.src (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
INSERT INTO test.src VALUES (1, '0', 1);
INSERT INTO test.src VALUES (1, '1', 1);
INSERT INTO test.dst_r1 VALUES (1, '1', 2); -- trash part to be deleted

-- Stop replication at the second replica and remove source table to use fetch instead of copying
SYSTEM STOP REPLICATION QUEUES test.dst_r2;
ALTER TABLE test.dst_r1 REPLACE PARTITION 1 FROM test.src;
DROP TABLE test.src;
SYSTEM START REPLICATION QUEUES test.dst_r2;

SYSTEM SYNC REPLICA test.dst_r2;
SELECT count(), sum(d) FROM test.dst_r1;
SELECT count(), sum(d) FROM test.dst_r2;


SELECT 'REPLACE with fetch of merged';
DROP TABLE IF EXISTS test.src;
ALTER TABLE test.dst_r1 DROP PARTITION 1;

CREATE TABLE test.src (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
INSERT INTO test.src VALUES (1, '0', 1);
INSERT INTO test.src VALUES (1, '1', 1);
INSERT INTO test.dst_r1 VALUES (1, '1', 2); -- trash part to be deleted

SYSTEM STOP MERGES test.dst_r2;
SYSTEM STOP REPLICATION QUEUES test.dst_r2;
ALTER TABLE test.dst_r1 REPLACE PARTITION 1 FROM test.src;
DROP TABLE test.src;

-- do not wait other replicas to execute OPTIMIZE
SET replication_alter_partitions_sync=0, optimize_throw_if_noop=1;
SELECT count(), sum(d), uniqExact(_part) FROM test.dst_r1;
OPTIMIZE TABLE test.dst_r1 PARTITION 1;
SET replication_alter_partitions_sync=1;
SYSTEM SYNC REPLICA test.dst_r1;
SELECT count(), sum(d), uniqExact(_part) FROM test.dst_r1;

SYSTEM START REPLICATION QUEUES test.dst_r2;
SYSTEM START MERGES test.dst_r2;
SYSTEM SYNC REPLICA test.dst_r2;
SELECT count(), sum(d), uniqExact(_part) FROM test.dst_r2;

SELECT 'After restart';
USE test;
SYSTEM RESTART REPLICA dst_r1;
SYSTEM RESTART REPLICAS;
SELECT count(), sum(d) FROM test.dst_r1;
SELECT count(), sum(d) FROM test.dst_r2;

SELECT 'DETACH+ATTACH PARTITION';
ALTER TABLE test.dst_r1 DETACH PARTITION 0;
ALTER TABLE test.dst_r1 DETACH PARTITION 1;
ALTER TABLE test.dst_r1 DETACH PARTITION 2;
ALTER TABLE test.dst_r1 ATTACH PARTITION 1;
SELECT count(), sum(d) FROM test.dst_r1;
SYSTEM SYNC REPLICA test.dst_r2;
SELECT count(), sum(d) FROM test.dst_r2;

DROP TABLE IF EXISTS test.src;
DROP TABLE IF EXISTS test.dst_r1;
DROP TABLE IF EXISTS test.dst_r2;
