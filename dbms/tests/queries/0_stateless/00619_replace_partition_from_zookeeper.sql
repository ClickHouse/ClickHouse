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

SET insert_quorum = 2;
SET replication_alter_partitions_sync = 2;

INSERT INTO test.dst_r1 VALUES (0, '1', 2);
INSERT INTO test.dst_r1 VALUES (1, '1', 2), (1, '2', 2);
INSERT INTO test.dst_r1 VALUES (2, '1', 2);


SELECT 'Initial';
SELECT count(), sum(d) FROM test.src;
SELECT count(), sum(d) FROM test.dst_r1;
SELECT count(), sum(d) FROM test.dst_r2;


SELECT 'REPLACE #1';
ALTER TABLE test.dst_r1 REPLACE PARTITION 1 FROM test.src;
ALTER TABLE test.src DROP PARTITION 1;

SELECT count(), sum(d) FROM test.src;
SELECT count(), sum(d) FROM test.dst_r1;
SELECT count(), sum(d) FROM test.dst_r2;


SELECT 'REPLACE empty #1';
ALTER TABLE test.dst_r1 REPLACE PARTITION 1 FROM test.src;
SELECT count(), sum(d) FROM test.dst_r1;
SELECT count(), sum(d) FROM test.dst_r2;


SELECT 'REPLACE recursive';
INSERT INTO test.dst_r1 VALUES (1, '1', 2), (1, '2', 2);

CREATE TEMPORARY table test_block_numbers (m UInt64);
INSERT INTO test_block_numbers SELECT max(max_block_number) AS m FROM system.parts WHERE database='test' AND  table='dst_r1' AND active AND name LIKE '1_%';

ALTER TABLE test.dst_r1 REPLACE PARTITION 1 FROM test.dst_r1;
SELECT count(), sum(d) FROM test.dst_r1;
SELECT count(), sum(d) FROM test.dst_r2;

INSERT INTO test_block_numbers SELECT max(max_block_number) AS m FROM system.parts WHERE database='test' AND  table='dst_r1' AND active AND name LIKE '1_%';
SELECT max(m) - min(m) FROM test_block_numbers;
DROP TABLE test_block_numbers;


SELECT 'ATTACH';
ALTER TABLE test.dst_r1 DROP PARTITION 1;
DROP TABLE test.src;

CREATE TABLE test.src (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
INSERT INTO test.src VALUES (1, '0', 1);
INSERT INTO test.src VALUES (1, '1', 1);

INSERT INTO test.dst_r2 VALUES (1, '1', 2);
ALTER TABLE test.dst_r2 ATTACH PARTITION 1 FROM test.src;
SELECT count(), sum(d) FROM test.dst_r1;
SELECT count(), sum(d) FROM test.dst_r2;


SELECT 'REPLACE with fetch';
SET insert_quorum = 0;
SET replication_alter_partitions_sync = 1;

DROP TABLE test.src;
CREATE TABLE test.src (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
INSERT INTO test.src VALUES (1, '0', 1);
INSERT INTO test.src VALUES (1, '1', 1);

INSERT INTO test.dst_r1 VALUES (1, '1', 2);
-- Stop replication at the second replica and remove source table to use fetch instead of copying
SYSTEM STOP REPLICATION QUEUES test.dst_r2;
ALTER TABLE test.dst_r1 ATTACH PARTITION 1 FROM test.src;
DROP TABLE test.src;
SYSTEM START REPLICATION QUEUES test.dst_r2;
SYSTEM SYNC REPLICA test.dst_r2;

SELECT count(), sum(d) FROM test.dst_r1;
SELECT count(), sum(d) FROM test.dst_r2;


SELECT 'REPLACE with fetch merged';
SET insert_quorum = 0;
SET replication_alter_partitions_sync = 1;

DROP TABLE IF EXISTS test.src;
CREATE TABLE test.src (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
INSERT INTO test.src VALUES (1, '0', 1);
INSERT INTO test.src VALUES (1, '1', 1);

--SYSTEM STOP MERGES;
INSERT INTO test.dst_r1 VALUES (1, '1', 2);
SYSTEM STOP REPLICATION QUEUES test.dst_r2;
ALTER TABLE test.dst_r1 ATTACH PARTITION 1 FROM test.src;
DROP TABLE test.src;

SET replication_alter_partitions_sync=0;
SET optimize_throw_if_noop=1;
SYSTEM START MERGES;
OPTIMIZE TABLE test.dst_r1 PARTITION 1;
SET replication_alter_partitions_sync=1;

SYSTEM SYNC REPLICA test.dst_r1;
SELECT uniqExact(_part) FROM test.dst_r1 WHERE _part LIKE '1_%';
SELECT count(), sum(d) FROM test.dst_r1 WHERE _part LIKE '1_%';

SYSTEM START REPLICATION QUEUES test.dst_r2;
SYSTEM SYNC REPLICA test.dst_r2;
SELECT count(), sum(d) FROM test.dst_r2 WHERE _part LIKE '1_%';
