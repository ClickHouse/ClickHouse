DROP TABLE IF EXISTS test.src;
DROP TABLE IF EXISTS test.dst_r1;
DROP TABLE IF EXISTS test.dst_r2;

CREATE TABLE test.src (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
CREATE TABLE test.dst_r1 (p UInt64, k String, d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/test/dst_1', '1') PARTITION BY p ORDER BY k;
CREATE TABLE test.dst_r2 (p UInt64, k String, d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/test/dst_1', '2') PARTITION BY p ORDER BY k;

INSERT INTO test.src VALUES (0, '0', 1);
INSERT INTO test.src VALUES (1, '0', 1), (1, '1', 1);
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

ALTER TABLE test.dst_r1 REPLACE PARTITION 1 FROM test.src;
ALTER TABLE test.src DROP PARTITION 1;

SELECT 'REPLACE #1';
SELECT count(), sum(d) FROM test.src;
SELECT count(), sum(d) FROM test.dst_r1;
SELECT count(), sum(d) FROM test.dst_r2;

SELECT 'REPLACE empty #1';
ALTER TABLE test.dst_r1 REPLACE PARTITION 1 FROM test.src;
SELECT count(), sum(d) FROM test.dst_r1;
SELECT count(), sum(d) FROM test.dst_r2;

DROP TABLE test.src;
CREATE TABLE test.src (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
INSERT INTO test.src VALUES (1, '0', 1), (1, '1', 1);

SELECT 'ATTACH';
INSERT INTO test.dst_r2 VALUES (1, '1', 2);
ALTER TABLE test.dst_r2 ATTACH PARTITION 1 FROM test.src;
SELECT count(), sum(d) FROM test.dst_r1;
SELECT count(), sum(d) FROM test.dst_r2;

