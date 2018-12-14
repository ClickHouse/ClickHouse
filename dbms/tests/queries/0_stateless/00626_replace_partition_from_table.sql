DROP TABLE IF EXISTS test.src;
DROP TABLE IF EXISTS test.dst;

CREATE TABLE test.src (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
CREATE TABLE test.dst (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;

SELECT 'Initial';
INSERT INTO test.src VALUES (0, '0', 1);
INSERT INTO test.src VALUES (1, '0', 1);
INSERT INTO test.src VALUES (1, '1', 1);
INSERT INTO test.src VALUES (2, '0', 1);

INSERT INTO test.dst VALUES (0, '1', 2);
INSERT INTO test.dst VALUES (1, '1', 2), (1, '2', 2);
INSERT INTO test.dst VALUES (2, '1', 2);

SELECT count(), sum(d) FROM test.src;
SELECT count(), sum(d) FROM test.dst;


SELECT 'REPLACE simple';
ALTER TABLE test.dst REPLACE PARTITION 1 FROM test.src;
ALTER TABLE test.src DROP PARTITION 1;
SELECT count(), sum(d) FROM test.src;
SELECT count(), sum(d) FROM test.dst;


SELECT 'REPLACE empty';
ALTER TABLE test.src DROP PARTITION 1;
ALTER TABLE test.dst REPLACE PARTITION 1 FROM test.src;
SELECT count(), sum(d) FROM test.dst;


SELECT 'REPLACE recursive';
ALTER TABLE test.dst DROP PARTITION 1;
INSERT INTO test.dst VALUES (1, '1', 2), (1, '2', 2);

CREATE TEMPORARY table test_block_numbers (m UInt64);
INSERT INTO test_block_numbers SELECT max(max_block_number) AS m FROM system.parts WHERE database='test' AND  table='dst' AND active AND name LIKE '1_%';

ALTER TABLE test.dst REPLACE PARTITION 1 FROM test.dst;
SELECT count(), sum(d) FROM test.dst;

INSERT INTO test_block_numbers SELECT max(max_block_number) AS m FROM system.parts WHERE database='test' AND  table='dst' AND active AND name LIKE '1_%';
SELECT (max(m) - min(m) > 1) AS new_block_is_generated FROM test_block_numbers;
DROP TEMPORARY TABLE test_block_numbers;


SELECT 'ATTACH FROM';
ALTER TABLE test.dst DROP PARTITION 1;
DROP TABLE test.src;

CREATE TABLE test.src (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
INSERT INTO test.src VALUES (1, '0', 1);
INSERT INTO test.src VALUES (1, '1', 1);

SYSTEM STOP MERGES test.dst;
INSERT INTO test.dst VALUES (1, '1', 2);
ALTER TABLE test.dst ATTACH PARTITION 1 FROM test.src;
SELECT count(), sum(d) FROM test.dst;


SELECT 'OPTIMIZE';
SELECT count(), sum(d), uniqExact(_part) FROM test.dst;
SYSTEM START MERGES;
SET optimize_throw_if_noop=1;
OPTIMIZE TABLE test.dst;
SELECT count(), sum(d), uniqExact(_part) FROM test.dst;


SELECT 'After restart';
DETACH TABLE test.dst;
ATTACH TABLE test.dst;
SELECT count(), sum(d) FROM test.dst;

SELECT 'DETACH+ATTACH PARTITION';
ALTER TABLE test.dst DETACH PARTITION 0;
ALTER TABLE test.dst DETACH PARTITION 1;
ALTER TABLE test.dst DETACH PARTITION 2;
ALTER TABLE test.dst ATTACH PARTITION 1;
SELECT count(), sum(d) FROM test.dst;

DROP TABLE IF EXISTS test.src;
DROP TABLE IF EXISTS test.dst;
