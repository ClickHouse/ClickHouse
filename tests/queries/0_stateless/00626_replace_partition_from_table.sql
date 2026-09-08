DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;

CREATE TABLE src (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
CREATE TABLE dst (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k SETTINGS merge_selector_base=1000;

SELECT 'Initial';
INSERT INTO src VALUES (0, '0', 1);
INSERT INTO src VALUES (1, '0', 1);
INSERT INTO src VALUES (1, '1', 1);
INSERT INTO src VALUES (2, '0', 1);

INSERT INTO dst VALUES (0, '1', 2);
INSERT INTO dst VALUES (1, '1', 2), (1, '2', 2);
INSERT INTO dst VALUES (2, '1', 2);

SELECT count(), sum(d) FROM src;
SELECT count(), sum(d) FROM dst;


SELECT 'REPLACE simple';
ALTER TABLE dst REPLACE PARTITION 1 FROM src;
ALTER TABLE src DROP PARTITION 1;
SELECT count(), sum(d) FROM src;
SELECT count(), sum(d) FROM dst;


SELECT 'REPLACE empty';
ALTER TABLE src DROP PARTITION 1;
ALTER TABLE dst REPLACE PARTITION 1 FROM src;
SELECT count(), sum(d) FROM dst;


SELECT 'REPLACE recursive';
ALTER TABLE dst DROP PARTITION 1;
INSERT INTO dst VALUES (1, '1', 2), (1, '2', 2);

CREATE TEMPORARY table test_block_numbers (m UInt64);
INSERT INTO test_block_numbers SELECT max(max_block_number) AS m FROM system.parts WHERE database=currentDatabase() AND  table='dst' AND active AND name LIKE '1_%';

ALTER TABLE dst REPLACE PARTITION 1 FROM dst;
SELECT count(), sum(d) FROM dst;

INSERT INTO test_block_numbers SELECT max(max_block_number) AS m FROM system.parts WHERE database=currentDatabase() AND  table='dst' AND active AND name LIKE '1_%';
SELECT (max(m) - min(m) > 1) AS new_block_is_generated FROM test_block_numbers;
DROP TEMPORARY TABLE test_block_numbers;


SELECT 'ATTACH FROM';
ALTER TABLE dst DROP PARTITION 1;
DROP TABLE src;

CREATE TABLE src (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
INSERT INTO src VALUES (1, '0', 1);
INSERT INTO src VALUES (1, '1', 1);
INSERT INTO src VALUES (2, '2', 1);
INSERT INTO src VALUES (3, '3', 1);

SYSTEM STOP MERGES dst;
INSERT INTO dst VALUES (1, '1', 2), (1, '2', 0);
ALTER TABLE dst ATTACH PARTITION 1 FROM src;
SELECT count(), sum(d) FROM dst;

ALTER TABLE dst ATTACH PARTITION ALL FROM src;
SELECT count(), sum(d) FROM dst;

SELECT 'OPTIMIZE';
SELECT count(), sum(d), uniqExact(_part) FROM dst;
SYSTEM START MERGES dst;
SET optimize_throw_if_noop=1;
OPTIMIZE TABLE dst;
SELECT count(), sum(d), uniqExact(_part) FROM dst;


SELECT 'After restart';
DETACH TABLE dst;
ATTACH TABLE dst;
SELECT count(), sum(d) FROM dst;

SELECT 'DETACH+ATTACH PARTITION';
ALTER TABLE dst DETACH PARTITION 0;
ALTER TABLE dst DETACH PARTITION 1;
ALTER TABLE dst DETACH PARTITION 2;
ALTER TABLE dst ATTACH PARTITION 1;
SELECT count(), sum(d) FROM dst;

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
