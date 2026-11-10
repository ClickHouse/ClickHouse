DROP TABLE IF EXISTS t_lwu_replace;

SET enable_lightweight_update = 1;

CREATE TABLE t_lwu_replace (c0 Int)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO TABLE t_lwu_replace (c0) VALUES (1);
ALTER TABLE t_lwu_replace REPLACE PARTITION ID '0' FROM t_lwu_replace;
UPDATE t_lwu_replace SET c0 = 2 WHERE TRUE;

SELECT * FROM t_lwu_replace ORDER BY c0;
DROP TABLE IF EXISTS t_lwu_replace;

CREATE TABLE t_lwu_replace (c0 Int)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwu_replace', '1') ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO TABLE t_lwu_replace (c0) VALUES (1);
ALTER TABLE t_lwu_replace REPLACE PARTITION ID '0' FROM t_lwu_replace;
UPDATE t_lwu_replace SET c0 = 2 WHERE TRUE;

SELECT * FROM t_lwu_replace ORDER BY c0;
DROP TABLE IF EXISTS t_lwu_replace;
