DROP TABLE IF EXISTS t_lightweight SYNC;
SET enable_lightweight_update = 1;

CREATE TABLE t_lightweight (id UInt64, c1 UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lightweight SELECT number, number FROM numbers(20);

UPDATE t_lightweight SET c1 = c1 + 100 WHERE id % 2 = 0;
UPDATE t_lightweight SET c1 = c1 + 1000 WHERE id % 3 = 0;
UPDATE t_lightweight SET c1 = 10000 WHERE id = 10;
UPDATE t_lightweight SET c1 = 13000 WHERE id = 10;
UPDATE t_lightweight SET c1 = 15000 WHERE id = 15;

SELECT * FROM t_lightweight ORDER BY id SETTINGS apply_patch_parts = 1;
SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_lightweight' AND active ORDER BY min_block_number;

OPTIMIZE TABLE t_lightweight PARTITION ID 'patch-3e1a7650697c132eb044cc6f1d82bc92-all' FINAL;

SELECT * FROM t_lightweight ORDER BY id SETTINGS apply_patch_parts = 1;
SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_lightweight' AND active ORDER BY min_block_number;
SELECT count() FROM t_lightweight WHERE c1 != id;

DROP TABLE t_lightweight SYNC;
