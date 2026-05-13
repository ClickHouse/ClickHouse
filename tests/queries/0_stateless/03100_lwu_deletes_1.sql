DROP TABLE IF EXISTS t_lwu_delete;

CREATE TABLE t_lwu_delete (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t_lwu_delete;

INSERT INTO t_lwu_delete SELECT number, number FROM numbers(10000);

SET enable_lightweight_update = 1;
SET lightweight_delete_mode = 'lightweight_update_force';

SELECT sum(v) FROM t_lwu_delete;
SELECT count() FROM t_lwu_delete;

DELETE FROM t_lwu_delete WHERE id % 4 = 1;

SELECT sum(v) FROM t_lwu_delete;
SELECT count() FROM t_lwu_delete;

UPDATE t_lwu_delete SET v = v + 1000 WHERE id % 10 = 0;

SELECT sum(v) FROM t_lwu_delete;
SELECT count() FROM t_lwu_delete;

DELETE FROM t_lwu_delete WHERE id % 2 = 0;

SELECT sum(v) FROM t_lwu_delete;
SELECT count() FROM t_lwu_delete;

DROP TABLE IF EXISTS t_lwu_delete;
