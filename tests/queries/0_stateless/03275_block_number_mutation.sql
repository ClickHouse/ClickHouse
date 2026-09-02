DROP TABLE IF EXISTS t_block_number_delete sync;

SET mutations_sync = 2;

CREATE TABLE t_block_number_delete (x UInt32, ts DateTime) ENGINE = MergeTree ORDER BY x SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0, min_bytes_for_wide_part = 1;

INSERT INTO t_block_number_delete SELECT number, now() - INTERVAL number minute from numbers(10);
OPTIMIZE TABLE t_block_number_delete final;
ALTER TABLE t_block_number_delete DELETE WHERE x < 2;

SELECT count(), sum(x) FROM t_block_number_delete;
SELECT command, is_done, latest_fail_reason FROM system.mutations WHERE database = currentDatabase() AND table = 't_block_number_delete';
SELECT column, count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_block_number_delete' AND active GROUP BY column ORDER BY column;

DETACH TABLE t_block_number_delete;
ATTACH TABLE t_block_number_delete;

SELECT count(), sum(x) FROM t_block_number_delete;
SELECT command, is_done, latest_fail_reason FROM system.mutations WHERE database = currentDatabase() AND table = 't_block_number_delete';
SELECT column, count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_block_number_delete' AND active GROUP BY column ORDER BY column;

DROP TABLE t_block_number_delete;

CREATE TABLE t_block_number_delete (x UInt32, ts DateTime) ENGINE = MergeTree ORDER BY x SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0, min_bytes_for_wide_part = '10G';

INSERT INTO t_block_number_delete SELECT number, now() - INTERVAL number minute from numbers(10);
OPTIMIZE TABLE t_block_number_delete final;
ALTER TABLE t_block_number_delete DELETE WHERE x < 2;

SELECT count(), sum(x) FROM t_block_number_delete;
SELECT command, is_done, latest_fail_reason FROM system.mutations WHERE database = currentDatabase() AND table = 't_block_number_delete';
SELECT column, count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_block_number_delete' AND active GROUP BY column ORDER BY column;

DETACH TABLE t_block_number_delete;
ATTACH TABLE t_block_number_delete;

SELECT count(), sum(x) FROM t_block_number_delete;
SELECT command, is_done, latest_fail_reason FROM system.mutations WHERE database = currentDatabase() AND table = 't_block_number_delete';
SELECT column, count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_block_number_delete' AND active GROUP BY column ORDER BY column;

DROP TABLE t_block_number_delete;
