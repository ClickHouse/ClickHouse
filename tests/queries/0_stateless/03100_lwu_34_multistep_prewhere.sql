DROP TABLE IF EXISTS t_lwu_multistep;

SET enable_multiple_prewhere_read_steps = 1;
SET enable_lightweight_update = 1;
SET move_all_conditions_to_prewhere = 1;

CREATE TABLE t_lwu_multistep(a UInt64, b UInt64, c UInt64, d UInt64, e UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_multistep SELECT number % 2, number, number, number, number % 2 FROM numbers(100000);

UPDATE t_lwu_multistep SET a = a + 1 WHERE 1;
UPDATE t_lwu_multistep SET b = b + 1 WHERE b < 50000;
UPDATE t_lwu_multistep SET c = c + 1000000 WHERE c < 50000;

SELECT count() FROM t_lwu_multistep WHERE a = 1 AND b > 10000 AND c < 100000;
SELECT count() FROM t_lwu_multistep WHERE a = 0 AND b > 10000 AND c < 100000;
SELECT count() FROM t_lwu_multistep WHERE a = 1 AND b > 10000 AND c > 200000;
SELECT count() FROM t_lwu_multistep WHERE a = 0 AND b > 10000 AND c > 200000;

DROP TABLE IF EXISTS t_lwu_multistep;
