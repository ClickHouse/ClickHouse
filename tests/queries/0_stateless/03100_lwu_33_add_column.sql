DROP TABLE IF EXISTS t_lwu_add_column;
SET enable_lightweight_update = 1;

CREATE TABLE t_lwu_add_column(a UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_add_column (a) SELECT number FROM numbers(100000);

ALTER TABLE t_lwu_add_column ADD COLUMN b UInt64;

UPDATE t_lwu_add_column SET b = 1 WHERE a % 2 = 0;

ALTER TABLE t_lwu_add_column ADD COLUMN c Array(String);

UPDATE t_lwu_add_column SET b = 2, c = ['a', 'b', 'c'] WHERE a % 3 = 0;

SELECT a % 6 AS n, sum(b), groupUniqArray(c) FROM t_lwu_add_column GROUP BY n ORDER BY n;
SELECT * FROM t_lwu_add_column ORDER BY a LIMIT 10;

DROP TABLE IF EXISTS t_lwu_add_column;
