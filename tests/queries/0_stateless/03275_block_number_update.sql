DROP TABLE IF EXISTS t_block_number_mut;

SET mutations_sync = 2;

CREATE TABLE t_block_number_mut (n int) ENGINE = MergeTree ORDER BY tuple() SETTINGS enable_block_number_column = 1, min_bytes_for_wide_part = 0;

INSERT INTO t_block_number_mut VALUES (1) (2);

OPTIMIZE TABLE t_block_number_mut FINAL;

ALTER TABLE t_block_number_mut UPDATE n = n + 1 WHERE 1;

SELECT * FROM t_block_number_mut;

DROP TABLE IF EXISTS t_block_number_mut;

CREATE TABLE t_block_number_mut (n int) ENGINE = MergeTree ORDER BY tuple() SETTINGS enable_block_number_column = 1, min_bytes_for_wide_part = '1G';

INSERT INTO t_block_number_mut VALUES (1) (2);

OPTIMIZE TABLE t_block_number_mut FINAL;

ALTER TABLE t_block_number_mut UPDATE n = n + 1 WHERE 1;

SELECT * FROM t_block_number_mut;

DROP TABLE IF EXISTS t_block_number_mut;
