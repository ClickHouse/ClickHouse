DROP TABLE IF EXISTS t_lightweight_8;
SET enable_lightweight_update = 1;

CREATE TABLE t_lightweight_8 (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1024, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lightweight_8 SELECT number, 0 FROM numbers(100000);

UPDATE t_lightweight_8 SET v = 1 WHERE id % 20000 = 0;

SET max_block_size = 8192;

SELECT sum(v) FROM t_lightweight_8;

DROP TABLE t_lightweight_8;
