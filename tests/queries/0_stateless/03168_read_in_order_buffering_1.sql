DROP TABLE IF EXISTS t_read_in_order_1;

CREATE TABLE t_read_in_order_1 (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1024, index_granularity_bytes = '10M';

INSERT INTO t_read_in_order_1 SELECT number, number FROM numbers(1000000);

SET max_threads = 8;
SET optimize_read_in_order = 1;
SET read_in_order_use_buffering = 1;

SELECT count() FROM
(
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_1 ORDER BY id
) WHERE explain LIKE '%BufferChunks%';

SELECT count() FROM
(
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_1 ORDER BY id LIMIT 10
) WHERE explain LIKE '%BufferChunks%';

SELECT count() FROM
(
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_1 WHERE v % 10 = 0 ORDER BY id LIMIT 10
) WHERE explain LIKE '%BufferChunks%';

SET read_in_order_use_buffering = 0;

SELECT count() FROM
(
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_1 ORDER BY id
) WHERE explain LIKE '%BufferChunks%';

SELECT count() FROM
(
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_1 ORDER BY id LIMIT 10
) WHERE explain LIKE '%BufferChunks%';

SELECT count() FROM
(
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_1 WHERE v % 10 = 0 ORDER BY id LIMIT 10
) WHERE explain LIKE '%BufferChunks%';

DROP TABLE t_read_in_order_1;
