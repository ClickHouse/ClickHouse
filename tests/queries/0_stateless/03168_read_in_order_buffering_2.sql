-- Tags: long, no-random-settings, no-tsan, no-asan, no-msan, no-s3-storage

DROP TABLE IF EXISTS t_read_in_order_2;

CREATE TABLE t_read_in_order_2 (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_read_in_order_2 SELECT number, number FROM numbers(10000000);
OPTIMIZE TABLE t_read_in_order_2 FINAL;

SET optimize_read_in_order = 1;
SET max_threads = 4;
SET read_in_order_use_buffering = 1;
SET max_memory_usage = '100M';

SELECT * FROM t_read_in_order_2 ORDER BY id FORMAT Null;

DROP TABLE t_read_in_order_2;
