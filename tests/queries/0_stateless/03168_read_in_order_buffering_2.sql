-- Tags: long, no-random-settings, no-tsan, no-asan, no-msan

DROP TABLE IF EXISTS t_read_in_order_2;

CREATE TABLE t_read_in_order_2 (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_read_in_order_2 SELECT number, number FROM numbers(100000000);

SET optimize_read_in_order = 1;
SET max_threads = 8;
SET read_in_order_max_bytes_to_buffer = '80M';
SET max_memory_usage = '250M';

SELECT * FROM t_read_in_order_2 ORDER BY id FORMAT Null;

DROP TABLE t_read_in_order_2;
