-- Tags: long, no-random-settings, no-tsan, no-asan, no-msan, no-object-storage

DROP TABLE IF EXISTS t_read_in_order_2;

CREATE TABLE t_read_in_order_2 (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;

-- Use multiple parts so that PrefetchingConcatProcessor is not used
-- (it requires a single part). This test is about memory behavior of
-- per-stream buffering in read-in-order with a tight memory limit.
SYSTEM STOP MERGES t_read_in_order_2;
INSERT INTO t_read_in_order_2 SELECT number, number FROM numbers(5000000);
INSERT INTO t_read_in_order_2 SELECT number + 5000000, number FROM numbers(5000000);

SET optimize_read_in_order = 1;
SET max_threads = 4;
SET read_in_order_use_buffering = 1;
SET max_memory_usage = '100M';

SELECT * FROM t_read_in_order_2 ORDER BY id FORMAT Null;

DROP TABLE t_read_in_order_2;
