-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

DROP TABLE IF EXISTS t_dist_read_in_order;

-- Three parts with fully interleaved key ranges, so an ordered result needs a real merge across the
-- parts rather than plain concatenation. Small granules so a LIMIT stops well inside a part.
CREATE TABLE t_dist_read_in_order (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_dist_read_in_order;
INSERT INTO t_dist_read_in_order SELECT number * 3, number FROM numbers(30000);
INSERT INTO t_dist_read_in_order SELECT number * 3 + 1, number FROM numbers(30000);
INSERT INTO t_dist_read_in_order SELECT number * 3 + 2, number FROM numbers(30000);

-- Distributed aggregation cannot enforce a global max_rows_to_group_by, so pin it to 0 (randomized
-- settings set it nonzero, which would make make_distributed_plan reject the aggregates below).
SET max_rows_to_group_by = 0;

SET distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3;
SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0;
SET automatic_parallel_replicas_mode = 0;

-- optimize_read_in_order and read_in_order_use_virtual_row are both randomized by the test runner, and
-- with the optimization off these queries would still return the right rows while testing nothing. Pin
-- them so the case always exercises the in-order read, and always the same variant of it.
SET optimize_read_in_order = 1, read_in_order_use_virtual_row = 0;

-- The distributed read-in-order path is off by default.
SET distributed_plan_read_in_order = 1;

-- Reading in the key's own order used to be rejected outright (SUPPORT_IS_DISABLED) because a bucketed
-- read is pinned to the coordinator's marks and cannot re-derive it. The contract now travels with the
-- step, so these return exactly what a non-distributed read returns.
SELECT k FROM t_dist_read_in_order ORDER BY k LIMIT 5;
SELECT k FROM t_dist_read_in_order ORDER BY k DESC LIMIT 5;

-- A limit spanning many granules of every part, so a stream that is ordered only within its own part
-- shows up as a wrong sum rather than a wrong first row.
SELECT sum(k), count() FROM (SELECT k FROM t_dist_read_in_order ORDER BY k LIMIT 1000);
SELECT sum(k), count() FROM (SELECT k FROM t_dist_read_in_order ORDER BY k DESC LIMIT 1000);

-- OFFSET makes a wrong merge visible even when the head of the stream happens to be right.
SELECT k FROM t_dist_read_in_order ORDER BY k LIMIT 5 OFFSET 44444;

-- The key prefix, not the whole key: the merge must compare on the prefix the optimization reported.
SELECT k, v FROM t_dist_read_in_order WHERE v < 3 ORDER BY k LIMIT 4;

DROP TABLE t_dist_read_in_order;

DROP TABLE IF EXISTS t_dist_read_in_order_desc;

-- A descending storage key: the read direction and the key's own reverse flag compose, so a merge that
-- honours only one of the two returns rows from the wrong end.
CREATE TABLE t_dist_read_in_order_desc (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY (k DESC)
SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_dist_read_in_order_desc;
INSERT INTO t_dist_read_in_order_desc SELECT number * 2, number FROM numbers(20000);
INSERT INTO t_dist_read_in_order_desc SELECT number * 2 + 1, number FROM numbers(20000);

SELECT k FROM t_dist_read_in_order_desc ORDER BY k DESC LIMIT 5;
SELECT k FROM t_dist_read_in_order_desc ORDER BY k ASC LIMIT 5;
SELECT sum(k), count() FROM (SELECT k FROM t_dist_read_in_order_desc ORDER BY k DESC LIMIT 1000);
SELECT sum(k), count() FROM (SELECT k FROM t_dist_read_in_order_desc ORDER BY k ASC LIMIT 1000);

DROP TABLE t_dist_read_in_order_desc;
