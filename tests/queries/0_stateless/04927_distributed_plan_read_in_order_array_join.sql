-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

DROP TABLE IF EXISTS t_dist_rio_array_join;

-- Three parts with fully interleaved key ranges, so an ordered result needs a real merge across the
-- parts rather than plain concatenation. Small granules so a LIMIT stops well inside a part.
CREATE TABLE t_dist_rio_array_join (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_dist_rio_array_join;
INSERT INTO t_dist_rio_array_join SELECT number * 3, number FROM numbers(30000);
INSERT INTO t_dist_rio_array_join SELECT number * 3 + 1, number FROM numbers(30000);
INSERT INTO t_dist_rio_array_join SELECT number * 3 + 2, number FROM numbers(30000);

SET distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3;
SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0;
SET automatic_parallel_replicas_mode = 0;

-- optimize_read_in_order is randomized by the test runner, and with it off the rows below would be right
-- while testing nothing. Pin it so the case always exercises the ordered read.
SET optimize_read_in_order = 1, read_in_order_use_virtual_row = 0;

-- The distributed read-in-order path is off by default.
SET distributed_plan_read_in_order = 1;

-- `optimizeExchanges` lifts the gather a distributed read leaves over the read only through the steps
-- `canHoistGatherThroughStep` accepts, and `ARRAY JOIN` is not one of them. So the scatter under the
-- sorting stays between the sorting and the read, and nothing above may rely on the read's order. While
-- the read was asked to read in order here anyway, the sorting merged streams the scatter had already
-- interleaved and this returned rows from the wrong part of the table on every run.
SELECT k FROM t_dist_rio_array_join ARRAY JOIN [1, 2] AS x ORDER BY k ASC LIMIT 10 OFFSET 24990;

-- Same shape with a filter between the read and the array join, and descending.
SELECT k FROM t_dist_rio_array_join ARRAY JOIN [1, 2] AS x WHERE v > 5 ORDER BY k DESC LIMIT 10;

DROP TABLE t_dist_rio_array_join;
