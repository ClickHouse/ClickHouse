-- A table the distributed planner keeps as one read task is read by every node. Its byte budget
-- counts the bytes the read scans, not only the rows it outputs: a filter the primary key cannot
-- use leaves the whole table to scan on every node even when few rows survive. Without this the
-- ten estimated rows passed the budget and every node scanned the 100000-row table.
SET enable_analyzer = 1;
-- The plan lines below show the filter moved to PREWHERE; the runner randomizes the move.
SET optimize_move_to_prewhere = 1;
SET enable_parallel_replicas = 0;
SET explain_query_plan_default = 'legacy';
SET max_rows_to_group_by = 0;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET distributed_plan_default_shuffle_join_bucket_count = 2;
SET distributed_plan_default_reader_bucket_count = 2;
SET distributed_plan_optimize_exchanges = 1;
SET use_statistics = 0;

DROP TABLE IF EXISTS t_rs_big;
DROP TABLE IF EXISTS t_rs_small;
CREATE TABLE t_rs_big (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_rs_small (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_rs_big SELECT number % 1000, number FROM numbers(100000);
INSERT INTO t_rs_small SELECT number, number FROM numbers(100000);

SET param__internal_join_table_stat_hints = '{"t_rs_small": {"cardinality": 10, "avg_row_bytes": 16}, "t_rs_big": {"cardinality": 100000, "avg_row_bytes": 16}}';

SELECT '-- ten estimated rows, but 100000 rows to scan on every node: above a 1 MB budget the read is distributed';
EXPLAIN SELECT count() FROM t_rs_big AS b JOIN t_rs_small AS s ON b.k = s.k WHERE s.v % 7 = 1
SETTINGS distributed_plan_max_bytes_to_broadcast = 1000000;

SELECT '-- within a 10 MB budget the small result is read by every node';
EXPLAIN SELECT count() FROM t_rs_big AS b JOIN t_rs_small AS s ON b.k = s.k WHERE s.v % 7 = 1
SETTINGS distributed_plan_max_bytes_to_broadcast = 10000000;

DROP TABLE t_rs_big;
DROP TABLE t_rs_small;
