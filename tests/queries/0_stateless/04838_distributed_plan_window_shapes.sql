-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- PARTITION BY window shapes under make_distributed_plan: pin the full distributed EXPLAIN of each
-- case, then run the same query distributed and local. The two result lines per case must show the
-- same value in the reference. Before partitioned sorts became serializable, every one of these
-- queries failed with SUPPORT_IS_DISABLED.

DROP TABLE IF EXISTS t_window_shapes;

CREATE TABLE t_window_shapes (a UInt32, b UInt32, v UInt32)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 256;

-- The plan snapshots below include part and granule counts, so the parts must not merge mid-test.
SYSTEM STOP MERGES t_window_shapes;

INSERT INTO t_window_shapes SELECT number % 5, number % 3, number FROM numbers(1000);

-- max_rows_to_group_by must be 0, otherwise make_distributed_plan declines plans with an aggregation.
SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0,
    distributed_plan_default_shuffle_join_bucket_count = 8, distributed_plan_default_reader_bucket_count = 8,
    optimize_read_in_order = 0, optimize_sorting_by_input_stream_properties = 1,
    distributed_plan_optimize_exchanges = 1, max_rows_to_group_by = 0;

SELECT '-- two windows with different partition keys';
EXPLAIN SELECT sum(v) OVER (PARTITION BY a ORDER BY v) AS s1, sum(v) OVER (PARTITION BY b ORDER BY v) AS s2 FROM t_window_shapes;
SELECT sum(cityHash64(v, s1, s2)) FROM (SELECT v, sum(v) OVER (PARTITION BY a ORDER BY v) AS s1, sum(v) OVER (PARTITION BY b ORDER BY v) AS s2 FROM t_window_shapes);
SELECT sum(cityHash64(v, s1, s2)) FROM (SELECT v, sum(v) OVER (PARTITION BY a ORDER BY v) AS s1, sum(v) OVER (PARTITION BY b ORDER BY v) AS s2 FROM t_window_shapes) SETTINGS make_distributed_plan = 0;

SELECT '-- expression partition key';
EXPLAIN SELECT sum(v) OVER (PARTITION BY a % 2 ORDER BY v) AS s FROM t_window_shapes;
SELECT sum(cityHash64(v, s)) FROM (SELECT v, sum(v) OVER (PARTITION BY a % 2 ORDER BY v) AS s FROM t_window_shapes);
SELECT sum(cityHash64(v, s)) FROM (SELECT v, sum(v) OVER (PARTITION BY a % 2 ORDER BY v) AS s FROM t_window_shapes) SETTINGS make_distributed_plan = 0;

SELECT '-- two partition columns';
EXPLAIN SELECT sum(v) OVER (PARTITION BY a, b ORDER BY v) AS s FROM t_window_shapes;
SELECT sum(cityHash64(v, s)) FROM (SELECT v, sum(v) OVER (PARTITION BY a, b ORDER BY v) AS s FROM t_window_shapes);
SELECT sum(cityHash64(v, s)) FROM (SELECT v, sum(v) OVER (PARTITION BY a, b ORDER BY v) AS s FROM t_window_shapes) SETTINGS make_distributed_plan = 0;

SELECT '-- window over a join';
-- The estimate settings are pinned because the snapshot includes the join cardinality estimates.
EXPLAIN SELECT sum(t1.v) OVER (PARTITION BY t1.a ORDER BY t1.v) AS s FROM t_window_shapes t1 INNER JOIN t_window_shapes t2 ON t1.v = t2.v
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 'false',
    use_statistics = 0, query_plan_optimize_join_order_randomize = 0, query_plan_optimize_join_order_limit = 10;
SELECT sum(cityHash64(v, s)) FROM (SELECT t1.v AS v, sum(t1.v) OVER (PARTITION BY t1.a ORDER BY t1.v) AS s FROM t_window_shapes t1 INNER JOIN t_window_shapes t2 ON t1.v = t2.v);
SELECT sum(cityHash64(v, s)) FROM (SELECT t1.v AS v, sum(t1.v) OVER (PARTITION BY t1.a ORDER BY t1.v) AS s FROM t_window_shapes t1 INNER JOIN t_window_shapes t2 ON t1.v = t2.v) SETTINGS make_distributed_plan = 0;

SELECT '-- window over a window';
EXPLAIN SELECT sum(s) OVER (PARTITION BY b ORDER BY v) AS s2 FROM (SELECT b, v, sum(v) OVER (PARTITION BY a ORDER BY v) AS s FROM t_window_shapes);
SELECT sum(cityHash64(v, s2)) FROM (SELECT v, sum(s) OVER (PARTITION BY b ORDER BY v) AS s2 FROM (SELECT b, v, sum(v) OVER (PARTITION BY a ORDER BY v) AS s FROM t_window_shapes));
SELECT sum(cityHash64(v, s2)) FROM (SELECT v, sum(s) OVER (PARTITION BY b ORDER BY v) AS s2 FROM (SELECT b, v, sum(v) OVER (PARTITION BY a ORDER BY v) AS s FROM t_window_shapes)) SETTINGS make_distributed_plan = 0;

SELECT '-- partitioned window with ORDER BY and LIMIT';
EXPLAIN SELECT a, v, sum(v) OVER (PARTITION BY a ORDER BY v) AS s FROM t_window_shapes ORDER BY a, v LIMIT 10;
SELECT sum(cityHash64(a, v, s)) FROM (SELECT a, v, sum(v) OVER (PARTITION BY a ORDER BY v) AS s FROM t_window_shapes ORDER BY a, v LIMIT 10);
SELECT sum(cityHash64(a, v, s)) FROM (SELECT a, v, sum(v) OVER (PARTITION BY a ORDER BY v) AS s FROM t_window_shapes ORDER BY a, v LIMIT 10) SETTINGS make_distributed_plan = 0;

SELECT '-- partition without ORDER BY in the window';
EXPLAIN SELECT sum(v) OVER (PARTITION BY a) AS s FROM t_window_shapes;
SELECT sum(cityHash64(v, s)) FROM (SELECT v, sum(v) OVER (PARTITION BY a) AS s FROM t_window_shapes);
SELECT sum(cityHash64(v, s)) FROM (SELECT v, sum(v) OVER (PARTITION BY a) AS s FROM t_window_shapes) SETTINGS make_distributed_plan = 0;

SELECT '-- ROWS frame';
EXPLAIN SELECT sum(v) OVER (PARTITION BY a ORDER BY v ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM t_window_shapes;
SELECT sum(cityHash64(v, s)) FROM (SELECT v, sum(v) OVER (PARTITION BY a ORDER BY v ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM t_window_shapes);
SELECT sum(cityHash64(v, s)) FROM (SELECT v, sum(v) OVER (PARTITION BY a ORDER BY v ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM t_window_shapes) SETTINGS make_distributed_plan = 0;

SELECT '-- RANGE frame';
EXPLAIN SELECT sum(v) OVER (PARTITION BY a ORDER BY v RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) AS s FROM t_window_shapes;
SELECT sum(cityHash64(v, s)) FROM (SELECT v, sum(v) OVER (PARTITION BY a ORDER BY v RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) AS s FROM t_window_shapes);
SELECT sum(cityHash64(v, s)) FROM (SELECT v, sum(v) OVER (PARTITION BY a ORDER BY v RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) AS s FROM t_window_shapes) SETTINGS make_distributed_plan = 0;

-- A float partition key must not be scattered by hash: the scatter hashes the raw bit pattern while
-- the window groups partitions with compareAt, and the two disagree on -0.0 vs +0.0 and on different
-- NaN encodings, so one logical partition would split across buckets. The plan must keep the window
-- gathered (no "scatter by" in the snapshot). The table mixes both zero signs and two NaN bit
-- patterns to cover exactly those values.
DROP TABLE IF EXISTS t_window_shapes_float;
CREATE TABLE t_window_shapes_float (k Float64, v UInt32)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 256;
SYSTEM STOP MERGES t_window_shapes_float;
INSERT INTO t_window_shapes_float SELECT
    multiIf(number % 4 = 0, -0.0,
            number % 4 = 1, 0.0,
            number % 4 = 2, reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9221120237041090560))),
            reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(18444492273895866368)))),
    number
FROM numbers(1000);

SELECT '-- float partition key stays gathered';
EXPLAIN SELECT sum(v) OVER (PARTITION BY k ORDER BY v) AS s FROM t_window_shapes_float;
SELECT sum(cityHash64(v, s)) FROM (SELECT v, sum(v) OVER (PARTITION BY k ORDER BY v) AS s FROM t_window_shapes_float);
SELECT sum(cityHash64(v, s)) FROM (SELECT v, sum(v) OVER (PARTITION BY k ORDER BY v) AS s FROM t_window_shapes_float) SETTINGS make_distributed_plan = 0;

DROP TABLE t_window_shapes_float;
DROP TABLE t_window_shapes;
