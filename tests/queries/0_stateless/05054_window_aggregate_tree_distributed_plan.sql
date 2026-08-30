-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Under make_distributed_plan the WindowStep is serialized for the worker fragment, and it
-- must carry the initiator's min_window_frame_rows_for_aggregate_tree: the frame aggregate
-- tree re-associates floating-point sums, so on this rounding-sensitive pattern the tree
-- and the recompute path give bitwise different results. All variants run through the
-- distributed plan so their block layout is the same, and only the threshold differs.
-- The window without PARTITION BY runs above the sorted gather; the one with PARTITION BY
-- is rebuilt per bucket below the gather (makeDistributed copies the threshold there).

DROP TABLE IF EXISTS t_window_tree_dist;
DROP TABLE IF EXISTS t_window_tree_dist_results;

CREATE TABLE t_window_tree_dist (n UInt32, v Float64, i Int64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_window_tree_dist
SELECT number, multiIf(number % 3 = 0, 1e16, number % 3 = 1, -1e16, 1.), (cityHash64(number) % 201) - 100
FROM numbers(20000);

CREATE TABLE t_window_tree_dist_results (variant String, r UInt64) ENGINE = Memory;

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0, max_block_size = 123;

INSERT INTO t_window_tree_dist_results
SELECT 'tree', groupBitXor(reinterpretAsUInt64(s))
FROM (SELECT sum(v) OVER w AS s FROM t_window_tree_dist WINDOW w AS (ORDER BY n ROWS BETWEEN 2999 PRECEDING AND CURRENT ROW));

INSERT INTO t_window_tree_dist_results
SELECT 'recompute', groupBitXor(reinterpretAsUInt64(s))
FROM (SELECT sum(v) OVER w AS s FROM t_window_tree_dist WINDOW w AS (ORDER BY n ROWS BETWEEN 2999 PRECEDING AND CURRENT ROW))
SETTINGS min_window_frame_rows_for_aggregate_tree = 1000000000;

INSERT INTO t_window_tree_dist_results
SELECT 'tree_partitioned', groupBitXor(reinterpretAsUInt64(s))
FROM (SELECT sum(v) OVER w AS s FROM (SELECT *, n % 2 AS p FROM t_window_tree_dist) WINDOW w AS (PARTITION BY p ORDER BY n ROWS BETWEEN 2999 PRECEDING AND CURRENT ROW));

INSERT INTO t_window_tree_dist_results
SELECT 'recompute_partitioned', groupBitXor(reinterpretAsUInt64(s))
FROM (SELECT sum(v) OVER w AS s FROM (SELECT *, n % 2 AS p FROM t_window_tree_dist) WINDOW w AS (PARTITION BY p ORDER BY n ROWS BETWEEN 2999 PRECEDING AND CURRENT ROW))
SETTINGS min_window_frame_rows_for_aggregate_tree = 1000000000;

-- Last: `SET compatibility = DEFAULT` would not restore the settings the compatibility changed.
SET compatibility = '26.6';
INSERT INTO t_window_tree_dist_results
SELECT 'compatibility', groupBitXor(reinterpretAsUInt64(s))
FROM (SELECT sum(v) OVER w AS s FROM t_window_tree_dist WINDOW w AS (ORDER BY n ROWS BETWEEN 2999 PRECEDING AND CURRENT ROW));

INSERT INTO t_window_tree_dist_results
SELECT 'compatibility_partitioned', groupBitXor(reinterpretAsUInt64(s))
FROM (SELECT sum(v) OVER w AS s FROM (SELECT *, n % 2 AS p FROM t_window_tree_dist) WINDOW w AS (PARTITION BY p ORDER BY n ROWS BETWEEN 2999 PRECEDING AND CURRENT ROW));

SELECT '-- the tree is active above the threshold in the worker fragment: its bits differ from the forced recompute';
SELECT (SELECT r FROM t_window_tree_dist_results WHERE variant = 'tree') != (SELECT r FROM t_window_tree_dist_results WHERE variant = 'recompute');
SELECT (SELECT r FROM t_window_tree_dist_results WHERE variant = 'tree_partitioned') != (SELECT r FROM t_window_tree_dist_results WHERE variant = 'recompute_partitioned');

SELECT '-- compatibility disables the tree in the worker fragment: its bits match the forced recompute';
SELECT (SELECT r FROM t_window_tree_dist_results WHERE variant = 'compatibility') = (SELECT r FROM t_window_tree_dist_results WHERE variant = 'recompute');
SELECT (SELECT r FROM t_window_tree_dist_results WHERE variant = 'compatibility_partitioned') = (SELECT r FROM t_window_tree_dist_results WHERE variant = 'recompute_partitioned');

SELECT '-- exact integer aggregates above the threshold match between the distributed and the plain plan';
SELECT countIf(NOT (s = s2 AND mn = mn2 AND c = c2)) AS mismatches
FROM
(
    SELECT n, sum(i) OVER w AS s, min(i) OVER w AS mn, count() OVER w AS c
    FROM (SELECT *, n % 2 AS p FROM t_window_tree_dist) WINDOW w AS (PARTITION BY p ORDER BY n ROWS BETWEEN 2999 PRECEDING AND CURRENT ROW)
) AS dist
INNER JOIN
(
    SELECT n, sum(i) OVER w AS s2, min(i) OVER w AS mn2, count() OVER w AS c2
    FROM (SELECT *, n % 2 AS p FROM t_window_tree_dist) WINDOW w AS (PARTITION BY p ORDER BY n ROWS BETWEEN 2999 PRECEDING AND CURRENT ROW)
    SETTINGS make_distributed_plan = 0
) AS plain USING (n);

DROP TABLE t_window_tree_dist_results;
DROP TABLE t_window_tree_dist;
