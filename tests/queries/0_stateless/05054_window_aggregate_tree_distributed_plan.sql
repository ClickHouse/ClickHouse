-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Under make_distributed_plan the WindowStep is serialized for the worker fragment, and it must
-- carry the initiator's min_window_frame_rows_for_aggregate_tree: the threshold decides between the
-- frame aggregate tree and the recompute path, whose floating-point results are not bit-identical.
-- The distributed plan prints the threshold of every Window step that can reach it: a sliding frame the tree
-- serves, large enough for the threshold. The window without PARTITION BY runs above the sorted gather; the one with PARTITION BY is
-- rebuilt per bucket below the gather (makeDistributed copies the threshold there).
-- Only the Window, GatherExchange and threshold lines of each plan are checked: the rest of the plan
-- (the reading step in particular) depends on the storage settings.

DROP TABLE IF EXISTS t_window_tree_dist;

CREATE TABLE t_window_tree_dist (n UInt32, i Int64) ENGINE = MergeTree ORDER BY n;

INSERT INTO t_window_tree_dist SELECT number, (cityHash64(number) % 201) - 100 FROM numbers(20000);

-- max_rows_to_group_by must be 0, otherwise make_distributed_plan declines plans with an aggregation.
SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0,
    distributed_plan_default_shuffle_join_bucket_count = 8, distributed_plan_default_reader_bucket_count = 8,
    optimize_read_in_order = 0, optimize_sorting_by_input_stream_properties = 1,
    distributed_plan_optimize_exchanges = 1, max_rows_to_group_by = 0;

SELECT '-- the window above the sorted gather carries the threshold set on the initiator';
SELECT explain FROM (EXPLAIN SELECT sum(i) OVER w AS s FROM t_window_tree_dist WINDOW w AS (ORDER BY n ROWS BETWEEN 2999 PRECEDING AND CURRENT ROW)
    SETTINGS min_window_frame_rows_for_aggregate_tree = 1000)
WHERE explain LIKE '%Window (%' OR explain LIKE '%GatherExchange%' OR explain LIKE '%Aggregate tree threshold%';

SELECT '-- and a threshold above the frame size';
SELECT explain FROM (EXPLAIN SELECT sum(i) OVER w AS s FROM t_window_tree_dist WINDOW w AS (ORDER BY n ROWS BETWEEN 2999 PRECEDING AND CURRENT ROW)
    SETTINGS min_window_frame_rows_for_aggregate_tree = 1000000000)
WHERE explain LIKE '%Window (%' OR explain LIKE '%GatherExchange%' OR explain LIKE '%Aggregate tree threshold%';

SELECT '-- the per-bucket window below the gather carries the threshold set on the initiator';
SELECT explain FROM (EXPLAIN SELECT sum(i) OVER w AS s FROM (SELECT *, n % 2 AS p FROM t_window_tree_dist) WINDOW w AS (PARTITION BY p ORDER BY n ROWS BETWEEN 2999 PRECEDING AND CURRENT ROW)
    SETTINGS min_window_frame_rows_for_aggregate_tree = 1000)
WHERE explain LIKE '%Window (%' OR explain LIKE '%GatherExchange%' OR explain LIKE '%Aggregate tree threshold%';

SELECT '-- and a threshold above the frame size';
SELECT explain FROM (EXPLAIN SELECT sum(i) OVER w AS s FROM (SELECT *, n % 2 AS p FROM t_window_tree_dist) WINDOW w AS (PARTITION BY p ORDER BY n ROWS BETWEEN 2999 PRECEDING AND CURRENT ROW)
    SETTINGS min_window_frame_rows_for_aggregate_tree = 1000000000)
WHERE explain LIKE '%Window (%' OR explain LIKE '%GatherExchange%' OR explain LIKE '%Aggregate tree threshold%';

SELECT '-- a RANGE or GROUPS frame without ORDER BY keeps its start at the partition start, so no threshold applies';
SELECT explain FROM (EXPLAIN SELECT sum(i) OVER w AS s FROM (SELECT *, n % 2 AS p FROM t_window_tree_dist) WINDOW w AS (PARTITION BY p RANGE BETWEEN CURRENT ROW AND CURRENT ROW)
    SETTINGS min_window_frame_rows_for_aggregate_tree = 1000)
WHERE explain LIKE '%Window (%' OR explain LIKE '%GatherExchange%' OR explain LIKE '%Aggregate tree threshold%';
SELECT explain FROM (EXPLAIN SELECT sum(i) OVER w AS s FROM (SELECT *, n % 2 AS p FROM t_window_tree_dist) WINDOW w AS (PARTITION BY p GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW)
    SETTINGS min_window_frame_rows_for_aggregate_tree = 1000)
WHERE explain LIKE '%Window (%' OR explain LIKE '%GatherExchange%' OR explain LIKE '%Aggregate tree threshold%';

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

-- Last: `SET compatibility = DEFAULT` would not restore the settings the compatibility changed.
-- The compatibility also switches EXPLAIN to the legacy format, which prints no step actions.
SET compatibility = '26.6';
SET explain_query_plan_default = 'pretty';

SELECT '-- compatibility disables the tree in both window shapes';
SELECT explain FROM (EXPLAIN SELECT sum(i) OVER w AS s FROM t_window_tree_dist WINDOW w AS (ORDER BY n ROWS BETWEEN 2999 PRECEDING AND CURRENT ROW))
WHERE explain LIKE '%Window (%' OR explain LIKE '%GatherExchange%' OR explain LIKE '%Aggregate tree threshold%';
SELECT explain FROM (EXPLAIN SELECT sum(i) OVER w AS s FROM (SELECT *, n % 2 AS p FROM t_window_tree_dist) WINDOW w AS (PARTITION BY p ORDER BY n ROWS BETWEEN 2999 PRECEDING AND CURRENT ROW))
WHERE explain LIKE '%Window (%' OR explain LIKE '%GatherExchange%' OR explain LIKE '%Aggregate tree threshold%';

DROP TABLE t_window_tree_dist;
