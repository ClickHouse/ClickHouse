-- ExpressionAnalyzer used to pass nullopt into `preferParallelHashLayout`, which
-- treats a missing estimate as parallel. Walk `joined_plan` so a 200-row MergeTree
-- right side stays serial under threshold 100000 (`enable_analyzer = 0`).
--
-- `join_algorithm = 'auto'` used to construct `JoinSwitcher` with `max_threads = 1`.
-- The hash phase now follows the same threshold as a bare `HashJoin`.
--
-- Join-order stats stay on so the planner AUTO path gets MergeTree `totalRows` (200).
-- The legacy analyzer has no join-order pass; it walks `joined_plan` with `estimateReadRowsCount`.

SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET collect_hash_table_stats_during_joins = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET explain_query_plan_default = 'legacy';
SET max_threads = 16;

DROP TABLE IF EXISTS t05045_l;
DROP TABLE IF EXISTS t05045_r;
CREATE TABLE t05045_l (n UInt64) ENGINE = MergeTree ORDER BY n;
CREATE TABLE t05045_r (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t05045_l SELECT number FROM numbers(100);
INSERT INTO t05045_r SELECT number FROM numbers(200);

SELECT 'legacy_analyzer_serial';
SET enable_analyzer = 0;
SET join_algorithm = 'hash';
SET parallel_hash_join_threshold = 100000;
SELECT countIf(explain LIKE '%FillingRightJoinSide%')
FROM (
    EXPLAIN PIPELINE
    SELECT t1.n FROM t05045_l AS t1 INNER JOIN t05045_r AS t2 ON t1.n = t2.n
);

SELECT 'legacy_analyzer_parallel';
SET parallel_hash_join_threshold = 1;
SELECT countIf(explain LIKE '%FillingRightJoinSide%')
FROM (
    EXPLAIN PIPELINE
    SELECT t1.n FROM t05045_l AS t1 INNER JOIN t05045_r AS t2 ON t1.n = t2.n
);

SELECT 'join_switcher_serial';
SET enable_analyzer = 1;
SET join_algorithm = 'auto';
SET parallel_hash_join_threshold = 100000;
SELECT countIf(explain LIKE '%FillingRightJoinSide%')
FROM (
    EXPLAIN PIPELINE
    SELECT t1.n FROM t05045_l AS t1 INNER JOIN t05045_r AS t2 ON t1.n = t2.n
);

SELECT 'join_switcher_parallel';
SET parallel_hash_join_threshold = 1;
SELECT countIf(explain LIKE '%FillingRightJoinSide%')
FROM (
    EXPLAIN PIPELINE
    SELECT t1.n FROM t05045_l AS t1 INNER JOIN t05045_r AS t2 ON t1.n = t2.n
);

DROP TABLE t05045_l;
DROP TABLE t05045_r;
