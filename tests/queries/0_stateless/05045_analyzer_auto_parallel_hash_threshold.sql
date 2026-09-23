-- The hash phase of `join_algorithm = 'auto'` follows `parallel_hash_join_threshold` like a bare `hash` join.
--
-- Join-order stats stay on so the planner AUTO path gets MergeTree `totalRows` (200).
-- `missing_estimate_parallel` turns join-order off: no rhs estimate, high threshold still parallel.

SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET collect_hash_table_stats_during_joins = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET explain_query_plan_default = 'legacy';
SET max_threads = 16;
-- clickhouse-test ships randomized `--max_threads` with every query, which overrides
-- session SET. Pin the knobs that decide parallel vs serial fill on the EXPLAIN itself.

DROP TABLE IF EXISTS t05045_l;
DROP TABLE IF EXISTS t05045_r;
CREATE TABLE t05045_l (n UInt64) ENGINE = MergeTree ORDER BY n;
CREATE TABLE t05045_r (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t05045_l SELECT number FROM numbers(100);
INSERT INTO t05045_r SELECT number FROM numbers(200);

SELECT 'join_switcher_serial';
SET join_algorithm = 'auto';
SET parallel_hash_join_threshold = 100000;
SELECT countIf(explain LIKE '%FillingRightJoinSide%')
FROM (
    EXPLAIN PIPELINE
    SELECT t1.n FROM t05045_l AS t1 INNER JOIN t05045_r AS t2 ON t1.n = t2.n
    SETTINGS max_threads = 16, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0, join_algorithm = 'auto', parallel_hash_join_threshold = 100000
);

SELECT 'join_switcher_parallel';
SET parallel_hash_join_threshold = 1;
SELECT countIf(explain LIKE '%FillingRightJoinSide%')
FROM (
    EXPLAIN PIPELINE
    SELECT t1.n FROM t05045_l AS t1 INNER JOIN t05045_r AS t2 ON t1.n = t2.n
    SETTINGS max_threads = 16, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0, join_algorithm = 'auto', parallel_hash_join_threshold = 1
);

SELECT 'missing_estimate_parallel';
SET join_algorithm = 'hash';
SET parallel_hash_join_threshold = 100000;
SELECT countIf(explain LIKE '%FillingRightJoinSide%')
FROM (
    EXPLAIN PIPELINE
    SELECT t1.n FROM t05045_l AS t1 INNER JOIN t05045_r AS t2 ON t1.n = t2.n
    SETTINGS max_threads = 16, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 0, query_plan_optimize_join_order_randomize = 0, join_algorithm = 'hash', parallel_hash_join_threshold = 100000
);

DROP TABLE t05045_l;
DROP TABLE t05045_r;
