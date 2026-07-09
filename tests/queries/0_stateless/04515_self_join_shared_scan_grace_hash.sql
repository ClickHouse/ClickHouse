SET enable_analyzer = 1;
SET query_plan_optimize_self_join_shared_scan = 1;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET enable_shared_storage_snapshot_in_query = 1;
-- Pin the join order: a swapped self-join changes which side's columns must be a subset of the
-- other's, so whether the rewrite fires.
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;

DROP TABLE IF EXISTS t_sjss_gh;
CREATE TABLE t_sjss_gh (x UInt64, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sjss_gh SELECT number, toString(number) FROM numbers(10);

-- `grace_hash` uses the producer-first pipeline (build side fully consumed before the probe side
-- is read), so it is compatible with the shared-scan rewrite: the rewrite must fire and must not
-- strip a user-requested `grace_hash` from the algorithm list.

-- Correctness with grace_hash requested first.
SELECT a.x, b.y FROM t_sjss_gh AS a INNER JOIN t_sjss_gh AS b ON a.x = b.x ORDER BY a.x
SETTINGS join_algorithm = 'grace_hash,hash';

-- Same query without the rewrite, results must match.
SELECT a.x, b.y FROM t_sjss_gh AS a INNER JOIN t_sjss_gh AS b ON a.x = b.x ORDER BY a.x
SETTINGS join_algorithm = 'grace_hash,hash', query_plan_optimize_self_join_shared_scan = 0;

-- The rewrite must fire with grace_hash alone.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%SaveSubqueryResultToBuffer%') AS save_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_gh AS a INNER JOIN t_sjss_gh AS b ON a.x = b.x
    SETTINGS join_algorithm = 'grace_hash'
);

-- Correctness with grace_hash alone.
SELECT a.x, b.y FROM t_sjss_gh AS a INNER JOIN t_sjss_gh AS b ON a.x = b.x ORDER BY a.x
SETTINGS join_algorithm = 'grace_hash';

DROP TABLE t_sjss_gh;
