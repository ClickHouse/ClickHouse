SET enable_analyzer = 1;
SET query_plan_optimize_self_join_shared_scan = 1;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET enable_shared_storage_snapshot_in_query = 1;
-- Pin the join order: a swapped self-join changes which side's columns must be a subset of the
-- other's, so whether the rewrite fires.
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;

DROP TABLE IF EXISTS t_sjss_auto;
CREATE TABLE t_sjss_auto (x UInt64, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sjss_auto SELECT number, toString(number) FROM numbers(10);

-- `auto` uses the producer-first pipeline (it resolves to `SpillingHashJoin`, `JoinSwitcher`, or
-- `HashJoin`), so it is compatible with the shared-scan rewrite: the rewrite must fire and must not
-- strip a user-requested `auto` from the algorithm list, or the configured under-memory-pressure
-- fallback (spill to disk or switch to merge join) would be silently replaced with an exception.

-- Correctness with auto requested first.
SELECT a.x, b.y FROM t_sjss_auto AS a INNER JOIN t_sjss_auto AS b ON a.x = b.x ORDER BY a.x
SETTINGS join_algorithm = 'auto,hash';

-- Same query without the rewrite, results must match.
SELECT a.x, b.y FROM t_sjss_auto AS a INNER JOIN t_sjss_auto AS b ON a.x = b.x ORDER BY a.x
SETTINGS join_algorithm = 'auto,hash', query_plan_optimize_self_join_shared_scan = 0;

-- The rewrite must fire with auto alone.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%SaveSubqueryResultToBuffer%') AS save_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_auto AS a INNER JOIN t_sjss_auto AS b ON a.x = b.x
    SETTINGS join_algorithm = 'auto'
);

-- Correctness with auto alone.
SELECT a.x, b.y FROM t_sjss_auto AS a INNER JOIN t_sjss_auto AS b ON a.x = b.x ORDER BY a.x
SETTINGS join_algorithm = 'auto';

DROP TABLE t_sjss_auto;
