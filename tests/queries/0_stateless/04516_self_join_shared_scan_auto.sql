SET enable_analyzer = 1; -- the rewrite requires the analyzer
SET query_plan_optimize_self_join_shared_scan = 1; -- the setting under test
SET enable_join_runtime_filters = 0; -- a runtime filter makes the scan non-plain and blocks the rewrite
SET enable_parallel_replicas = 0; -- reading with parallel replicas blocks the rewrite
SET enable_shared_storage_snapshot_in_query = 1; -- the rewrite requires both scans to share one storage snapshot
SET query_plan_join_swap_table = 0; -- a swap changes which side's columns must be a subset of the other's
SET query_plan_optimize_join_order_randomize = 0; -- join order randomization may swap the sides

DROP TABLE IF EXISTS t_sjss_auto;
CREATE TABLE t_sjss_auto (x UInt64, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sjss_auto SELECT number, toString(number) FROM numbers(10);

-- `auto` is an external-memory algorithm: it resolves to `SpillingHashJoin` or `JoinSwitcher`,
-- which spill to disk or switch to an on-disk merge join under memory pressure. The rewrite would
-- keep the whole build-side scan in an in-memory buffer outside the join's spill accounting,
-- breaking that contract, so the rewrite must NOT fire (2 scans, no buffer).

-- Correctness with auto requested first.
SELECT a.x, b.y FROM t_sjss_auto AS a INNER JOIN t_sjss_auto AS b ON a.x = b.x ORDER BY a.x
SETTINGS join_algorithm = 'auto,hash';

-- Same query with the optimization setting off, results must match.
SELECT a.x, b.y FROM t_sjss_auto AS a INNER JOIN t_sjss_auto AS b ON a.x = b.x ORDER BY a.x
SETTINGS join_algorithm = 'auto,hash', query_plan_optimize_self_join_shared_scan = 0;

-- The rewrite must NOT fire with auto alone.
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
