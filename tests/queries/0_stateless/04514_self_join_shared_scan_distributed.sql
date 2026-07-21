-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan and the optimization require the analyzer.

-- The self-join shared-scan rewrite introduces steps that do not support plan serialization
-- (`CommonSubplanStep` / `CommonSubplanReferenceStep` and the buffer steps they are lowered to),
-- so it must not fire under `make_distributed_plan`: previously a qualifying self-join failed
-- with an exception from `assertFragmentSerializable` instead of skipping the optimization.

SET enable_analyzer = 1; -- the rewrite requires the analyzer
SET query_plan_optimize_self_join_shared_scan = 1; -- the setting under test
SET enable_join_runtime_filters = 0; -- a runtime filter makes the scan non-plain and blocks the rewrite
SET enable_parallel_replicas = 0; -- reading with parallel replicas blocks the rewrite
SET enable_shared_storage_snapshot_in_query = 1; -- the rewrite requires both scans to share one storage snapshot
SET max_rows_to_group_by = 0; -- the CI config sets a limit, which make_distributed_plan rejects

DROP TABLE IF EXISTS t_sjss_dist;
CREATE TABLE t_sjss_dist (x UInt64, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sjss_dist SELECT number, toString(number) FROM numbers(10);

-- Must not fail and must return correct results.
SELECT a.x, b.y FROM t_sjss_dist AS a INNER JOIN t_sjss_dist AS b ON a.x = b.x ORDER BY a.x
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;

-- Plan shape: the rewrite must not fire, so no shared buffer appears in the distributed plan.
-- `make_distributed_plan` is set only on the inner query: the outer wrapper reads from the
-- EXPLAIN storage, which itself cannot be distributed.
SELECT countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_dist AS a INNER JOIN t_sjss_dist AS b ON a.x = b.x
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1
);

DROP TABLE t_sjss_dist;
