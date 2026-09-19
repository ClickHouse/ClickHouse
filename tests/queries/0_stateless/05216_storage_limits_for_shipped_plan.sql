-- A read inside a shipped plan fragment has to honour the byte limits, as it does when the query is
-- shipped as SQL instead. Every step's `deserialize` builds a fresh `SelectQueryInfo`, so a
-- deserialized plan carries no `StorageLimitsList` and the receiving node derives one from the
-- settings that arrived with the query.
--
-- The row-count limits are not a useful check here: `MergeTree` enforces those while selecting
-- parts, so they fire on both paths regardless.

DROP TABLE IF EXISTS t_shipped_limits SYNC;

CREATE TABLE t_shipped_limits (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_shipped_limits SELECT number FROM numbers(100000);

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 0;

SELECT 'the query-based implementation refuses';
SELECT sum(a) FROM t_shipped_limits
SETTINGS parallel_replicas_plan_based = 0, max_bytes_to_read_leaf = 1000; -- { serverError TOO_MANY_BYTES }

SELECT 'and so does the plan-based one';
SELECT sum(a) FROM t_shipped_limits
SETTINGS parallel_replicas_plan_based = 1, max_bytes_to_read_leaf = 1000; -- { serverError TOO_MANY_BYTES }

SELECT 'the total byte limit as well';
SELECT sum(a) FROM t_shipped_limits
SETTINGS parallel_replicas_plan_based = 1, max_bytes_to_read = 1000; -- { serverError TOO_MANY_BYTES }

SELECT 'a limit that is not exceeded does not fire';
SELECT sum(a) FROM t_shipped_limits
SETTINGS parallel_replicas_plan_based = 1, max_bytes_to_read_leaf = 100000000;

DROP TABLE t_shipped_limits SYNC;
