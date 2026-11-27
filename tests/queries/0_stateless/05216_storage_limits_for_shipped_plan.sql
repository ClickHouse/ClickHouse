-- A read inside a shipped plan fragment has to honour the byte limits, as it does when the query is
-- shipped as SQL instead. Every step's `deserialize` builds a fresh `SelectQueryInfo`, so a
-- deserialized plan carries no `StorageLimitsList` and the receiving node derives one from the
-- settings that arrived with the query.
--
-- The row-count limits are not a useful check here: `MergeTree` enforces those while selecting
-- parts, so they fire on both paths regardless. The byte limits count uncompressed bytes, so the
-- 800000 bytes of the column are far above 1000 whatever the compression.

DROP TABLE IF EXISTS t_shipped_limits SYNC;
DROP TABLE IF EXISTS t_shipped_limits_small SYNC;

CREATE TABLE t_shipped_limits (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_shipped_limits SELECT number FROM numbers(100000);

CREATE TABLE t_shipped_limits_small (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_shipped_limits_small VALUES (1), (2), (3);

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

SELECT 'a distributed plan refuses too';
SELECT sum(a) FROM t_shipped_limits
SETTINGS enable_parallel_replicas = 0, make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_default_reader_bucket_count = 3, max_rows_to_group_by = 0,
    max_bytes_to_read = 1000; -- { serverError TOO_MANY_BYTES }

SELECT sum(a) FROM t_shipped_limits
SETTINGS enable_parallel_replicas = 0, make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_default_reader_bucket_count = 3, max_rows_to_group_by = 0,
    max_bytes_to_read_leaf = 1000; -- { serverError TOO_MANY_BYTES }

SELECT sum(a) FROM t_shipped_limits
SETTINGS enable_parallel_replicas = 0, make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_default_reader_bucket_count = 3, max_rows_to_group_by = 0,
    max_bytes_to_read_leaf = 100000000;

SELECT 'the read of an IN subquery in a shipped plan is limited as well';
SELECT count() FROM t_shipped_limits_small WHERE a IN (SELECT a FROM t_shipped_limits)
SETTINGS parallel_replicas_plan_based = 1, use_index_for_in_with_subqueries = 1,
    max_bytes_to_read_leaf = 1000; -- { serverError TOO_MANY_BYTES }

SELECT count() FROM t_shipped_limits_small WHERE a IN (SELECT a FROM t_shipped_limits)
SETTINGS parallel_replicas_plan_based = 1, use_index_for_in_with_subqueries = 1,
    max_bytes_to_read_leaf = 100000000;

DROP TABLE t_shipped_limits SYNC;
DROP TABLE t_shipped_limits_small SYNC;
