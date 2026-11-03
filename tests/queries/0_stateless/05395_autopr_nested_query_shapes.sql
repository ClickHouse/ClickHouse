-- Automatic parallel replicas over a query nested in an outer aggregation, the way a result is hashed by
-- wrapping the query in `SELECT sum(cityHash64(tuple(*))) FROM (...)`. Two shapes used to be declined
-- even when the cost model wanted replicas:
--
-- * A `GROUP BY` subquery: the replicas plan names its table `__table1`, being built from the subquery
--   alone, while the single-node plan calls it `__table2`, so its reads were not paired with the
--   single-node ones to hand over their index analysis.
-- * A subquery with an `ORDER BY` the outer aggregation makes redundant: the single-node plan dropped the
--   sorting, the replicas kept it, and the two plans did not match.
--
-- The `ORDER BY` shape needs https://github.com/ClickHouse/ClickHouse/pull/125685 and is commented out
-- until it is merged.

DROP TABLE IF EXISTS t_nested;

-- The layout is pinned: the cost model works off the estimated bytes to read.
CREATE TABLE t_nested (key UInt64, kind UInt8, value UInt64) ENGINE = MergeTree ORDER BY key
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO t_nested SELECT number, number % 3, number * 7 FROM numbers(1000000);
OPTIMIZE TABLE t_nested FINAL;

SET enable_analyzer = 1;
SET use_statistics = 1;
SET use_statistics_cache = 1;
SET query_plan_remove_redundant_sorting = 1;
-- With a single reading thread the cost model sees replicas as a win on a table of this size.
SET max_threads = 1;
SET merge_tree_min_bytes_per_task_for_remote_reading = 1024;
SET automatic_parallel_replicas_min_bytes_per_replica = 0;

-- Results on a single node.
SELECT sum(cityHash64(tuple(*))) FROM (SELECT kind, count() FROM t_nested WHERE key < 900000 GROUP BY kind)
SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, log_comment = '05395_single_node_group_by';
-- Needs https://github.com/ClickHouse/ClickHouse/pull/125685
-- SELECT sum(cityHash64(tuple(*))) FROM (SELECT key, value FROM t_nested WHERE kind = 1 ORDER BY key)
-- SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, log_comment = '05395_single_node_order_by';

SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- The first run of each shape only collects statistics; the next ones can act on them.
SELECT sum(cityHash64(tuple(*))) FROM (SELECT kind, count() FROM t_nested WHERE key < 900000 GROUP BY kind)
SETTINGS log_comment = '05395_warmup_group_by';
SELECT sum(cityHash64(tuple(*))) FROM (SELECT kind, count() FROM t_nested WHERE key < 900000 GROUP BY kind)
SETTINGS log_comment = '05395_replicas_group_by';
SELECT sum(cityHash64(tuple(*))) FROM (SELECT kind, count() FROM t_nested WHERE key < 900000 GROUP BY kind)
SETTINGS log_comment = '05395_replicas_group_by';

-- Needs https://github.com/ClickHouse/ClickHouse/pull/125685
-- SELECT sum(cityHash64(tuple(*))) FROM (SELECT key, value FROM t_nested WHERE kind = 1 ORDER BY key)
-- SETTINGS log_comment = '05395_warmup_order_by';
-- SELECT sum(cityHash64(tuple(*))) FROM (SELECT key, value FROM t_nested WHERE kind = 1 ORDER BY key)
-- SETTINGS log_comment = '05395_replicas_order_by';
-- SELECT sum(cityHash64(tuple(*))) FROM (SELECT key, value FROM t_nested WHERE kind = 1 ORDER BY key)
-- SETTINGS log_comment = '05395_replicas_order_by';

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- Every run above prints its result, so the reference holds them equal to the single-node one. What it
-- cannot show is whether replicas were used; that is this, and it holds both runs after the warmup to it.
SELECT
    replaceOne(log_comment, '05395_replicas_', '') AS shape,
    count() = 2 AND min(ProfileEvents['ParallelReplicasUsedCount'] > 0) AS replicas_were_used_on_every_run
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
  AND event_date >= yesterday()
  -- Add '05395_replicas_order_by' with https://github.com/ClickHouse/ClickHouse/pull/125685
  AND log_comment IN ('05395_replicas_group_by')
GROUP BY shape
ORDER BY shape
FORMAT TSVWithNames;

DROP TABLE t_nested;
