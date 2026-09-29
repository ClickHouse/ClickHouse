-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- The granules a filter matched are recorded by tagging the filter step, which
-- `updateQueryConditionCache` does early in plan optimization and only for a read that already has its
-- filter actions. The plan automatic parallel replicas builds has none at that point - it is built with
-- `query_plan_optimize_primary_key` off, so `applyFilters` never runs on its reads, and they are handed
-- their analysis only once the decision is made. So nothing was tagged, a query the optimization
-- rewrote wrote nothing to the query condition cache, and every later query over the same predicate
-- read the table again.
--
-- `add_minmax_index_for_numeric_columns = 0` keeps the implicit index out of it, so the filter on a
-- column outside the primary key is pruned by the cache or not at all.

DROP TABLE IF EXISTS t_autopr_qcc;

CREATE TABLE t_autopr_qcc (a Int64, b Int64, pad String) ENGINE = MergeTree ORDER BY a
    SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 8192,
             min_bytes_for_wide_part = 0;

INSERT INTO t_autopr_qcc SELECT number, number, repeat('x', 40) FROM numbers(1000000);

-- One part, so that the plan the decision is matched against does not depend on how the insert was
-- split; otherwise the optimization declines on some runs and the test measures nothing.
OPTIMIZE TABLE t_autopr_qcc FINAL;

SET enable_analyzer = 1;
SET use_query_condition_cache = 1;
SET optimize_move_to_prewhere = 0;
-- With one reading thread the cost model sees replicas as a win on a table of this size, which is what
-- makes the optimization actually rewrite the query rather than decline.
SET max_threads = 1;
SET automatic_parallel_replicas_min_bytes_per_replica = 0;
SET merge_tree_min_bytes_per_task_for_remote_reading = 1024;

SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

SYSTEM DROP QUERY CONDITION CACHE;

-- The decision needs statistics from an earlier execution, so this run only collects them. It runs as a
-- single node and would populate the cache itself, which would then hide whether the rewritten run
-- below populates anything - so the cache is dropped again after it.
SELECT count() FROM t_autopr_qcc WHERE b = 10000 FORMAT Null SETTINGS log_comment = '05218_warmup';

SYSTEM DROP QUERY CONDITION CACHE;

-- This one is rewritten to use parallel replicas, and has to leave the granules it discarded behind.
SELECT count() FROM t_autopr_qcc WHERE b = 10000 FORMAT Null SETTINGS log_comment = '05218_rewritten';

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

-- Reads the same predicate on a single node: it must find what the rewritten query left.
SELECT count() FROM t_autopr_qcc WHERE b = 10000 FORMAT Null SETTINGS log_comment = '05218_reader';

SYSTEM FLUSH LOGS query_log;

-- `the_query_was_rewritten` keeps the check honest: if the optimization declined, the rewritten run was
-- an ordinary single-node query and populating the cache would say nothing.
SELECT
    (SELECT ProfileEvents['ParallelReplicasUsedCount'] > 0
     FROM system.query_log
     WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
       AND event_date >= yesterday() AND log_comment = '05218_rewritten') AS the_query_was_rewritten,
    (SELECT ProfileEvents['QueryConditionCacheHits'] > 0
     FROM system.query_log
     WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
       AND event_date >= yesterday() AND log_comment = '05218_reader') AS the_next_query_found_it
FORMAT TSVWithNames;

DROP TABLE t_autopr_qcc;
