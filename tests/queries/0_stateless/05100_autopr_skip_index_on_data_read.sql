-- A skip index is applied while granules are read (`use_skip_indexes_on_data_read`, on by default)
-- rather than during index analysis. The reader that does that is only created when the read carries
-- the conditions built from the query, and the plan built for automatic parallel replicas never builds
-- them: it is built with `query_plan_optimize_primary_key` off, and a read that is handed an analysis
-- result never builds them later either. So the index was not applied at all and every granule the
-- primary key did not exclude was read.
--
-- The query condition cache hides this after the first execution - whichever run does apply the index
-- records the granules it discarded, and later runs prune up front from the cache. The cache is turned
-- off for this test rather than dropped, because dropping it is server-wide and would perturb whatever
-- else is running against the same server.

DROP TABLE IF EXISTS t_autopr_skip_index;

CREATE TABLE t_autopr_skip_index (key UInt64, np UInt64, INDEX ix_np np TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY key
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

-- `np` equals `key`, so the index on `np` is selective while the primary key on `key` cannot be used
-- for a filter written on `np`.
INSERT INTO t_autopr_skip_index SELECT number, number FROM numbers(400000);
OPTIMIZE TABLE t_autopr_skip_index FINAL;

SET enable_analyzer = 1;
SET use_skip_indexes_on_data_read = 1;
SET max_threads = 1;
SET merge_tree_min_bytes_per_task_for_remote_reading = 1024;
SET automatic_parallel_replicas_min_bytes_per_replica = 0;
SET use_query_condition_cache = 0;

SELECT sum(key) FROM t_autopr_skip_index WHERE np < 20000
FORMAT Null SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0,
    log_comment = '05100_single_node';

SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- The decision needs statistics from an earlier execution, so this run only collects them.
SELECT sum(key) FROM t_autopr_skip_index WHERE np < 20000
FORMAT Null SETTINGS log_comment = '05100_warmup';

SELECT sum(key) FROM t_autopr_skip_index WHERE np < 20000
FORMAT Null SETTINGS log_comment = '05100_with_replicas';

-- Again with `parallel_replicas_min_number_of_rows_per_replica` set, which makes the planner run index
-- analysis of its own before the filters are attached. The conditions the candidate ends up with have
-- to be the ones the single-node plan built, whatever was there before.
SELECT sum(key) FROM t_autopr_skip_index WHERE np < 20000
FORMAT Null SETTINGS parallel_replicas_min_number_of_rows_per_replica = 1000, log_comment = '05100_warmup';

SELECT sum(key) FROM t_autopr_skip_index WHERE np < 20000
FORMAT Null SETTINGS parallel_replicas_min_number_of_rows_per_replica = 1000,
    log_comment = '05100_with_replicas_min_rows';

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- The initiating query's `SelectedRows` already covers what the replicas read, so it is read on its
-- own: adding the replicas' own rows to it counts the same reads twice, which is only invisible when
-- the local replica happens to do all the work.
--
-- Reported per run. Pooling them would let the run that used replicas vouch for one that quietly
-- stayed single-node, and that run's branch would then go untested while the test still passed.
WITH
    (SELECT ProfileEvents['SelectedRows']
     FROM system.query_log
     WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
       AND event_date >= yesterday() AND log_comment = '05100_single_node') AS single_node_rows
SELECT
    log_comment AS run,
    max(ProfileEvents['ParallelReplicasUsedCount']) > 0 AS replicas_were_used,
    max(ProfileEvents['SelectedRows']) <= single_node_rows AS reads_no_more_than_a_single_node
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
  AND event_date >= yesterday()
  AND log_comment IN ('05100_with_replicas', '05100_with_replicas_min_rows')
GROUP BY run
ORDER BY run
FORMAT TSVWithNames;

DROP TABLE t_autopr_skip_index;
