-- `ReadFromMergeTree::clone` must carry `join_runtime_filters_for_index_analysis` over to the clone.
-- Plan-based parallel replicas clones the plan fragment (`cloneSubtree` in `applyParallelReplicas`, plus a
-- second `QueryPlan::clone` for the initiator's local fragment) AFTER `tryAddJoinRuntimeFilter` registered the
-- descriptors, and the local fragment is then re-optimized with `enable_join_runtime_filters = false` because
-- the filters are expected to be in the clone already. Losing the descriptors there drops runtime-filter
-- granule pruning on the fragment's non-coordinated (broadcast) reads, and hides that pruning from
-- `mayPruneRangesOnDataRead`, which is what exempts such a read from the `read_in_order_max_primary_key_ratio`
-- guard: the guard would otherwise judge a mark count that is only a pre-pruning upper bound.
--
-- A `RIGHT JOIN` makes the build side the coordinated one, so the probe side - the side that carries the
-- runtime filter - is the broadcast read inside the cloned fragment, which is exactly the read whose
-- descriptors the clone used to drop. The remote replicas do not prune here (the descriptors are
-- deliberately not serialized, see `ReadFromMergeTree::serialize`), so the granule counters below can only
-- come from the initiator's local fragment.

DROP TABLE IF EXISTS rf_clone_probe SYNC;
DROP TABLE IF EXISTS rf_clone_build SYNC;

CREATE TABLE rf_clone_probe (a UInt64, s String) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1024;
CREATE TABLE rf_clone_build (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1024;
INSERT INTO rf_clone_probe SELECT number, toString(number) FROM numbers(300000);
-- Only the first 1000 of the 300000 probe keys match, so a correct filter prunes almost every granule.
INSERT INTO rf_clone_build SELECT number FROM numbers(1000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_prefer_local_replica = 1;
SET join_runtime_filter_min_probe_rows = 1;
-- Pin the join order: a randomized order swaps the sides, and a filter is only built for a join that may
-- drop probe rows, so without this the filter is sometimes not created at all.
SET query_plan_join_swap_table = 'false';
SET query_plan_optimize_join_order_randomize = 0;
SET join_runtime_filter_blocks_to_skip_before_reenabling = 0;
SET enable_join_runtime_filters = 1;

SELECT 'result', count() FROM rf_clone_probe RIGHT JOIN rf_clone_build ON rf_clone_probe.a = rf_clone_build.a
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes_on_data_read = 1, log_comment = '04891_pruning';

-- Control: the same query without index analysis prunes no granule, so the counters below cannot come from
-- anything but the runtime-filter reader.
SELECT 'result', count() FROM rf_clone_probe RIGHT JOIN rf_clone_build ON rf_clone_probe.a = rf_clone_build.a
SETTINGS enable_join_runtime_filters_index_analysis = 0, use_skip_indexes_on_data_read = 1, log_comment = '04891_no_pruning';

SYSTEM FLUSH LOGS query_log;

-- Read the log with plain local reads: `system.query_log` is a MergeTree table too, so the settings above
-- would ship these queries to the replicas as well.
SET enable_parallel_replicas = 0;

SELECT 'local fragment prunes granules',
       ProfileEvents['RuntimeFilterGranulesConsidered'] > 0,
       ProfileEvents['RuntimeFilterGranulesDropped'] > 0
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query = 1 AND current_database = currentDatabase()
  AND Settings['log_comment'] = '04891_pruning'
  AND event_date >= yesterday() AND event_time > now() - INTERVAL 1 HOUR
ORDER BY event_time DESC LIMIT 1;

SELECT 'no pruning without index analysis',
       ProfileEvents['RuntimeFilterGranulesConsidered'] = 0,
       ProfileEvents['RuntimeFilterGranulesDropped'] = 0
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query = 1 AND current_database = currentDatabase()
  AND Settings['log_comment'] = '04891_no_pruning'
  AND event_date >= yesterday() AND event_time > now() - INTERVAL 1 HOUR
ORDER BY event_time DESC LIMIT 1;

DROP TABLE rf_clone_probe SYNC;
DROP TABLE rf_clone_build SYNC;
