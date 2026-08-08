-- Automatic parallel replicas transplants the single-node plan's index analysis onto the
-- coordinated local read of the adopted replicas plan (`considerEnablingParallelReplicas`).
-- When `query_plan_join_shard_by_pk_ranges` had sharded the single-node plan
-- (`optimizeJoinByShards` attaches `split_parts` to that shared analysis), the coordinated read
-- used to read by primary-key layers, creating one reading pool per layer. Every pool announced
-- its ranges to the coordinator under the same stream, which rejects the second announcement from
-- the same replica: "Duplicate announcement received for replica number N" (a `LOGICAL_ERROR`
-- exception; an abort in debug builds). The second run of the query below (statistics collected by
-- the first run, so the replicas plan is adopted) used to hit it.

-- For runs with the old analyzer
SET enable_analyzer=1;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';
SET parallel_replicas_prefer_local_join=1;

-- Keep the parallelized side oriented as written (the randomizer may flip this).
SET query_plan_join_swap_table='false';

SET automatic_parallel_replicas_min_bytes_per_replica=0;
SET merge_tree_min_bytes_per_task_for_remote_reading=0;
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;
-- Several streams, so that the sharding produces more than one layer.
SET max_threads=4, max_block_size=128;
SET use_query_condition_cache=0;

-- The sharding under test.
SET join_algorithm='full_sorting_merge', query_plan_join_shard_by_pk_ranges=1;

-- With materialized statistics the planner estimates the join from them instead of running index
-- analysis on the replicas-plan read, leaving that read un-analyzed — the precondition for the
-- analysis transplant that used to leak `split_parts` into the coordinated read.
SET materialize_statistics_on_insert=1;

DROP TABLE IF EXISTS jspr_left;
DROP TABLE IF EXISTS jspr_right;

CREATE TABLE jspr_left (key UInt64, payload String) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity=128;
CREATE TABLE jspr_right (key UInt64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity=128;

-- A merge would change the read hash and force a statistics recollection on the second run.
SYSTEM STOP MERGES jspr_left;
SYSTEM STOP MERGES jspr_right;

-- Several overlapping parts, so that the sharding finds intersecting ranges to split into layers.
INSERT INTO jspr_left SELECT number, toString(cityHash64(number)) FROM numbers(25000) WHERE number % 4 = 0;
INSERT INTO jspr_left SELECT number, toString(cityHash64(number)) FROM numbers(25000) WHERE number % 4 = 1;
INSERT INTO jspr_left SELECT number, toString(cityHash64(number)) FROM numbers(25000) WHERE number % 4 = 2;
INSERT INTO jspr_left SELECT number, toString(cityHash64(number)) FROM numbers(25000) WHERE number % 4 = 3;
INSERT INTO jspr_right SELECT number * 2 FROM numbers(12500);

-- First run: statistics cache miss, dataflow statistics are collected on the single-node (sharded) plan.
SELECT sum(length(t1.payload)) FROM jspr_left AS t1 INNER JOIN jspr_right AS t2 USING (key)
SETTINGS log_comment='04812_autopr_join_shard_by_pk_ranges_run_1';
-- Second run: statistics available, the parallel replicas plan is adopted, reusing the sharded analysis.
SELECT sum(length(t1.payload)) FROM jspr_left AS t1 INNER JOIN jspr_right AS t2 USING (key)
SETTINGS log_comment='04812_autopr_join_shard_by_pk_ranges_run_2';

-- The regression is specific to a narrow plan shape, so pin its preconditions instead of only
-- comparing the sums (which stay correct even when the buggy path is not exercised).
-- The single-node plan must still shard this join by primary-key ranges.
SELECT count() > 0 AS single_node_plan_is_sharded
FROM (EXPLAIN actions = 1 SELECT sum(length(t1.payload)) FROM jspr_left AS t1 INNER JOIN jspr_right AS t2 USING (key))
WHERE explain LIKE '%Sharding:%'
SETTINGS enable_parallel_replicas=0, explain_query_plan_default='legacy';

DROP TABLE jspr_left;
DROP TABLE jspr_right;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- The first run must have stayed on the single-node plan and collected the dataflow statistics...
SELECT
    ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 AS stats_collected,
    ProfileEvents['ParallelReplicasUsedCount'] > 0 AS parallel_replicas_used
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase()
    AND log_comment = '04812_autopr_join_shard_by_pk_ranges_run_1' AND type = 'QueryFinish' AND is_initial_query
ORDER BY event_time_microseconds DESC
LIMIT 1
FORMAT TSVWithNames;

-- ...and the second run must have really adopted the parallel replicas plan.
SELECT
    ProfileEvents['ParallelReplicasUsedCount'] > 0 AS parallel_replicas_used
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase()
    AND log_comment = '04812_autopr_join_shard_by_pk_ranges_run_2' AND type = 'QueryFinish' AND is_initial_query
ORDER BY event_time_microseconds DESC
LIMIT 1
FORMAT TSVWithNames;
