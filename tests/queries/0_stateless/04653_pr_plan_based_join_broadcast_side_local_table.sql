-- Plan-based parallel replicas must not ship a JOIN whose broadcast side reads a table that is not safe
-- to read on every replica. The broadcast side is executed in full by each replica, so a non-replicated
-- MergeTree (which may hold different data per replica) is only allowed with
-- `parallel_replicas_for_non_replicated_merge_tree`. `collectReadsToDistribute` follows the coordinated
-- side only, so the check has to happen when the split is lifted above the join.
-- See PR #112268 review (comment r3675009364).

DROP TABLE IF EXISTS rmt_fact SYNC;
DROP TABLE IF EXISTS mt_dim SYNC;

CREATE TABLE rmt_fact (k UInt64, x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/rmt_fact', 'r1') ORDER BY k;
CREATE TABLE mt_dim  (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO rmt_fact SELECT number, number FROM numbers(1000);
INSERT INTO mt_dim  SELECT number, number * 2 FROM numbers(1000);   -- sum(v) = 999000

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;
-- Pin the plan shape: the local plan must be present to hold the `Join` step, and a randomized join order
-- can put a `BuildRuntimeFilter` between the join and the coordinated read, blocking the lift by itself.
SET parallel_replicas_local_plan = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';

-- Without the opt-in the join stays local: the distributed read appears below it, next to the local read
-- of `mt_dim`. With the opt-in the whole join ships, so the distributed read comes last, as a sibling of
-- the local plan which holds the join.
SELECT 'parallel_replicas_for_non_replicated_merge_tree=0 steps:', arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count() FROM rmt_fact LEFT JOIN mt_dim ON rmt_fact.k = mt_dim.k)
    WHERE step IN ('Aggregating', 'Union', 'Join', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
) SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0;

SELECT count(), sum(mt_dim.v)
FROM rmt_fact LEFT JOIN mt_dim ON rmt_fact.k = mt_dim.k
SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0;

SELECT 'parallel_replicas_for_non_replicated_merge_tree=1 steps:', arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count() FROM rmt_fact LEFT JOIN mt_dim ON rmt_fact.k = mt_dim.k)
    WHERE step IN ('Aggregating', 'Union', 'Join', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
) SETTINGS parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT count(), sum(mt_dim.v)
FROM rmt_fact LEFT JOIN mt_dim ON rmt_fact.k = mt_dim.k
SETTINGS parallel_replicas_for_non_replicated_merge_tree = 1;

DROP TABLE rmt_fact SYNC;
DROP TABLE mt_dim SYNC;
