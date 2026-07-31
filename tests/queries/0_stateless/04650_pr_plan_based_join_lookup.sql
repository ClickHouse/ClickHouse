-- Plan-based parallel replicas must keep lookup-backed joins local: a join whose right side is a
-- Join-engine table (or dictionary / key-value storage) becomes a JoinStepLogicalLookup, which cannot be
-- cloned/serialized into a shipped fragment. Distributing it used to throw "Cannot clone
-- JoinStepLogicalLookup plan step". Only the join is kept local: the coordinated read below it is still
-- distributed. A plain MergeTree-MergeTree join ships as one fragment instead.
-- The assertions print the plan steps rather than just "is there a distributed read", so that all three
-- outcomes are distinguishable: join shipped, join local with a distributed read, fully local.
-- See PR #112268 review (comments r3665280903, r3678401956).

DROP TABLE IF EXISTS lkj_fact SYNC;
DROP TABLE IF EXISTS lkj_dim SYNC;
DROP TABLE IF EXISTS lkj_mt SYNC;

CREATE TABLE lkj_fact (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE lkj_dim  (k UInt64, v UInt64) ENGINE = Join(ALL, LEFT, k);
CREATE TABLE lkj_mt   (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO lkj_fact SELECT number, number * 10 FROM numbers(1000);   -- k 0..999
INSERT INTO lkj_dim  SELECT number, number * 100 FROM numbers(500);   -- k 0..499
INSERT INTO lkj_mt   SELECT number, number * 100 FROM numbers(500);   -- k 0..499

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;
-- Pin the plan shape: the local plan must be present to hold the join step, and a randomized join order
-- can put a `BuildRuntimeFilter` between the join and the coordinated read, blocking the lift by itself.
SET parallel_replicas_local_plan = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';

-- Lookup join (Join-engine table on the right): must not throw; result equals non-parallel execution.
SELECT 'JOIN-ENGINE result', count(), sum(lkj_dim.v)
FROM lkj_fact LEFT JOIN lkj_dim ON lkj_fact.k = lkj_dim.k;
-- The join itself stays local - it is above the distributed read, not inside the shipped fragment.
SELECT 'JOIN-ENGINE steps:', arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count(), sum(lkj_dim.v) FROM lkj_fact LEFT JOIN lkj_dim ON lkj_fact.k = lkj_dim.k)
    WHERE step IN ('Aggregating', 'Union', 'Join', 'FilledJoin', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);

-- Regression: a plain MergeTree-MergeTree join is shipped whole, so the distributed read comes last.
SELECT 'MERGETREE result', count(), sum(lkj_mt.v)
FROM lkj_fact LEFT JOIN lkj_mt ON lkj_fact.k = lkj_mt.k;
SELECT 'MERGETREE steps:', arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count(), sum(lkj_mt.v) FROM lkj_fact LEFT JOIN lkj_mt ON lkj_fact.k = lkj_mt.k)
    WHERE step IN ('Aggregating', 'Union', 'Join', 'FilledJoin', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);

DROP TABLE lkj_fact SYNC;
DROP TABLE lkj_dim SYNC;
DROP TABLE lkj_mt SYNC;
