-- Plan-based parallel replicas must keep lookup-backed joins local: a join whose right side is a
-- Join-engine table (or dictionary / key-value storage) becomes a JoinStepLogicalLookup, which cannot be
-- cloned/serialized into a shipped fragment. Distributing it used to throw "Cannot clone
-- JoinStepLogicalLookup plan step". Such joins now execute locally (correct results, not distributed),
-- while plain MergeTree-MergeTree joins still distribute. See PR #112268 review (comment r3665280903).

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

-- Lookup join (Join-engine table on the right): must not throw; result equals non-parallel execution.
SELECT 'JOIN-ENGINE LEFT', count(), sum(lkj_dim.v)
FROM lkj_fact LEFT JOIN lkj_dim ON lkj_fact.k = lkj_dim.k;
-- ... and it is kept local (no distributed read).
SELECT 'JOIN-ENGINE remote', countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN optimize = 1, description = 0
      SELECT count(), sum(lkj_dim.v) FROM lkj_fact LEFT JOIN lkj_dim ON lkj_fact.k = lkj_dim.k);

-- Regression: a plain MergeTree-MergeTree join still distributes (the gate did not over-reject).
SELECT 'MERGETREE LEFT', count(), sum(lkj_mt.v)
FROM lkj_fact LEFT JOIN lkj_mt ON lkj_fact.k = lkj_mt.k;
SELECT 'MERGETREE remote', countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN optimize = 1, description = 0
      SELECT count(), sum(lkj_mt.v) FROM lkj_fact LEFT JOIN lkj_mt ON lkj_fact.k = lkj_mt.k);

DROP TABLE lkj_fact SYNC;
DROP TABLE lkj_dim SYNC;
DROP TABLE lkj_mt SYNC;
