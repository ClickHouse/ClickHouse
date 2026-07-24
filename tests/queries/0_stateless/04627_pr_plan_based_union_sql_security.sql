-- Plan-based parallel replicas merges a UNION ALL over MergeTree branches into a single distributed
-- plan fragment that executes under one context. Result-affecting transforms (here a per-branch SQL-security
-- row policy) are baked as plan steps during each branch's own analysis before the split is inserted, so
-- the shared execution context must not change the rows returned. Checked with parallel_replicas_local_plan
-- both off (remote plan fragment shipped/deserialized by name) and on (local plan fragment). Regression test for PR
-- #111063 review.

DROP VIEW IF EXISTS dv1_04627 SYNC;
DROP VIEW IF EXISTS iv2_04627 SYNC;
DROP TABLE IF EXISTS ub1_04627 SYNC;
DROP TABLE IF EXISTS ub2_04627 SYNC;
DROP ROW POLICY IF EXISTS rp_04627 ON ub2_04627;
DROP USER IF EXISTS user_04627;

CREATE USER user_04627;
GRANT SELECT ON *.* TO user_04627;

CREATE TABLE ub1_04627 (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE ub2_04627 (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO ub1_04627 SELECT number FROM numbers(1_000_000);
INSERT INTO ub2_04627 SELECT number + 1_000_000 FROM numbers(1_000_000);

-- Restrict ub2 to the lower half ONLY for user_04627; the caller/invoker has no such policy.
CREATE ROW POLICY rp_04627 ON ub2_04627 FOR SELECT USING a < 1_500_000 TO user_04627;

-- dv1: DEFINER = user_04627 over ub1 (its inner read carries the definer's context).
CREATE VIEW dv1_04627 DEFINER = user_04627 SQL SECURITY DEFINER AS SELECT a FROM ub1_04627;
-- iv2: INVOKER over ub2 (its inner read carries the caller's context -> the policy does NOT apply).
CREATE VIEW iv2_04627 SQL SECURITY INVOKER AS SELECT a FROM ub2_04627;

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;

-- The invoker branch must return all of ub2; if the merged plan fragment applied the definer's row policy to
-- it, the upper half would be missing (smaller count / sum). Checked for both plan fragment execution paths:
-- local_plan = 0 ships every read as a remote plan fragment; local_plan = 1 runs a local plan fragment on
-- the initiator, with the failpoint slowing its read so the remote replicas emit rows first (deterministically
-- exercising coordinated remote reading rather than the fast local read winning the race).
SELECT count(), sum(a) FROM (SELECT a FROM dv1_04627 UNION ALL SELECT a FROM iv2_04627)
SETTINGS parallel_replicas_local_plan = 0;

SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;
SELECT count(), sum(a) FROM (SELECT a FROM dv1_04627 UNION ALL SELECT a FROM iv2_04627)
SETTINGS parallel_replicas_local_plan = 1;
SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

-- The union ships as one distributed plan fragment (single shared context), no leftover split.
SELECT
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read,
    countIf(explain LIKE '%ParallelReplicasSplit%') AS splits
FROM (EXPLAIN optimize = 1, description = 0
    SELECT a FROM (SELECT a FROM dv1_04627 UNION ALL SELECT a FROM iv2_04627));

DROP VIEW dv1_04627 SYNC;
DROP VIEW iv2_04627 SYNC;
DROP ROW POLICY rp_04627 ON ub2_04627;
DROP TABLE ub1_04627 SYNC;
DROP TABLE ub2_04627 SYNC;
DROP USER user_04627;
