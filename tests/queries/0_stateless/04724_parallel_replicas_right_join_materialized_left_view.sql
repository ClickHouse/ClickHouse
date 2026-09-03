DROP TABLE IF EXISTS t_replicated_right;
DROP VIEW IF EXISTS v_left;

-- The arms below run at the default parallel_replicas_for_non_replicated_merge_tree = 0,
-- where a plain MergeTree is not eligible, so the right side has to be replicated to keep
-- the JOIN offloaded at all.
CREATE TABLE t_replicated_right (key UInt64)
ENGINE = ReplicatedMergeTree('/parallel_replicas/{database}/t_replicated_right', 'r1') ORDER BY key;
INSERT INTO t_replicated_right SELECT number FROM numbers(10);

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1,
    max_parallel_replicas = 2,
    parallel_replicas_local_plan = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_prefer_local_join = 0;

-- A plain View is resolved by a separate branch, gated by its own setting, so it needs its own
-- arms. Here the third arm is the setting's default, which is what makes the first two meaningful.

CREATE VIEW v_left AS SELECT key FROM t_replicated_right;

SELECT '-- view left, right eligible: the JOIN is still offloaded';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT key FROM v_left) AS l
    RIGHT JOIN (SELECT key FROM t_replicated_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0,
             parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0,
             parallel_replicas_allow_view_over_mergetree = 1
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%' AND explain ILIKE '%RIGHT JOIN%';

SELECT '-- view left, right eligible: the local join is kept';
SELECT count() FROM (
    EXPLAIN SELECT * FROM (SELECT key FROM v_left) AS l
    RIGHT JOIN (SELECT key FROM t_replicated_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0,
             parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0,
             parallel_replicas_allow_view_over_mergetree = 1
) WHERE explain ILIKE '%GLOBAL ALL RIGHT JOIN%';

SELECT '-- view left with views over mergetree disallowed: left is materialized';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT key FROM v_left) AS l
    RIGHT JOIN (SELECT key FROM t_replicated_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0,
             parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0,
             parallel_replicas_allow_view_over_mergetree = 0
) WHERE explain ILIKE '%GLOBAL ALL RIGHT JOIN%' AND explain ILIKE '%_data_%';

SELECT '-- view left, right eligible: results are correct';
SELECT r.key, ifNull(l.key = r.key, 0) AS matched FROM (SELECT key FROM v_left WHERE key < 5) AS l
RIGHT JOIN (SELECT key FROM t_replicated_right) AS r ON l.key = r.key
ORDER BY r.key
SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0, parallel_replicas_prefer_local_join = 1,
         parallel_replicas_allow_view_over_mergetree = 1;

SELECT '-- view left, right eligible: the wrapper contributes matched rows';
SELECT countIf(ifNull(l.key = r.key, 0) AND r.key > 0) FROM (SELECT key FROM v_left WHERE key < 5) AS l
RIGHT JOIN (SELECT key FROM t_replicated_right) AS r ON l.key = r.key
SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0, parallel_replicas_prefer_local_join = 1,
         parallel_replicas_allow_view_over_mergetree = 1;

-- The same View with an outer FINAL is not eligible, so the JOIN is not offloaded at all and only
-- the right subquery is read with replicas. Unwrapping the View re-applies the eligibility check to
-- the outer table node, which is what rejects the FINAL there as it is rejected for a bare table and
-- for a MaterializedView; a whole-query FINAL check in the planner reaches the same verdict first, so
-- these arms pin the behaviour rather than a single decision point. FINAL on a plain View is dropped
-- by StorageView::readImpl anyway, hence the same rows as the arms above.

SELECT '-- view left with FINAL: the JOIN is not offloaded';
SELECT count() FROM (
    EXPLAIN SELECT * FROM (SELECT key FROM v_left FINAL) AS l
    RIGHT JOIN (SELECT key FROM t_replicated_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0,
             parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0,
             parallel_replicas_allow_view_over_mergetree = 1
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%' AND explain ILIKE '%RIGHT JOIN%';

SELECT '-- view left with FINAL: the right side is still read with replicas';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT key FROM v_left FINAL) AS l
    RIGHT JOIN (SELECT key FROM t_replicated_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0,
             parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0,
             parallel_replicas_allow_view_over_mergetree = 1
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';

SELECT '-- view left with FINAL: results are correct';
SELECT r.key, ifNull(l.key = r.key, 0) AS matched FROM (SELECT key FROM v_left FINAL WHERE key < 5) AS l
RIGHT JOIN (SELECT key FROM t_replicated_right) AS r ON l.key = r.key
ORDER BY r.key
SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0, parallel_replicas_prefer_local_join = 1,
         parallel_replicas_allow_view_over_mergetree = 1;

SELECT '-- view left with FINAL: the wrapper contributes matched rows';
SELECT countIf(ifNull(l.key = r.key, 0) AND r.key > 0) FROM (SELECT key FROM v_left FINAL WHERE key < 5) AS l
RIGHT JOIN (SELECT key FROM t_replicated_right) AS r ON l.key = r.key
SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0, parallel_replicas_prefer_local_join = 1,
         parallel_replicas_allow_view_over_mergetree = 1;

DROP VIEW v_left;
DROP TABLE t_replicated_right SYNC;
