DROP TABLE IF EXISTS v_pr_view;
DROP TABLE IF EXISTS mt_pr_view;

CREATE TABLE mt_pr_view (a Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO mt_pr_view SELECT number FROM numbers(10);
CREATE VIEW v_pr_view AS SELECT a FROM mt_pr_view;

SET allow_experimental_parallel_reading_from_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'parallel_replicas';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_allow_view_over_mergetree = 1;
-- The assertions below look for `ReadFromRemoteParallelReplicas` and for the view's name inside that
-- step, i.e. for the SQL the initiator ships. Plan-based ships a serialized plan fragment through
-- `ReadFromParallelReplicas`, and it resolves the view before placing the split, so neither the step
-- name nor the relation it names can match. `parallel_replicas_allow_view_over_mergetree` is a
-- setting of the query-based decision as well - it is read in `findParallelReplicasQuery` and
-- `PlannerJoinTree`, which plan-based does not go through. See
-- `05407_pr_plan_based_view_shapes` for what plan-based does with the same views.
SET parallel_replicas_plan_based = 0;
-- Mode 2 only collects statistics and does not use parallel replicas, which would
-- make every assertion below pass without exercising the planner paths under test.
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 1;
SET enable_analyzer = 1;

-- Per-query values of parallel_replicas_allow_view_over_mergetree must be honored
-- consistently by the parallel-replicas decision and the plan build.
SELECT 1 FROM v_pr_view SETTINGS parallel_replicas_allow_view_over_mergetree = 0
EXCEPT DISTINCT
SELECT 1024 FROM v_pr_view SETTINGS parallel_replicas_allow_view_over_mergetree = 1;

SELECT 1 FROM v_pr_view SETTINGS parallel_replicas_allow_view_over_mergetree = 0
EXCEPT DISTINCT
SELECT materialize(-2147483648) FROM v_pr_view LIMIT 255 SETTINGS parallel_replicas_allow_view_over_mergetree = 0
EXCEPT DISTINCT
SELECT 1024 FROM v_pr_view LIMIT 1024 SETTINGS parallel_replicas_allow_view_over_mergetree = 1;

-- Parallel replicas must be used either way, so assert on what the setting actually
-- changes: which relation the remote step reads. Enabled sends the view, disabled sends
-- the underlying table. Result-only assertions cannot tell these apart.
SELECT count() > 0
FROM viewExplain('EXPLAIN', '', (
    SELECT count() FROM v_pr_view SETTINGS parallel_replicas_allow_view_over_mergetree = 1
))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%' AND explain LIKE '%v_pr_view%';

SELECT count()
FROM viewExplain('EXPLAIN', '', (
    SELECT count() FROM v_pr_view SETTINGS parallel_replicas_allow_view_over_mergetree = 0
))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%' AND explain LIKE '%v_pr_view%';

SELECT count() FROM v_pr_view SETTINGS parallel_replicas_allow_view_over_mergetree = 1;

DROP TABLE v_pr_view;
DROP TABLE mt_pr_view;
