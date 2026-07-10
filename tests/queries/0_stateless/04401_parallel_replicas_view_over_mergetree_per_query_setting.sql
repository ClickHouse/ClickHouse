-- Tags: no-parallel-replicas

DROP TABLE IF EXISTS v_pr_view;
DROP TABLE IF EXISTS mt_pr_view;

CREATE TABLE mt_pr_view (a Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO mt_pr_view SELECT number FROM numbers(1000);
CREATE VIEW v_pr_view AS SELECT a FROM mt_pr_view;

SET allow_experimental_parallel_reading_from_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'parallel_replicas';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_allow_view_over_mergetree = 1;

-- A UNION where subqueries over a view carry different per-query values of
-- parallel_replicas_allow_view_over_mergetree used to abort with
-- "Can't determine table for parallel replicas": the parallel-replicas decision
-- and the plan build evaluated view eligibility with inconsistent contexts.
SELECT 1 FROM v_pr_view SETTINGS parallel_replicas_allow_view_over_mergetree = 0
EXCEPT DISTINCT
SELECT 1024 FROM v_pr_view SETTINGS parallel_replicas_allow_view_over_mergetree = 1;

SELECT 1 FROM v_pr_view SETTINGS parallel_replicas_allow_view_over_mergetree = 0
EXCEPT DISTINCT
SELECT materialize(-2147483648) FROM v_pr_view LIMIT 255 SETTINGS parallel_replicas_allow_view_over_mergetree = 0
EXCEPT DISTINCT
SELECT 1024 FROM v_pr_view LIMIT 1024 SETTINGS parallel_replicas_allow_view_over_mergetree = 1;

-- Still works (does not crash) when the view is eligible.
SELECT count() FROM v_pr_view SETTINGS parallel_replicas_allow_view_over_mergetree = 1;

DROP TABLE v_pr_view;
DROP TABLE mt_pr_view;
