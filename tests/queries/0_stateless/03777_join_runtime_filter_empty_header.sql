SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET analyzer_compatibility_join_using_top_level_identifier = 1;
SET enable_join_runtime_filters = 1;
SET query_plan_remove_unused_columns = 1; -- unused join-side columns must be pruned for stable EXPLAIN header output
SET query_plan_merge_filter_into_join_condition = 0; -- absorbing post-join filter into join condition changes Filter→Expression and drops join header columns
SET query_plan_optimize_join_order_limit = 10; -- CI may inject 0; without chooseJoinOrder the join column layout isn't rebuilt, causing __table3.c0 to drop from join output and runtime filter Filter step to collapse to Expression

EXPLAIN header=1
SELECT 1 AS c0 FROM (SELECT 1 AS c1) t0 JOIN (SELECT 1 AS c0) t1 USING (c0);

SELECT 1 AS c0 FROM (SELECT 1 AS c1) t0 JOIN (SELECT 1 AS c0) t1 USING (c0);
