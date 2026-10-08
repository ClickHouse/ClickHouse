-- The step description of a plan-based parallel replicas read is formatted with `actions` and
-- `indexes` disabled: with them, `describeActions` and `describeIndexes` ask every MergeTree read of
-- the shipped fragment for its analysis result, so index analysis ran while a description string was
-- being built. That analysis is done without the query's filter pushed into the read, so it reports
-- the whole table and a query with `max_rows_to_read` failed with `TOO_MANY_ROWS` even though the
-- actual read is empty.

DROP TABLE IF EXISTS t_pr_step_description;

CREATE TABLE t_pr_step_description (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_step_description SELECT number, number % 10 FROM numbers(100000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
-- Statistics-only mode: the plan with parallel replicas is built (so its step description is
-- formatted) but never substituted for the local plan. This is the combination the functional tests
-- randomize.
SET automatic_parallel_replicas_mode = 2;
SET max_rows_to_read = 1000;

SELECT a, uniq(b) FROM t_pr_step_description WHERE 0 != 0 GROUP BY a;

DROP TABLE t_pr_step_description;
