-- Naming the plan fragment that plan-based parallel replicas send to a replica must not run index
-- analysis, because index analysis enforces the read limits. A `count()` answered from partition
-- metadata reads one row and no marks, yet describing the fragment sized the whole partition and the
-- query failed with TOO_MANY_ROWS before a replica was even contacted. `max_rows_to_read_leaf`, which
-- applies to the replica's read, failed the same way and from the same place.

DROP TABLE IF EXISTS t_pr_dump_limits;

CREATE TABLE t_pr_dump_limits (p Date, k UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
INSERT INTO t_pr_dump_limits SELECT toDate('2020-09-01') + number % 3, number FROM numbers(100000);

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 1,
    parallel_replicas_plan_based = 1;
-- The metadata shortcut is what makes the read trivial; the trivial-count optimization is off so the
-- count goes through the projection rather than around it.
SET optimize_use_projections = 1, optimize_use_implicit_projections = 1, optimize_trivial_count_query = 0;

SELECT count() FROM t_pr_dump_limits WHERE p = '2020-09-01' SETTINGS max_rows_to_read = 1;
SELECT count() FROM t_pr_dump_limits WHERE p = '2020-09-01' SETTINGS max_rows_to_read_leaf = 1;

DROP TABLE t_pr_dump_limits;
