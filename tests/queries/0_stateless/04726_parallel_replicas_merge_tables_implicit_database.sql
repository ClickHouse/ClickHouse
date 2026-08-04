-- The one-argument form of the merge table function resolves the implicit database on the
-- initiator. The query shipped to the replicas must carry that database explicitly - a replica
-- executes it with its own current database (not the initiator session's one), so the regexp
-- would otherwise be matched against the wrong database.
-- https://github.com/ClickHouse/ClickHouse/issues/67770

DROP TABLE IF EXISTS t_pr_merge_implicit_1;
DROP TABLE IF EXISTS t_pr_merge_implicit_2;

CREATE TABLE t_pr_merge_implicit_1 (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_pr_merge_implicit_2 (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pr_merge_implicit_1 SELECT number FROM numbers(1000);
INSERT INTO t_pr_merge_implicit_2 SELECT number + 1000 FROM numbers(1000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_allow_merge_tables = 1;
SET automatic_parallel_replicas_mode = 0;

-- Slow the initiator's local reads so that the remote replicas actually receive and plan the query.
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

SELECT count(), sum(k) FROM merge('^t_pr_merge_implicit_');

SELECT count(), sum(m.k) FROM merge('^t_pr_merge_implicit_1$') AS m INNER JOIN t_pr_merge_implicit_2 AS d ON m.k + 1000 = d.k;

SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

DROP TABLE t_pr_merge_implicit_1;
DROP TABLE t_pr_merge_implicit_2;
