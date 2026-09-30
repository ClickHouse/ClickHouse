-- FINAL is incompatible with parallel-replica reading (the FINAL merge path requires the read not to be
-- in parallel-reading mode). Classic parallel replicas disables PR for a query with FINAL; plan-based
-- parallel replicas must do the same instead of distributing the FINAL read. Regression test for PR
-- #111063 (an AST-fuzzer query added FINAL to a union branch and crashed the plan-based split).

DROP TABLE IF EXISTS t_pr_final_1 SYNC;
DROP TABLE IF EXISTS t_pr_final_2 SYNC;

CREATE TABLE t_pr_final_1 (a UInt64, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY a;
CREATE TABLE t_pr_final_2 (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_final_1 SELECT number, 1 FROM numbers(1000);
INSERT INTO t_pr_final_1 SELECT number, 2 FROM numbers(1000);  -- duplicate keys; FINAL keeps one per key
INSERT INTO t_pr_final_2 SELECT number + 1000 FROM numbers(1000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;

-- Single FINAL: correct dedup (1000 rows, sum of 0..999), kept local (no remote read).
SELECT count(), sum(a) FROM t_pr_final_1 FINAL;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') AS has_remote_read
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM t_pr_final_1 FINAL);

-- FINAL in a union branch (the fuzzer shape): correct results, no exception, for both fragment paths.
SELECT count(), sum(a) FROM (SELECT a FROM t_pr_final_1 FINAL UNION ALL SELECT a FROM t_pr_final_2)
SETTINGS parallel_replicas_local_plan = 1;
SELECT count(), sum(a) FROM (SELECT a FROM t_pr_final_1 FINAL UNION ALL SELECT a FROM t_pr_final_2)
SETTINGS parallel_replicas_local_plan = 0;

-- With FINAL present the whole query is not distributed (matches classic parallel replicas).
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') AS has_remote_read
FROM (EXPLAIN optimize = 1, description = 0
    SELECT a FROM t_pr_final_1 FINAL UNION ALL SELECT a FROM t_pr_final_2);

DROP TABLE t_pr_final_1 SYNC;
DROP TABLE t_pr_final_2 SYNC;
