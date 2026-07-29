-- Regression test for serializing an IN-subquery prepared set under plan-based parallel replicas.
-- The plan-based fragment shares FutureSetFromSubquery objects with the local plan and ships the
-- already-optimized fragment to the replicas. Three things used to break serialization:
--   1. the shared set's subquery plan (`source`) is consumed by the deferred set build during local
--      execution, so lazy send-time serialization threw "Cannot serialize FutureSetFromSubquery with
--      no query plan";
--   2. the subquery's own plan reads a MergeTree under parallel replicas, so it carries a transient
--      ParallelReplicasSplitStep, which has no serialization ("Method serialize is not implemented for
--      ParallelReplicasSplit");
--   3. primary key analysis builds the set in place and then dropped the still-valid subquery plan, so
--      serializing the fragment threw the same "no query plan" error. Needs a key column left of the IN.
-- Results must match non-parallel execution. src holds 0..49, so `a IN (SELECT k FROM src)` keeps a=0..49.

DROP TABLE IF EXISTS t_pr_in;
DROP TABLE IF EXISTS src_pr_in;

CREATE TABLE t_pr_in (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_in SELECT number, number % 10 FROM numbers(100000);
CREATE TABLE src_pr_in (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO src_pr_in SELECT number FROM numbers(50);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
-- Pin the manual mode: CI's randomized automatic_parallel_replicas_mode can cost-decide against
-- parallel replicas, so the plan-based split (and this serialization path) would not engage.
SET automatic_parallel_replicas_mode = 0;
SET serialize_query_plan = 1;
SET use_index_for_in_with_subqueries = 1;

-- local_plan = 1: the initiator builds a local plan and ships a serialized fragment to the replicas.
SET parallel_replicas_local_plan = 1;
SELECT count() FROM t_pr_in WHERE a IN (SELECT k FROM src_pr_in);
SELECT count() FROM t_pr_in WHERE b IN (SELECT k FROM src_pr_in);
SELECT b, count() FROM t_pr_in WHERE a IN (SELECT k FROM src_pr_in) GROUP BY b ORDER BY b;

-- local_plan = 0: the whole fragment (including the set) is serialized and sent to every replica.
SET parallel_replicas_local_plan = 0;
SELECT count() FROM t_pr_in WHERE a IN (SELECT k FROM src_pr_in);
SELECT b, count() FROM t_pr_in WHERE a IN (SELECT k FROM src_pr_in) GROUP BY b ORDER BY b;

SET parallel_replicas_local_plan = 1;

-- Once the in-place built set exceeds `use_index_for_in_with_subqueries_max_values` it drops its
-- materialized elements and keeps only the hash table, so the set cannot be serialized by value and the
-- retained subquery plan is the only thing that can be shipped.
SELECT count() FROM t_pr_in WHERE a IN (SELECT k FROM src_pr_in) SETTINGS use_index_for_in_with_subqueries_max_values = 5;

-- Serialization must happen even with `serialize_query_plan = 0`, because this path always sends the plan.
-- `use_index_for_in_with_subqueries = 0` keeps the set out of index analysis, so only the local pipeline
-- consumes its subquery plan -- the case that only the eager serialization can catch.
SELECT count() FROM t_pr_in WHERE a IN (SELECT k FROM src_pr_in)
    SETTINGS serialize_query_plan = 0, use_index_for_in_with_subqueries = 0;
SELECT count() FROM t_pr_in WHERE b IN (SELECT k FROM src_pr_in)
    SETTINGS serialize_query_plan = 0, use_index_for_in_with_subqueries = 0;

-- A NOT IN over the key column reaches index analysis the same way.
SELECT count() FROM t_pr_in WHERE a NOT IN (SELECT k FROM src_pr_in);

DROP TABLE t_pr_in;
DROP TABLE src_pr_in;
