-- Regression test for serializing an IN-subquery prepared set under plan-based parallel replicas.
-- The plan-based fragment shares `FutureSetFromSubquery` objects with the local plan and ships the
-- already-optimized fragment to the replicas. Two things used to break serialization, both raising
-- `Cannot serialize FutureSetFromSubquery with no query plan`:
--   1. the shared set's subquery plan (`source`) is consumed by the deferred set build during local
--      execution, so lazy send-time serialization found no plan;
--   2. primary key analysis builds the set in place and then dropped the still-valid subquery plan.
--      Needs a key column left of the `IN`.
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

-- Guard the preconditions, so that losing either one turns this test red instead of silently making the
-- assertions below pass under plain local execution: the plan must really contain a distributed read, and
-- index analysis must really build the set (otherwise nothing consumes its subquery plan early).
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t_pr_in WHERE a IN (SELECT k FROM src_pr_in))
    WHERE explain ILIKE '%ReadFromParallelReplicas%';
-- Assert the IN set reached the primary key condition, not merely that some index analysis ran: with
-- `use_index_for_in_with_subqueries = 0` the condition degrades to `true` while `Granules:` still prints.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_pr_in WHERE a IN (SELECT k FROM src_pr_in))
    WHERE explain ILIKE '%Condition:%a in %set)%';

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
-- `parallel_replicas_local_plan = 0` leaves the initiator with no local read, so a replica has to receive
-- the fragment: the coordinator cannot cancel every remote source before it sends.
SELECT count() FROM t_pr_in WHERE a IN (SELECT k FROM src_pr_in)
    SETTINGS serialize_query_plan = 0, use_index_for_in_with_subqueries = 0, parallel_replicas_local_plan = 0;
SELECT count() FROM t_pr_in WHERE b IN (SELECT k FROM src_pr_in)
    SETTINGS serialize_query_plan = 0, use_index_for_in_with_subqueries = 0, parallel_replicas_local_plan = 0;

-- `serialize_query_plan = 0` with index analysis still ON: the plan-based path serializes anyway, and the
-- set's plan is dropped by index analysis, so this is the only shape that shows the retention must key on
-- `parallel_replicas_plan_based` and not on `serialize_query_plan` alone.
SELECT count() FROM t_pr_in WHERE a IN (SELECT k FROM src_pr_in)
    SETTINGS serialize_query_plan = 0, use_index_for_in_with_subqueries = 1, parallel_replicas_local_plan = 0;

-- A NOT IN over the key column reaches index analysis the same way.
SELECT count() FROM t_pr_in WHERE a NOT IN (SELECT k FROM src_pr_in);

DROP TABLE t_pr_in;
DROP TABLE src_pr_in;
