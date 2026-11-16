-- An `IN (subquery)` set is shipped as its subquery plan and rebuilt by every replica, so a
-- non-deterministic source must never be shipped: each replica would build a *different* set.
-- `generateRandom()` is read through `ReadFromStorageStep`, whose `isSerializable` accepts only
-- `system.one`, so the set's plan cannot be serialized and the fragment has to stay local. Shipping it
-- threw `Method serialize is not implemented`.
-- `max_string_length = 0` pins every generated value to the empty string (the generator takes
-- `length = rng() % (max_string_length + 1)`), so the result is stable while the source stays genuinely
-- random - and therefore genuinely unshippable.

DROP TABLE IF EXISTS t_gen_in;

CREATE TABLE t_gen_in (k Int64, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_gen_in SELECT number, if(number = 0, '', toString(number)) FROM numbers(100);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;

-- Only the empty string is generated, so exactly the one row with `s = ''` matches. This threw before.
-- `parallel_replicas_local_plan` is left to the value CI randomizes it to.
SELECT count() FROM t_gen_in WHERE s IN (SELECT x FROM generateRandom('x String', 1, 0, 1) LIMIT 3);

-- The fragment stays local: the split marker is left unconverted.
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') = 0 AS stayed_local
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM t_gen_in WHERE s IN (SELECT x FROM generateRandom('x String', 1, 0, 1) LIMIT 3));

-- Regression guard: a subquery over a MergeTree table is serializable and is still distributed.
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS distributed
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM t_gen_in WHERE s IN (SELECT s FROM t_gen_in WHERE k % 2 = 0));

DROP TABLE t_gen_in;
