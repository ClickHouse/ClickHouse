-- An `IN (subquery)` in a join's `ON` lives in `JoinStepLogical`'s own `ActionsDAG`, which
-- `JoinStepLogical::serialize` ships with the fragment. A conjunct reading the preserved side of an outer
-- join stays there instead of being pushed down to that side (`canPushDownFromOn`), so it is not covered by
-- the `FilterStep` check. With an unshippable set that made fragment serialization throw
-- `ReadFromStorageStep serialization is implemented only for StorageSystemOne` instead of keeping the
-- fragment local.
-- `generateRandom` with `max_string_length = 0` only ever produces the empty string, so the result is
-- stable while the source stays non-deterministic and unshippable.

DROP TABLE IF EXISTS t_join_on_fact;
DROP TABLE IF EXISTS t_join_on_dim;

CREATE TABLE t_join_on_fact (k Int64, s String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_join_on_dim (k Int64, v Int64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_join_on_fact SELECT number, if(number = 0, '', toString(number)) FROM numbers(100);
-- `v` is offset so a matched row is distinguishable from the default an unmatched one gets.
INSERT INTO t_join_on_dim SELECT number, number + 1000 FROM numbers(50);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;

-- `ALL LEFT JOIN` keeps every left row, so the count does not depend on how many rows matched. This threw
-- before. `parallel_replicas_local_plan` is left to the value CI randomizes it to.
SELECT count() FROM t_join_on_fact ALL LEFT JOIN t_join_on_dim ON t_join_on_fact.k = t_join_on_dim.k
    AND t_join_on_fact.s IN (SELECT x FROM generateRandom('x String', 1, 0, 1) LIMIT 3);

-- The rows themselves are right: only `s = ''`, i.e. `k = 0`, can satisfy the `IN`, so exactly one row
-- matches and the other 99 get the default `v`.
SELECT countIf(t_join_on_dim.v > 0) FROM t_join_on_fact ALL LEFT JOIN t_join_on_dim
    ON t_join_on_fact.k = t_join_on_dim.k
    AND t_join_on_fact.s IN (SELECT x FROM generateRandom('x String', 1, 0, 1) LIMIT 3);

-- The fragment stays local: the split marker above the join is left unconverted, so nothing is shipped.
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') = 0 AS stayed_local
FROM (EXPLAIN optimize = 1, description = 0
    SELECT count() FROM t_join_on_fact ALL LEFT JOIN t_join_on_dim ON t_join_on_fact.k = t_join_on_dim.k
        AND t_join_on_fact.s IN (SELECT x FROM generateRandom('x String', 1, 0, 1) LIMIT 3));

-- Regression guard: a serializable subquery in `ON` is still shipped with the join.
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS distributed
FROM (EXPLAIN optimize = 1, description = 0
    SELECT count() FROM t_join_on_fact ALL LEFT JOIN t_join_on_dim ON t_join_on_fact.k = t_join_on_dim.k
        AND t_join_on_fact.s IN (SELECT s FROM t_join_on_fact WHERE k % 2 = 0));

-- And it gives the right rows when the replicas actually run it: the tables are small enough that the
-- initiator's local arm would otherwise finish first and the shipped fragment would never be rebuilt
-- remotely, so slow the local read down. The failpoint is a no-op when the local plan is off, which is
-- fine. Matches are the even `k` below 50, i.e. 25 rows.
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;
SELECT countIf(t_join_on_dim.v > 0) FROM t_join_on_fact ALL LEFT JOIN t_join_on_dim
    ON t_join_on_fact.k = t_join_on_dim.k
    AND t_join_on_fact.s IN (SELECT s FROM t_join_on_fact WHERE k % 2 = 0);
SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

DROP TABLE t_join_on_fact;
DROP TABLE t_join_on_dim;
