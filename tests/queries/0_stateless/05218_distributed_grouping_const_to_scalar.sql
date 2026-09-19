-- Tags: shard
-- ^ the queries read through `remote` and a `Distributed` table over `test_shard_localhost`.

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/120250
-- `grouping` under `GROUPING SETS` / `ROLLUP` / `CUBE` over a `Distributed` table threw
-- "Unknown expression or function identifier `__grouping_set`" (`UNKNOWN_IDENTIFIER`) once a
-- specialization state constant crossed `optimize_const_name_size`: `ReplaceLongConstWithScalarVisitor`
-- turned those constants into `__getScalar` calls, so `removeGroupingFunctionSpecializations` no longer
-- recognised the analyzer-built call and shipped it, virtual `__grouping_set` argument and all.
--
-- The expected values below are the results of the SAME queries against the LOCAL table, so they
-- assert the grouping bitmasks themselves and not merely that the query stopped throwing.

SET optimize_const_name_size = 0; -- rewrite every constant: the smallest reproducer.
SET prefer_localhost_replica = 0; -- `clickhouse-test` randomizes this; without the pin half the runs ship nothing.
SET serialize_query_plan = 0; -- the query-text path is what regressed; case 9 turns it on explicitly.
SET automatic_parallel_replicas_mode = 0; -- `clickhouse-test` randomizes this; case 10 needs the explicit mode.

DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS d0;

CREATE TABLE t0 (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t0 SELECT number FROM numbers(10);
CREATE TABLE d0 AS t0 ENGINE = Distributed(test_shard_localhost, currentDatabase(), t0);

-- 1. `GROUPING SETS` over `remote` -- the `ClusterProxy::executeQuery` path, and the query from the issue.
SELECT number, grouping(number, number % 2) AS gr
FROM remote('127.0.0.1', numbers(10))
GROUP BY GROUPING SETS ((number), (number % 2))
ORDER BY number, gr;

-- 2. `GROUPING SETS` over a `Distributed` table -- `StorageDistributed::read` and `SelectStreamFactory`.
SELECT a, grouping(a, a % 2) AS gr
FROM d0
GROUP BY GROUPING SETS ((a), (a % 2))
ORDER BY a, gr;

-- 3. Same, on the local-shard path.
SELECT a, grouping(a, a % 2) AS gr
FROM d0
GROUP BY GROUPING SETS ((a), (a % 2))
ORDER BY a, gr
SETTINGS prefer_localhost_replica = 1;

-- 4. Same at a positive threshold -- the `max_size > 0` branch of the visitor, which case 2 skips.
SELECT a, grouping(a, a % 2) AS gr
FROM d0
GROUP BY GROUPING SETS ((a), (a % 2))
ORDER BY a, gr
SETTINGS optimize_const_name_size = 1;

-- 5. `ROLLUP` -- `__groupingForRollup`.
SELECT a, grouping(a) AS gr
FROM d0
WHERE a < 5
GROUP BY ROLLUP(a)
ORDER BY a, gr;

-- 6. `CUBE` -- `__groupingForCube`.
SELECT a % 2 AS k1, a % 3 AS k2, grouping(k1, k2) AS gr
FROM d0
WHERE a < 6
GROUP BY CUBE(k1, k2)
ORDER BY k1, k2, gr;

-- 7. Plain `GROUP BY` -- `__groupingOrdinary`, which carries no `__grouping_set` argument and so was
-- shipped in a broken-but-benign shape. This pins the generalized text it gets now.
SELECT a, grouping(a) AS gr
FROM d0
GROUP BY a
ORDER BY a, gr;

-- 8. A long constant inside a non-constant key that is ALSO a `grouping` argument. The guard must skip
-- only the trailing state arguments: skipping the whole call instead rewrites the `GROUP BY` copy of
-- `k` while the copy inside `grouping` stays a literal, and the two stop denoting the same key. The key
-- must not be a pure constant, because `optimize_group_by_constant_keys` is randomized and would
-- eliminate it.
SELECT concat('cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc', toString(a)) AS k,
       a,
       grouping(k, a) AS gr
FROM d0
WHERE a < 3
GROUP BY GROUPING SETS ((k), (a))
ORDER BY k, a, gr;

-- 9. The serialized-plan shipping path: with `serialize_query_plan = 1` the shard stream is built by
-- `createLocalPlan` from the generalized query text (`SelectStreamFactory`), instead of the
-- `getSampleBlockAndPlannerContext` branch cases 1-8 take.
SELECT a, grouping(a, a % 2) AS gr
FROM d0
GROUP BY GROUPING SETS ((a), (a % 2))
ORDER BY a, gr
SETTINGS serialize_query_plan = 1;

-- 10. Parallel replicas over the local table, which is the fourth de-specializer call site
-- (`findParallelReplicasQuery` -> `buildQueryPlanForParallelReplicas`), where the specialization is
-- re-resolved on a peer. The assertion below is what makes this case about parallel replicas: without it
-- an inert configuration would still return the right rows.
SELECT a, grouping(a, a % 2) AS gr
FROM t0
GROUP BY GROUPING SETS ((a), (a % 2))
ORDER BY a, gr
SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 3,
         parallel_replicas_for_non_replicated_merge_tree = 1,
         parallel_replicas_local_plan = 0, -- randomized too; 0 is the arm that re-resolves on a peer.
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         log_comment = '05218_parallel_replicas';

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05218_parallel_replicas'
  AND type = 'QueryFinish' AND initial_query_id = query_id
SETTINGS enable_parallel_replicas = 0;

DROP TABLE d0;
DROP TABLE t0;
