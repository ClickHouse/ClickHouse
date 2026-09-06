-- Regression test for an abort in `moveSetsFromLocalPlanToReplicasPlan`.
--
-- Once automatic parallel replicas picks the replicas plan, the sets the single-node plan already
-- filled are transplanted into it. The walk over the chosen plan reaches the sets nested inside a
-- set's own source plan, and every set it reaches must have a counterpart in the single-node plan or
-- it throws. A nested `IN` breaks that: the outer set on the single-node side was filled in place
-- during index analysis, which consumed its source plan, so its nested set was never collected -
-- while the chosen plan still carries both. The lookup for the nested one then failed and the server
-- aborted with `Cannot find a matching set in the map of sets from single-replica plan`
-- (`abort_on_logical_error` is on for tests).
--
-- Nothing is missing: a set that has just been adopted is built, so `makePlansForSets` skips its
-- source plan and the sets nested in it are never created. The walk stops there now.

DROP TABLE IF EXISTS t_autopr_nested_in;
DROP TABLE IF EXISTS t_autopr_nested_in_mid;
DROP TABLE IF EXISTS t_autopr_nested_in_inner;

CREATE TABLE t_autopr_nested_in (key UInt64, non_key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_autopr_nested_in SELECT number, number % 500 FROM numbers(100000);

CREATE TABLE t_autopr_nested_in_mid (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_autopr_nested_in_mid SELECT number, number % 10 FROM numbers(300);

CREATE TABLE t_autopr_nested_in_inner (z UInt64) ENGINE = MergeTree ORDER BY z;
INSERT INTO t_autopr_nested_in_inner SELECT number FROM numbers(5);

-- `merge_tree_min_bytes_per_task_for_remote_reading` is what lets the replicas side of the cost model
-- divide the read across replicas at this data size, so replicas are actually chosen and the
-- transplant runs at all. The aggregate keeps the output small, which is the other half of that.
SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    automatic_parallel_replicas_min_bytes_per_replica = 0,
    merge_tree_min_bytes_per_task_for_remote_reading = 1024, max_threads = 4;
SET enable_analyzer = 1;

-- The first run only collects the statistics the cost model needs; the second is the one that can
-- pick replicas and reach the transplant. Both must return 1496910000.
SELECT sum(key) FROM t_autopr_nested_in
WHERE non_key IN (SELECT x FROM t_autopr_nested_in_mid WHERE y IN (SELECT z FROM t_autopr_nested_in_inner))
FORMAT TSV;

SELECT sum(key) FROM t_autopr_nested_in
WHERE non_key IN (SELECT x FROM t_autopr_nested_in_mid WHERE y IN (SELECT z FROM t_autopr_nested_in_inner))
FORMAT TSV;

DROP TABLE t_autopr_nested_in;
DROP TABLE t_autopr_nested_in_mid;
DROP TABLE t_autopr_nested_in_inner;
