-- Under plan-based parallel replicas the runtime filter decision has to be the same on the initiator
-- and on every replica, because it decides the join algorithm and therefore the read type of the
-- coordinated side, whose stream identity the initiator registers with the coordinator up front
-- (`#split_i` is appended only on the in-order path).
--
-- The decision reads a row estimate: `tryAddJoinRuntimeFilter` declines a probe side of at most
-- `join_runtime_filter_min_probe_rows` rows. Row estimates are deliberately not part of the plan
-- format, so on a deserialized fragment that estimate is absent. A replica that re-decided would
-- therefore add the filter the initiator declined, and adding one erases every non-hash algorithm,
-- which takes the sorting step with it: the coordinated read turns from `InOrder` into `Default` and
-- the coordinator rejects the replica's read request with `Got read request from replica N for
-- unknown stream`.
--
-- `jd_probe` is deliberately below the threshold while `jd_build` is far above it, so the initiator
-- declines and only a replica could disagree.

DROP TABLE IF EXISTS jd_probe SYNC;
DROP TABLE IF EXISTS jd_build SYNC;

CREATE TABLE jd_probe (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE jd_build (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO jd_probe SELECT number FROM numbers(500);            -- below join_runtime_filter_min_probe_rows
INSERT INTO jd_build SELECT number FROM numbers(100000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;

-- Pinned against the test runner's randomization, which otherwise suppresses the disagreement:
-- `join_runtime_filter_min_probe_rows` is the threshold under test, `enable_join_runtime_filters`
-- arms the pass at all, `join_algorithm` has to offer both a sorting and a hash family member for
-- the pruning to be observable, `query_plan_join_swap_table` keeps the small side as the probe,
-- `optimize_read_in_order` allows the in-order read whose stream identity differs, and
-- `query_plan_optimize_join_order_limit` has to be non-zero for the initiator to derive the estimate
-- it declines on. One thread keeps the coordinated read to a single split.
SET join_runtime_filter_min_probe_rows = 1000;
SET enable_join_runtime_filters = 1;
SET join_algorithm = 'full_sorting_merge,hash';
SET query_plan_join_swap_table = 'false';
SET optimize_read_in_order = 1;
SET query_plan_optimize_join_order_limit = 10;
SET max_threads = 1;

-- The join is really shipped, so the assertions below are about a deserialized fragment.
SELECT 'distributed', countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM jd_probe INNER JOIN jd_build ON jd_probe.id = jd_build.id);

-- The failpoint slows the initiator's local read, so a remote replica reaches the coordinator for the
-- coordinated side rather than finding the ranges already taken.
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

-- INNER and LEFT coordinate the left (probe) side, RIGHT coordinates the right (build) side, so both
-- placements of the coordinated read relative to the runtime filter are covered.
SELECT 'INNER small probe', count() FROM jd_probe INNER JOIN jd_build ON jd_probe.id = jd_build.id
SETTINGS parallel_replicas_local_plan = 1;
SELECT 'LEFT small probe', count() FROM jd_probe LEFT JOIN jd_build ON jd_probe.id = jd_build.id
SETTINGS parallel_replicas_local_plan = 1;
SELECT 'RIGHT small probe', count() FROM jd_probe RIGHT JOIN jd_build ON jd_probe.id = jd_build.id
SETTINGS parallel_replicas_local_plan = 1;

-- The same three without the initiator's local plan, so every fragment that runs is a deserialized one.
SELECT 'INNER small probe remote', count() FROM jd_probe INNER JOIN jd_build ON jd_probe.id = jd_build.id
SETTINGS parallel_replicas_local_plan = 0;
SELECT 'LEFT small probe remote', count() FROM jd_probe LEFT JOIN jd_build ON jd_probe.id = jd_build.id
SETTINGS parallel_replicas_local_plan = 0;
SELECT 'RIGHT small probe remote', count() FROM jd_probe RIGHT JOIN jd_build ON jd_probe.id = jd_build.id
SETTINGS parallel_replicas_local_plan = 0;

SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

-- A probe side above the threshold is the case the initiator filters itself, where the pruned
-- algorithm list travels and both sides already agreed before this change.
SELECT 'INNER large probe', count() FROM jd_build INNER JOIN jd_probe ON jd_build.id = jd_probe.id
SETTINGS parallel_replicas_local_plan = 1;

DROP TABLE jd_probe SYNC;
DROP TABLE jd_build SYNC;
