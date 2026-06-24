-- Regression for the follow-up to #108129: the planner NDV used to size the parallel_hash build hash
-- map must be the right (build) input relation's distinct-key count, NOT the post-join result-key NDV.
-- The join-order DP clamps both equi-key NDVs to min(left, right) (see `DPJoinEntry` in joinOrder.cpp),
-- so reading the right key NDV from the join *output* stats undersizes the build map whenever the probe
-- (left) side has fewer distinct keys than the build (right) side: the streaming build would then
-- preallocate to that tiny min and rehash through the full right key set, defeating the exact-size goal.
--
-- Here the build (right) side has ~30000 distinct keys while the probe (left) side has only 10, so the
-- post-join min NDV is 10. The build hash map must still be preallocated to ~30000 (the right input
-- relation NDV), and the deferred (HLL-sized) build must be skipped because a trustworthy uniq-backed
-- NDV is available.
--
-- `query_plan_optimize_join_order_limit` and `query_plan_join_swap_table` are set explicitly so the
-- column statistics are produced and the large table stays the build side regardless of the randomized
-- test settings.

SET allow_experimental_statistics = 1;
SET allow_statistics_optimize = 1;
SET mutations_sync = 2; -- MATERIALIZE STATISTICS must finish before the joins read the uniq stats
SET enable_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 'false';
SET collect_hash_table_stats_during_joins = 0; -- no cross-run cache hint, so the plan NDV is consulted
SET parallel_hash_join_threshold = 0; -- force ConcurrentHashJoin regardless of build size
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_probe_small;
DROP TABLE IF EXISTS t_build_large;

-- Probe (left) side: only 10 distinct keys, uniq-backed.
CREATE TABLE t_probe_small (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();
-- Build (right) side: ~30000 distinct keys, uniq-backed.
CREATE TABLE t_build_large (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_probe_small SELECT number % 10 AS k, number AS v FROM numbers(1000);
INSERT INTO t_build_large SELECT number % 30000 AS k, number AS v FROM numbers(300000);

ALTER TABLE t_probe_small MATERIALIZE STATISTICS k;
ALTER TABLE t_build_large MATERIALIZE STATISTICS k;

-- Reference result: plain hash join.
SET join_algorithm = 'hash';
SELECT 'asym', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe_small l INNER JOIN t_build_large r ON l.k = r.k;

SET join_algorithm = 'parallel_hash';
SET log_comment = '04409_asym_ndv';
SELECT 'asym', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe_small l INNER JOIN t_build_large r ON l.k = r.k;

SYSTEM FLUSH LOGS query_log;

-- The build map preallocates to ~the build-side NDV (30000), not the post-join min(10, 30000) = 10,
-- and the deferred build is skipped. A value < 20000 here means the post-join (clamped) NDV leaked in.
SELECT
    'right ndv sizes build not min',
    countIf(ProfileEvents['HashJoinPreallocatedElementsInHashTables'] BETWEEN 20000 AND 60000) = count(),
    countIf(ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables'] > 0) = 0,
    count() > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = '04409_asym_ndv';

DROP TABLE t_probe_small;
DROP TABLE t_build_large;
