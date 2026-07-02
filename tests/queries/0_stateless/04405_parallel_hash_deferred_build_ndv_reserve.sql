-- Tags: no-random-settings, no-random-merge-tree-settings
-- The regression guards below assert exact spill decisions and tight reserve/preallocation ranges, so
-- they must run with the settings this test pins (the build path, the spill cap, max_threads); the
-- stateless-test settings randomizer perturbs the spill decision and the map sizing through unpinned
-- side channels (see 04146_spilling_hash_join_low_threshold.sql for the same class of test).
--
-- Part 2 of the follow-up to #108129: the deferred (exact-size) parallel_hash build reserves the
-- hash maps by the estimated number of distinct keys (a per-slot HyperLogLog over the scatter
-- hashes), not by the source-row count. For a duplicate-heavy build side this avoids over-allocating
-- the maps (and, when wrapped by SpillingHashJoin, avoids a spurious spill the over-allocation would
-- have triggered).
--
-- The build side (t_build) has 50000 distinct keys, each repeated 20 times => 1,000,000 source rows.
-- The reserve must be ~NDV (50000), not ~source rows (1,000,000). `query_plan_join_swap_table=false`
-- pins the duplicate-heavy table as the build side (otherwise the planner would swap the small probe
-- in and the duplicate-heavy build would never be exercised).

SET collect_hash_table_stats_during_joins = 0; -- no size hint => the deferred build path
SET parallel_hash_join_threshold = 0; -- force ConcurrentHashJoin regardless of the build size
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 'false'; -- keep t_build (the right table) as the build side

DROP TABLE IF EXISTS t_build;
DROP TABLE IF EXISTS t_probe;

CREATE TABLE t_build (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE t_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY ();

INSERT INTO t_build SELECT number % 50000 AS k, number AS v FROM numbers(1000000);
INSERT INTO t_probe SELECT number AS k, number * 7 AS v FROM numbers(60000);

-- Reference: plain hash join, no spilling.
SET join_algorithm = 'hash';
SET max_bytes_before_external_join = 0;

SELECT 'inner', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k;
SELECT 'left', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l LEFT JOIN t_build r ON l.k = r.k;
SELECT 'right', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l RIGHT JOIN t_build r ON l.k = r.k;
SELECT 'full', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l FULL JOIN t_build r ON l.k = r.k;

-- Scenario A: standalone parallel_hash (no spill cap). Results must match, and the reserve must be
-- sized by distinct keys.
SET join_algorithm = 'parallel_hash';
SET max_bytes_before_external_join = 0;
SET log_comment = '04405_ndv_standalone';

SELECT 'inner', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k;
SELECT 'left', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l LEFT JOIN t_build r ON l.k = r.k;
SELECT 'right', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l RIGHT JOIN t_build r ON l.k = r.k;
SELECT 'full', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l FULL JOIN t_build r ON l.k = r.k;

-- Scenario B: wrapped by SpillingHashJoin with a 64 MiB cap. The distinct-key-sized footprint
-- (~44 MiB, mostly the 1,000,000 stored rows and their RowRefList batches) fits, so the build must
-- NOT spill. Sizing the reserve by the 1,000,000 source rows (the old behavior) would project well
-- over 100 MiB and spuriously switch to GraceHashJoin here.
SET max_bytes_before_external_join = 67108864; -- 64 MiB
SET log_comment = '04405_ndv_capped_no_spill';

SELECT 'inner', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k;

SYSTEM FLUSH LOGS query_log;

-- Effectiveness: the deferred reserve is ~NDV (50000), far below the 1,000,000 source rows.
SELECT
    'reserve ~ ndv not rows',
    max(reserved) BETWEEN 40000 AND 150000
FROM
(
    SELECT ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables'] AS reserved
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND query_kind = 'Select'
        AND log_comment = '04405_ndv_standalone'
);

-- Regression guard: the capped wrapped build did not spill.
SELECT
    'capped build did not spill',
    countIf(ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0) = 0,
    count() > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = '04405_ndv_capped_no_spill';

DROP TABLE t_build;
DROP TABLE t_probe;
