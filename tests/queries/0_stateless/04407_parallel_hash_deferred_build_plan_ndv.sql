-- Part 4 of the follow-up to #108129: when the planner has a trustworthy (uniq-backed) distinct-key
-- estimate for the right join key, the exact-size parallel_hash build skips the deferred build and
-- preallocates the maps to that distinct-key count on the streaming path (like the warm ht_size
-- path). The 0.1 * rows mock cardinality (used when no `uniq` statistic exists) must NOT be trusted:
-- such a build still uses the deferred (HLL-sized) path.
--
-- `query_plan_optimize_join_order_limit` and `query_plan_join_swap_table` are set explicitly so the
-- column statistics are produced and the duplicate-heavy table stays the build side regardless of the
-- randomized test settings.

SET allow_experimental_statistics = 1;
SET allow_statistics_optimize = 1;
SET mutations_sync = 2; -- MATERIALIZE STATISTICS must finish before the joins read the uniq stats
SET enable_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 'false';
SET collect_hash_table_stats_during_joins = 0; -- no cross-run cache hint
SET parallel_hash_join_threshold = 0; -- force ConcurrentHashJoin regardless of build size
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_build_uniq;
DROP TABLE IF EXISTS t_probe_uniq;
DROP TABLE IF EXISTS t_build_nostat;
DROP TABLE IF EXISTS t_probe_nostat;

-- Both sides carry a real `uniq` statistic on the key, so the result key NDV is trustworthy.
CREATE TABLE t_build_uniq (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe_uniq (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();
-- No statistics: the planner can only use the 0.1 * rows mock, which must not be trusted.
CREATE TABLE t_build_nostat (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe_nostat (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();

-- 30000 distinct keys, each repeated 10 times = 300000 source rows.
INSERT INTO t_build_uniq SELECT number % 30000 AS k, number * 3 AS v FROM numbers(300000);
INSERT INTO t_probe_uniq SELECT number % 30000 AS k, number * 7 AS v FROM numbers(300000);
INSERT INTO t_build_nostat SELECT number % 30000 AS k, number * 3 AS v FROM numbers(300000);
INSERT INTO t_probe_nostat SELECT number % 30000 AS k, number * 7 AS v FROM numbers(300000);

ALTER TABLE t_build_uniq MATERIALIZE STATISTICS k;
ALTER TABLE t_probe_uniq MATERIALIZE STATISTICS k;

-- Reference results: plain hash join.
SET join_algorithm = 'hash';
SELECT 'uniq', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe_uniq l INNER JOIN t_build_uniq r ON l.k = r.k;
SELECT 'nostat', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe_nostat l INNER JOIN t_build_nostat r ON l.k = r.k;
-- Self-join: both sides resolve to the same INPUT column names; results must stay correct.
SELECT 'self', count(), sum(cityHash64(a.k, a.v, b.v)) FROM t_build_uniq a INNER JOIN t_build_uniq b ON a.k = b.k;

SET join_algorithm = 'parallel_hash';

SET log_comment = '04407_uniq_ndv';
SELECT 'uniq', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe_uniq l INNER JOIN t_build_uniq r ON l.k = r.k;

SET log_comment = '04407_mock_ndv';
SELECT 'nostat', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe_nostat l INNER JOIN t_build_nostat r ON l.k = r.k;

SET log_comment = '04407_self_join';
SELECT 'self', count(), sum(cityHash64(a.k, a.v, b.v)) FROM t_build_uniq a INNER JOIN t_build_uniq b ON a.k = b.k;

SYSTEM FLUSH LOGS query_log;

-- A uniq-backed key skips the deferred build and preallocates ~NDV (30000) on the streaming path:
-- the warm prealloc event fires (and is ~NDV), the deferred event does not.
SELECT
    'uniq key preallocates not defers',
    countIf(ProfileEvents['HashJoinPreallocatedElementsInHashTables'] BETWEEN 20000 AND 60000) = count(),
    countIf(ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables'] > 0) = 0,
    count() > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = '04407_uniq_ndv';

-- A no-uniq (mock) key is not trusted: it still uses the deferred build, no warm preallocation.
SELECT
    'mock key still defers',
    countIf(ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables'] > 0) = count(),
    countIf(ProfileEvents['HashJoinPreallocatedElementsInHashTables'] > 0) = 0,
    count() > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = '04407_mock_ndv';

DROP TABLE t_build_uniq;
DROP TABLE t_probe_uniq;
DROP TABLE t_build_nostat;
DROP TABLE t_probe_nostat;
