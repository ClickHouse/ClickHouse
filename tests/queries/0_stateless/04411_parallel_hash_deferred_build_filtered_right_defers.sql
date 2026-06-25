-- Tags: no-random-settings, no-random-merge-tree-settings

-- Follow-up to #108129: the planner NDV shortcut that skips the cold deferred parallel_hash build
-- (`extractTrustworthyRightKeyNdv`) must trust only a real, unfiltered `uniq` statistic of a base
-- relation. When the build (right) relation is filtered, its key NDV from the condition-selectivity
-- estimator is clamped by an estimated row count (possibly from default factors of an unrelated
-- predicate) and a filter can drop whole keys, so the value is no longer a reliable `uniq` count. Such
-- a build must fall back to the deferred (HLL-sized) build, not the streaming preallocation shortcut.
--
-- The build table has a `uniq` statistic on the key `k` but no statistics on the filter column `f`. The
-- filtered join must defer (HLL); the otherwise-identical unfiltered join uses the shortcut. Both must
-- return the same result as a plain `hash` join.
--
-- `no-random-settings`/`no-random-merge-tree-settings`: the assertions check exact preallocation vs
-- deferral decisions, which the stateless settings randomizer perturbs through unpinned side channels
-- (max_threads, the external-spill ratio, the join-order algorithm/limit, the statistics toggles); the
-- in-file SET statements still win over the command-line defaults.

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

DROP TABLE IF EXISTS t_build;
DROP TABLE IF EXISTS t_probe;

-- `k` has a uniq statistic; `f` (the filter column) has none.
CREATE TABLE t_build (k UInt64 STATISTICS(uniq), f UInt8, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();

-- 30000 distinct build keys, each repeated 10 times.
INSERT INTO t_build SELECT number % 30000 AS k, number % 2 AS f, number * 3 AS v FROM numbers(300000);
INSERT INTO t_probe SELECT number % 30000 AS k, number * 7 AS v FROM numbers(300000);

ALTER TABLE t_build MATERIALIZE STATISTICS k;
ALTER TABLE t_probe MATERIALIZE STATISTICS k;

-- Reference results: plain hash join.
SET join_algorithm = 'hash';
SELECT 'filtered', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k WHERE r.f = 1;
SELECT 'unfiltered', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k;

SET join_algorithm = 'parallel_hash';

SET log_comment = '04411_filtered';
SELECT 'filtered', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k WHERE r.f = 1;

SET log_comment = '04411_unfiltered';
SELECT 'unfiltered', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k;

SYSTEM FLUSH LOGS query_log;

-- A filtered right side (only `k` is uniq-backed) is NOT trusted: it still uses the deferred build,
-- with no streaming preallocation.
SELECT
    'filtered right side still defers',
    countIf(ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables'] > 0) = count(),
    countIf(ProfileEvents['HashJoinPreallocatedElementsInHashTables'] > 0) = 0,
    count() > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = '04411_filtered';

-- Control: the same join without a filter trusts the uniq NDV and preallocates on the streaming path.
SELECT
    'unfiltered right side preallocates',
    countIf(ProfileEvents['HashJoinPreallocatedElementsInHashTables'] BETWEEN 20000 AND 60000) = count(),
    countIf(ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables'] > 0) = 0,
    count() > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = '04411_unfiltered';

DROP TABLE t_build;
DROP TABLE t_probe;
