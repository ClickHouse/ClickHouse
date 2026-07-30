-- Tags: no-random-settings, no-random-merge-tree-settings

-- Follow-up to #108129: the planner NDV shortcut that skips the cold deferred parallel_hash build
-- (`extractTrustworthyRightKeyNdv`) trusts a build-relation key NDV only while it is genuinely a
-- `uniq`-backed distinct count. In the filtered relation profile the value is
-- `min(estimated_filtered_rows, uniq)`; when the estimated row count clamps it (estimated rows <= uniq,
-- e.g. from default selectivity factors of a predicate on an un-analyzed column), the value is no longer
-- a `uniq` count, so such a build must fall back to the deferred (HLL-sized) build rather than the
-- streaming preallocation shortcut. (Conversely, a filter that keeps more rows than there are distinct
-- keys leaves the value equal to the real `uniq` and is still trusted -- otherwise a benign key-range
-- filter would needlessly lose the warm preallocation.)
--
-- The build key `k` is unique (uniq ~= rows) and the filter is on `f`, which has no statistics. The
-- estimated post-filter row count therefore falls below the key uniq, clamping it, so the filtered join
-- must defer; the otherwise-identical unfiltered join uses the shortcut. Both must match a plain `hash`
-- join.

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

-- `k` is unique with a uniq statistic; `f` (the filter column) has no statistics.
CREATE TABLE t_build (k UInt64 STATISTICS(uniq), f UInt8, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_build SELECT number AS k, number % 2 AS f, number * 3 AS v FROM numbers(100000);
INSERT INTO t_probe SELECT number AS k, number * 7 AS v FROM numbers(100000);

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

SET log_comment = ''; -- so the assertion SELECTs below are not themselves matched by the filter
SYSTEM FLUSH LOGS query_log;

-- The filtered right side's key NDV is clamped by the estimated row count, so it is NOT trusted: the
-- join uses the deferred (HLL) build, with no streaming preallocation.
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

-- Control: the same join without a filter keeps the real uniq NDV and preallocates on the streaming path.
SELECT
    'unfiltered right side preallocates',
    countIf(ProfileEvents['HashJoinPreallocatedElementsInHashTables'] BETWEEN 50000 AND 200000) = count(),
    countIf(ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables'] > 0) = 0,
    count() > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = '04411_unfiltered';

DROP TABLE t_build;
DROP TABLE t_probe;
