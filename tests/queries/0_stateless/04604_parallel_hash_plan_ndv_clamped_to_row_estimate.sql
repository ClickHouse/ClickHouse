-- Tags: no-random-settings, no-random-merge-tree-settings
-- The regression guards below assert tight preallocation ranges, which depend on the planner
-- consulting the uniq statistics; the settings randomizer perturbs the statistics toggles and the
-- join-order path through unpinned side channels (see 04407_parallel_hash_deferred_build_plan_ndv.sql
-- for the same class of test).
--
-- Follow-up to #108129 (review): the trustworthy (uniq-backed) right-key distinct-count hint is
-- clamped to the right-side row estimate. Column statistics survive row-truncating steps (`LimitStep`,
-- `SortingStep` with a limit) that cap the row estimate only, so without the clamp a LIMIT-ed build
-- side would preallocate the hash maps to the full-relation `uniq` count (here: 30000 entries for a
-- 100-row build side).

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

DROP TABLE IF EXISTS t_build_clamp;
DROP TABLE IF EXISTS t_probe_clamp;

CREATE TABLE t_build_clamp (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe_clamp (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();

-- 30000 distinct keys, each repeated 10 times = 300000 source rows.
INSERT INTO t_build_clamp SELECT number % 30000 AS k, number * 3 AS v FROM numbers(300000);
INSERT INTO t_probe_clamp SELECT number % 30000 AS k, number * 7 AS v FROM numbers(300000);

ALTER TABLE t_build_clamp MATERIALIZE STATISTICS k;
ALTER TABLE t_probe_clamp MATERIALIZE STATISTICS k;

-- Reference results: plain hash join. Each LIMIT-ed build row matches its key's 10 probe rows, so the
-- counts do not depend on which 100 rows the limit picks.
SET join_algorithm = 'hash';
SELECT 'nolimit', count() FROM t_probe_clamp l INNER JOIN t_build_clamp r ON l.k = r.k;
SELECT 'limit', count() FROM t_probe_clamp l INNER JOIN (SELECT k, v FROM t_build_clamp LIMIT 100) r ON l.k = r.k;
SELECT 'orderby_limit', count() FROM t_probe_clamp l INNER JOIN (SELECT k, v FROM t_build_clamp ORDER BY v LIMIT 100) r ON l.k = r.k;

SET join_algorithm = 'parallel_hash';

SET log_comment = '04604_nolimit';
SELECT 'nolimit', count() FROM t_probe_clamp l INNER JOIN t_build_clamp r ON l.k = r.k;

SET log_comment = '04604_limit';
SELECT 'limit', count() FROM t_probe_clamp l INNER JOIN (SELECT k, v FROM t_build_clamp LIMIT 100) r ON l.k = r.k;

SET log_comment = '04604_orderby_limit';
SELECT 'orderby_limit', count() FROM t_probe_clamp l INNER JOIN (SELECT k, v FROM t_build_clamp ORDER BY v LIMIT 100) r ON l.k = r.k;

SYSTEM FLUSH LOGS query_log;

-- The unlimited build side still preallocates ~NDV (30000) on the streaming path.
SELECT
    'nolimit preallocates ~NDV',
    countIf(ProfileEvents['HashJoinPreallocatedElementsInHashTables'] BETWEEN 20000 AND 60000) = count(),
    countIf(ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables'] > 0) = 0,
    count() > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = '04604_nolimit';

-- A LIMIT-ed build side clamps the hint to the row estimate: ~limit (100), not the full-table NDV.
SELECT
    'limited build side clamps prealloc to ~limit',
    countIf(ProfileEvents['HashJoinPreallocatedElementsInHashTables'] BETWEEN 1 AND 1000) = count(),
    countIf(ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables'] > 0) = 0,
    count() = 2
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment IN ('04604_limit', '04604_orderby_limit');

DROP TABLE t_build_clamp;
DROP TABLE t_probe_clamp;
