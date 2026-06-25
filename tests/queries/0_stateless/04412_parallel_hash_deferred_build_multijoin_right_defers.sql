-- Tags: no-random-settings, no-random-merge-tree-settings

-- Follow-up to #108129: the planner NDV shortcut that skips the cold deferred parallel_hash build must
-- trust only a real `uniq` statistic of a *base* relation. When the build (right) input of a join is
-- itself a join sub-tree, its key NDV is an estimate (the join-order DP clamps equi-key NDVs to
-- min(left, right) and caps every NDV to the join's estimated output rows), even if a uniq-provenance
-- flag survived. Such a build must fall back to the deferred (HLL-sized) build, not the streaming
-- preallocation shortcut, so that an estimated/clamped NDV never sizes the build map.
--
-- A flat three-way join is used (single join-order DP). The tiny `s` table pairs with `b` into the
-- build sub-tree of the outer join, whose build (right) input is therefore the sub-join `(b JOIN s)`
-- rather than a base relation; `query_plan_join_swap_table = 'true'` keeps that shape. The outer join
-- must use the deferred build, and results must match a plain `hash` join. Verified discriminating:
-- before the fix the outer join trusts the sub-join's key NDV and preallocates (does not defer).

SET allow_experimental_statistics = 1;
SET allow_statistics_optimize = 1;
SET mutations_sync = 2; -- MATERIALIZE STATISTICS must finish before the joins read the uniq stats
SET enable_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 'true'; -- keep the build sub-tree on the build side of the outer join
SET collect_hash_table_stats_during_joins = 0; -- no cross-run cache hint, so the plan NDV is consulted
SET parallel_hash_join_threshold = 0; -- force ConcurrentHashJoin regardless of build size
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_s;
DROP TABLE IF EXISTS t_b;
DROP TABLE IF EXISTS t_c;

CREATE TABLE t_s (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_b (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_c (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();

-- Tiny table (10 distinct keys) that pairs into the build sub-tree; b and c have ~30000 distinct keys.
INSERT INTO t_s SELECT number % 10 AS k, number * 3 AS v FROM numbers(1000);
INSERT INTO t_b SELECT number % 30000 AS k, number * 5 AS v FROM numbers(300000);
INSERT INTO t_c SELECT number % 30000 AS k, number * 7 AS v FROM numbers(300000);

ALTER TABLE t_s MATERIALIZE STATISTICS k;
ALTER TABLE t_b MATERIALIZE STATISTICS k;
ALTER TABLE t_c MATERIALIZE STATISTICS k;

-- Reference: plain hash join.
SET join_algorithm = 'hash';
SELECT 'multijoin', count(), sum(cityHash64(s.k, s.v, b.v, c.v))
FROM t_s s INNER JOIN t_b b ON s.k = b.k INNER JOIN t_c c ON b.k = c.k;

SET join_algorithm = 'parallel_hash';
SET log_comment = '04412_multijoin';
SELECT 'multijoin', count(), sum(cityHash64(s.k, s.v, b.v, c.v))
FROM t_s s INNER JOIN t_b b ON s.k = b.k INNER JOIN t_c c ON b.k = c.k;

SET log_comment = ''; -- so the assertion SELECTs below are not themselves matched by the filter
SYSTEM FLUSH LOGS query_log;

-- The outer join's build input is a join sub-tree, so its key NDV is not a trustworthy base-relation
-- uniq count: the outer join uses the deferred (HLL) build. Without the fix, the sub-tree's clamped key
-- NDV would be trusted and drive a streaming preallocation instead (no deferred event).
SELECT
    'multijoin right build defers',
    countIf(ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables'] > 0) = count(),
    count() > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = '04412_multijoin';

DROP TABLE t_s;
DROP TABLE t_b;
DROP TABLE t_c;
