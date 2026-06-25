-- Tags: no-random-settings, no-random-merge-tree-settings

-- Follow-up to #108129: the planner NDV shortcut that skips the cold deferred parallel_hash build must
-- trust only a real `uniq` statistic of a *base* relation. When the build (right) input of a join is
-- itself a join sub-tree, its key NDV is an estimate (the join-order DP clamps equi-key NDVs to
-- min(left, right) and caps every NDV to the join's estimated output rows), even if a uniq-provenance
-- flag survived. Such a build must fall back to the deferred (HLL-sized) build, not the streaming
-- preallocation shortcut.
--
-- The build side of the outer join here is the sub-join (t_b JOIN t_c). The outer join must use the
-- deferred build; results must match a plain `hash` join. `query_plan_join_swap_table = 'false'` keeps
-- the sub-join as the build side of the outer join.

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

DROP TABLE IF EXISTS t_small;
DROP TABLE IF EXISTS t_b;
DROP TABLE IF EXISTS t_c;

CREATE TABLE t_small (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_b (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_c (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();

-- Probe side of the outer join: only 10 distinct keys.
INSERT INTO t_small SELECT number % 10 AS k, number AS v FROM numbers(1000);
-- Build sub-join inputs: ~30000 distinct keys each.
INSERT INTO t_b SELECT number % 30000 AS k, number * 3 AS v FROM numbers(300000);
INSERT INTO t_c SELECT number % 30000 AS k, number * 7 AS v FROM numbers(300000);

ALTER TABLE t_small MATERIALIZE STATISTICS k;
ALTER TABLE t_b MATERIALIZE STATISTICS k;
ALTER TABLE t_c MATERIALIZE STATISTICS k;

-- Reference: plain hash join.
SET join_algorithm = 'hash';
SELECT 'multijoin', count(), sum(cityHash64(s.k, s.v, r.v, r.cv))
FROM t_small s
INNER JOIN (SELECT b.k AS k, b.v AS v, c.v AS cv FROM t_b b INNER JOIN t_c c ON b.k = c.k) r
ON s.k = r.k;

SET join_algorithm = 'parallel_hash';
SET log_comment = '04412_multijoin';
SELECT 'multijoin', count(), sum(cityHash64(s.k, s.v, r.v, r.cv))
FROM t_small s
INNER JOIN (SELECT b.k AS k, b.v AS v, c.v AS cv FROM t_b b INNER JOIN t_c c ON b.k = c.k) r
ON s.k = r.k;

SYSTEM FLUSH LOGS query_log;

-- The outer join's build input is a join sub-tree, so its key NDV is not a trustworthy base-relation
-- uniq count: the outer join must use the deferred (HLL) build. Without the fix, a leaked
-- output-capped NDV would have driven a streaming preallocation for it instead.
SELECT
    'multijoin right build defers',
    countIf(ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables'] > 0) = count(),
    count() > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = '04412_multijoin';

DROP TABLE t_small;
DROP TABLE t_b;
DROP TABLE t_c;
