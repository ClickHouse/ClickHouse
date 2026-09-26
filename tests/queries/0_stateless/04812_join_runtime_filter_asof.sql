-- Runtime filter should be built on the equality key(s) of an ASOF JOIN, skipping the
-- mandatory closest-match (non-equality) condition, same as for mixed equi + non-equi
-- predicates on regular joins (see 04410_join_runtime_filter_mixed_predicates).
-- The filter is an over-approximation: a probe row whose equality key has no match at
-- all on the build side can never satisfy the closest-match condition either, so it is
-- safe to drop such rows before the join. RIGHT ASOF JOIN is not supported by ClickHouse
-- at all (regardless of join algorithm), so only INNER is covered here.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET join_algorithm = 'hash';
SET query_plan_join_swap_table = 0;
SET join_runtime_filter_min_probe_rows = 0;

DROP TABLE IF EXISTS t_rf_asof_left;
DROP TABLE IF EXISTS t_rf_asof_right;

CREATE TABLE t_rf_asof_left (id UInt64, ts UInt64, val UInt64) ENGINE = MergeTree ORDER BY (id, ts);
CREATE TABLE t_rf_asof_right (id UInt64, ts UInt64, val UInt64) ENGINE = MergeTree ORDER BY (id, ts);

-- id=1 and id=3 have matches on the right side; id=2 and id=4 have no right-side rows at
-- all, so the runtime filter on `id` should drop them before the ASOF closest-match check.
-- id=1, ts=1 is the negative `ASOF` case: `id` has build-side rows (ts=5, ts=15), but every one
-- of them fails `l.ts >= r.ts`, so this row must still be dropped by the join itself. Since the
-- runtime filter only checks the equality key `id` (not the closest-match predicate), it lets this
-- row through to the join unchanged; it is the join's own `ASOF` matching, not the filter, that
-- excludes it. This proves the filter stays an over-approximation rather than a semantics-changing
-- prefilter.
INSERT INTO t_rf_asof_left VALUES (1, 1, 999), (1, 10, 100), (1, 20, 200), (2, 5, 50), (3, 15, 150), (4, 8, 80);
INSERT INTO t_rf_asof_right VALUES (1, 5, 1000), (1, 15, 1001), (3, 10, 3000), (3, 20, 3001);

SELECT '--- ASOF INNER JOIN: same result without and with runtime filter ---';

SELECT l.id, l.ts, r.ts, r.val
FROM t_rf_asof_left l ASOF INNER JOIN t_rf_asof_right r ON l.id = r.id AND l.ts >= r.ts
ORDER BY l.id, l.ts
SETTINGS enable_join_runtime_filters = 0;

SELECT l.id, l.ts, r.ts, r.val
FROM t_rf_asof_left l ASOF INNER JOIN t_rf_asof_right r ON l.id = r.id AND l.ts >= r.ts
ORDER BY l.id, l.ts
SETTINGS enable_join_runtime_filters = 1;

SELECT '--- runtime filter IS applied to equality key for ASOF INNER JOIN ---';

SELECT replaceRegexpOne(explain, '^[\\s└├─│]+', '')
FROM (
    EXPLAIN PLAN
    SELECT l.id, l.ts, r.ts, r.val
    FROM t_rf_asof_left l ASOF INNER JOIN t_rf_asof_right r ON l.id = r.id AND l.ts >= r.ts
    ORDER BY l.id, l.ts
    SETTINGS enable_join_runtime_filters = 1
)
WHERE explain LIKE '%Build runtime join filter%';

DROP TABLE t_rf_asof_left;
DROP TABLE t_rf_asof_right;
