-- Runtime filter should be built on equality keys even when the ON clause also
-- contains non-equality predicates (e.g. range conditions).
-- The filter is an over-approximation: rows that pass the Bloom filter on the
-- equality key but fail the range condition are rejected at join time.

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0;
SET join_algorithm = 'hash';
SET query_plan_join_swap_table = 0;

DROP TABLE IF EXISTS t_rf_mixed_left;
DROP TABLE IF EXISTS t_rf_mixed_right;

CREATE TABLE t_rf_mixed_left (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_rf_mixed_right (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;

-- id=1: 10 < 15 -> match; id=2: 20 < 10 -> no match; id=3: 30 < 35 -> match; id=4: 40 < 38 -> no match
INSERT INTO t_rf_mixed_left VALUES (1, 10), (2, 20), (3, 30), (4, 40);
INSERT INTO t_rf_mixed_right VALUES (1, 15), (2, 10), (3, 35), (4, 38);

SELECT '--- INNER JOIN: same result without and with runtime filter ---';

SELECT l.id, l.val, r.val
FROM t_rf_mixed_left l JOIN t_rf_mixed_right r ON l.id = r.id AND l.val < r.val
ORDER BY l.id
SETTINGS enable_join_runtime_filters = 0;

SELECT l.id, l.val, r.val
FROM t_rf_mixed_left l JOIN t_rf_mixed_right r ON l.id = r.id AND l.val < r.val
ORDER BY l.id
SETTINGS enable_join_runtime_filters = 1;

SELECT '--- runtime filter IS applied to equality key when filters enabled ---';

SELECT trimLeft(explain)
FROM (
    EXPLAIN PLAN
    SELECT l.id, l.val, r.val
    FROM t_rf_mixed_left l JOIN t_rf_mixed_right r ON l.id = r.id AND l.val < r.val
    ORDER BY l.id
    SETTINGS enable_join_runtime_filters = 1
)
WHERE explain LIKE '%Build runtime join filter%';

SELECT '--- LEFT SEMI JOIN: same result without and with runtime filter ---';

SELECT l.id, l.val
FROM t_rf_mixed_left l LEFT SEMI JOIN t_rf_mixed_right r ON l.id = r.id AND l.val < r.val
ORDER BY l.id
SETTINGS enable_join_runtime_filters = 0;

SELECT l.id, l.val
FROM t_rf_mixed_left l LEFT SEMI JOIN t_rf_mixed_right r ON l.id = r.id AND l.val < r.val
ORDER BY l.id
SETTINGS enable_join_runtime_filters = 1;

SELECT '--- LEFT ANTI JOIN with mixed predicates: runtime filter disabled (correct) ---';

-- For LEFT ANTI JOIN the NOT IN filter would be over-broad with non-equality conditions,
-- so the runtime filter must not be applied. Verify results are still correct.
SELECT l.id, l.val
FROM t_rf_mixed_left l LEFT ANTI JOIN t_rf_mixed_right r ON l.id = r.id AND l.val < r.val
ORDER BY l.id
SETTINGS enable_join_runtime_filters = 0;

SELECT l.id, l.val
FROM t_rf_mixed_left l LEFT ANTI JOIN t_rf_mixed_right r ON l.id = r.id AND l.val < r.val
ORDER BY l.id
SETTINGS enable_join_runtime_filters = 1;

DROP TABLE t_rf_mixed_left;
DROP TABLE t_rf_mixed_right;
