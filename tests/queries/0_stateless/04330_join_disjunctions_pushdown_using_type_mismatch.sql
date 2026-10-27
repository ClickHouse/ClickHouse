-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET use_join_disjunctions_push_down = 1;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t04330;
DROP TABLE IF EXISTS nr04330;

CREATE TABLE nr04330 (x Nullable(UInt32), s Nullable(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO nr04330 VALUES (2, NULL);

CREATE TABLE t04330 (x UInt32, s LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04330 VALUES (1, 'l');

-- The USING key x is UInt32 on the left and Nullable(UInt32) on the right, so the join
-- output column is widened to Nullable. A predicate over x is built against that widened
-- type, but the disjunction (partial predicate) push-down used to apply it to the
-- non-widened input column, tripping a result-type check inside FilterStep:
--   "Unexpected return type from equals. Expected Nullable(UInt8). Got UInt8"
-- The query must plan and execute without a logical error.
SELECT t04330.x
FROM t04330 AS l
SEMI LEFT JOIN nr04330 AS r USING (x)
WHERE and(or(and(and(equals(or(divide(NULL, '922337203685477580.7'), toString(NULL, NULL)), r.s), equals(r.s, '127.0.0.1')), equals(t04330.x, toLowCardinality(-9223372036854775808))), greater(l.s, toString(NULL, 0.0001))), greater(r.s, toString(10.0001, NULL)))
QUALIFY 1024
LIMIT 0;

DROP TABLE t04330;
DROP TABLE nr04330;

DROP TABLE IF EXISTS l04330;
DROP TABLE IF EXISTS r04330;

CREATE TABLE l04330 (a UInt32, x String) ENGINE = Memory;
CREATE TABLE r04330 (a UInt32, y String) ENGINE = Memory;
INSERT INTO l04330 VALUES (1, 'FRANCE'), (2, 'GERMANY');
INSERT INTO r04330 VALUES (1, 'GERMANY'), (2, 'FRANCE');

-- Disjunction push-down must still fire for columns whose type is stable across the join.
-- The plan assertion proves it: with push-down enabled the per-side pre-join filters
-- (one over the left x, one over the right y) are added in addition to the outer
-- disjunction filter; with push-down disabled only the outer filter remains.
-- enable_join_runtime_filters=0 keeps the plan free of non-deterministic runtime-filter ids.
SELECT '--- plan (enabled) ---';
SELECT trimLeft(explain)
FROM (
    EXPLAIN actions = 1
    SELECT l.x, r.y
    FROM l04330 AS l
    INNER JOIN r04330 AS r USING (a)
    WHERE (l.x = 'FRANCE' AND r.y = 'GERMANY') OR (l.x = 'GERMANY' AND r.y = 'FRANCE')
    ORDER BY l.x
)
WHERE explain ILIKE '%Filter column: %' SETTINGS use_join_disjunctions_push_down = 1, enable_join_runtime_filters = 0, enable_parallel_replicas = 0, query_plan_optimize_join_order_randomize = 0 -- Pinned because the test asserts on join plan/order
FORMAT TSV;

SELECT '--- plan (disabled) ---';
SELECT trimLeft(explain)
FROM (
    EXPLAIN actions = 1
    SELECT l.x, r.y
    FROM l04330 AS l
    INNER JOIN r04330 AS r USING (a)
    WHERE (l.x = 'FRANCE' AND r.y = 'GERMANY') OR (l.x = 'GERMANY' AND r.y = 'FRANCE')
    ORDER BY l.x
)
WHERE explain ILIKE '%Filter column: %' SETTINGS use_join_disjunctions_push_down = 0, enable_join_runtime_filters = 0, enable_parallel_replicas = 0, query_plan_optimize_join_order_randomize = 0 -- Pinned because the test asserts on join plan/order
FORMAT TSV;

-- Result must be identical with push-down enabled (sanity check that the optimization is result-preserving).
SELECT '--- result (enabled) ---';
SELECT l.x, r.y
FROM l04330 AS l
INNER JOIN r04330 AS r USING (a)
WHERE (l.x = 'FRANCE' AND r.y = 'GERMANY') OR (l.x = 'GERMANY' AND r.y = 'FRANCE')
ORDER BY l.x
FORMAT TSV;

-- Analyzer identifier-resolution crash (NULL deref) reachable from this query family.
-- scope.join_using_columns holds raw pointers to a stack-local map pushed while resolving
-- one JOIN side; a statically-dead if/multiIf branch that references an unknown identifier
-- throws UNKNOWN_IDENTIFIER, which the constant-fold path swallows. The push was not undone
-- on that throw, so a later identifier lookup dereferenced a dangling pointer. Must not crash.
SELECT '--- analyzer no crash ---';
SELECT count() > 0
FROM viewExplain('EXPLAIN', 'actions = 1', (
    SELECT DISTINCT l.x, r.y
    FROM l04330 AS l
    SEMI LEFT JOIN r04330 AS r USING (a)
    WHERE xor(toLowCardinality(100), '1' = l.x, multiIf(toString(NULL),
        plus(if(and(divide('9', NULL), toString(NULL, NULL)),
            isNull(toFixedString('9', t04330.x, 20)), notEquals('9', r.s)), 0),
        and(toString(NULL, NULL), divide(NULL, ';'))))
    ORDER BY l.x DESC NULLS FIRST
))
SETTINGS use_join_disjunctions_push_down = 1, enable_join_runtime_filters = 0, enable_parallel_replicas = 0
FORMAT TSV;

DROP TABLE l04330;
DROP TABLE r04330;
