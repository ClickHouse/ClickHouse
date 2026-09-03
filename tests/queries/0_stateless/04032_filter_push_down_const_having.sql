-- Regression test: filter push-down through aggregation must preserve filter column constness.
-- When an AND expression short-circuits to a constant (e.g., and(LowCardinality(0), 1, aggregate))
-- and parts of it are pushed below aggregation, the remaining expression above must keep the
-- same constness to avoid "Block structure mismatch" or "non constant in source but must be constant" errors.

DROP TABLE IF EXISTS t_const_having;
CREATE TABLE t_const_having (c0 Int32) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_const_having VALUES (1), (2), (3);

-- Minimal reproduction: toLowCardinality constant makes AND short-circuit to Const(0),
-- but after push-down the remaining expression was non-const, causing header mismatch.
SELECT
    -c0 AS g,
    toLowCardinality(toUInt8(0)) AND 1 AND MAX(c0) AS h
FROM t_const_having
GROUP BY GROUPING SETS ((g))
HAVING h;

-- Original fuzzer query (simplified)
SELECT
    -c0 AS g,
    greater(isNull(6), toUInt16(toLowCardinality(toUInt256(6))))
        AND 1
        AND sqrt(MAX(c0) OR MAX(c0)) AS h,
    h IS NULL
FROM t_const_having
GROUP BY GROUPING SETS ((g))
HAVING h
ORDER BY g DESC;

-- After partial push-down of AND, the remaining expression may fold to a constant
-- on 0-row header input (e.g., plus(X, NULL) → const NULL), even though the DAG
-- node has non-const inputs. This caused "Block structure mismatch" when
-- mergeExpressions used a stale non-const header from the parent step.
SELECT
    (MAX(c0) + NULL) AND materialize(0) AS h,
    -c0 AS g
FROM t_const_having
GROUP BY g
HAVING h;

DROP TABLE t_const_having;

-- Regression test for the JOIN path of the same fix (filterPushDown.cpp:711-714).
-- constifyFilterColumnAfterPushDown must also be called after splitActionsForJOINFilterPushDown
-- when the filter was const before push-down but is non-const after (same bug, JOIN code path).
DROP TABLE IF EXISTS t_const_join_left;
DROP TABLE IF EXISTS t_const_join_right;
CREATE TABLE t_const_join_left (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_const_join_right (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_const_join_left VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t_const_join_right VALUES (1, 100), (2, 200), (3, 300);

-- AND short-circuits to const(0); cross-side expression cannot be pushed to either side.
SELECT l.a, l.b, r.b
FROM t_const_join_left AS l
INNER JOIN t_const_join_right AS r ON l.a = r.a
WHERE toLowCardinality(toUInt8(0)) AND 1 AND (l.b + r.b > 0)
ORDER BY l.a;

SELECT l.a, l.b, r.b
FROM t_const_join_left AS l
LEFT JOIN t_const_join_right AS r ON l.a = r.a
WHERE toLowCardinality(toUInt8(0)) AND 1 AND (l.b + r.b > 0)
ORDER BY l.a;

-- AND short-circuits to const(0); single-side expression can be pushed to left.
SELECT l.a, l.b, r.b
FROM t_const_join_left AS l
INNER JOIN t_const_join_right AS r ON l.a = r.a
WHERE toLowCardinality(toUInt8(0)) AND 1 AND l.b > 5
ORDER BY l.a;

DROP TABLE t_const_join_left;
DROP TABLE t_const_join_right;
