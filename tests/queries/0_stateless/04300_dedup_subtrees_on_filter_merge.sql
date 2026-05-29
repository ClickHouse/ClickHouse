SELECT count() FROM numbers(10000) AS l
INNER JOIN (SELECT number FROM numbers(10000)) AS r
ON l.number = r.number AND l.number > 0 AND r.number > 0
SETTINGS enable_join_runtime_filters = 0;

SELECT 'dedup hits',
       countIf(explain LIKE '%FUNCTION greater(number%-> greater(__table2.number, 0_UInt8)%')
FROM (
    EXPLAIN actions = 1
    SELECT number FROM numbers(10000) AS l
    INNER JOIN (SELECT number FROM numbers(10000)) AS r
    ON l.number = r.number AND l.number > 0 AND r.number > 0
    SETTINGS enable_join_runtime_filters = 0
);

SELECT count() FROM (SELECT 1 AS x WHERE x = 1 AND 1 = x);

SELECT count() FROM numbers(100) WHERE number > 0 AND number > 0;

SELECT count() FROM numbers(100) WHERE rand() % 256 < 256 AND rand() % 256 < 256;

-- same predicate appears with the inner alias and the outer alias
-- ALIAS look-through during keying collapses them despite different alias chains
DROP TABLE IF EXISTS u_4263;
CREATE TABLE u_4263 (uid Int16) ENGINE = MergeTree ORDER BY uid;
INSERT INTO u_4263 VALUES (10), (20);

SELECT count() FROM (SELECT * FROM u_4263 WHERE uid = 10) WHERE uid = 10;

SELECT 'one equals + one const',
       countIf(explain LIKE '%FUNCTION equals(uid%'),
       countIf(explain LIKE '%COLUMN Const(UInt8) -> 10_UInt8%')
FROM (
    EXPLAIN actions = 1
    SELECT * FROM (SELECT * FROM u_4263 WHERE uid = 10) WHERE uid = 10
    SETTINGS optimize_move_to_prewhere = 0
);

DROP TABLE u_4263;

-- and/or are order-sensitive under short_circuit, dedup must NOT canonicalize them
-- Outer `or(intDiv(1, n) > 0, n = 0)` evaluates intDiv first -> throws on n=0
SELECT count() FROM (
    SELECT number FROM numbers(3) WHERE or(number = 0, intDiv(1, number) > 0)
) WHERE or(intDiv(1, number) > 0, number = 0)
SETTINGS short_circuit_function_evaluation = 'enable'; -- { serverError ILLEGAL_DIVISION }

-- projected output named f and the filter predicate share structure
-- dedup must not rewrite the filter output to f (would lose its name)
SELECT * FROM (SELECT number, equals(number, 0) AS f FROM numbers(3))
WHERE equals(number, 0) ORDER BY number;

-- IN-set constants are backed by ColumnSet (a dummy column whose `get` throws) dedup must skip them
SELECT count() FROM (
    SELECT number FROM numbers(10) WHERE number IN (1, 2, 3, 4)
) WHERE number IN (3, 4, 5, 6);

-- Two arrayMap with different lambdas but same signature must not collapse
-- Outer filter uses `x + 2`; correct answer is `number = 3` (3 + 2 = 5)
SELECT number FROM (
    SELECT number, arrayMap(x -> x + 1, [number])[1] AS a FROM numbers(5)
) WHERE arrayMap(x -> x + 2, [number])[1] = 5;

-- Outer `randConstant() = x` should not merge with inner `randConstant()`
SELECT count() FROM (SELECT randConstant() AS x FROM numbers(100))
WHERE randConstant() = x AND x != x; -- second clause ensures: even if randConstants collide by chance, the row count is 0
