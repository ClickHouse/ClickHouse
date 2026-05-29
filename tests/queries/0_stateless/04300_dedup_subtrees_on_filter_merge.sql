-- Shared `x > 0` predicate from both join sides becomes the same canonical node
-- after dedup; runtime evaluates the greater only once per side

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

-- Different argument order keeps predicates as separate nodes; result still correct
SELECT count() FROM (SELECT 1 AS x WHERE x = 1 AND 1 = x);

-- Idempotence stays correct: AND of duplicate children still evaluates AND but on
-- the same canonical column; query result must be unchanged
SELECT count() FROM numbers(100) WHERE number > 0 AND number > 0;

-- Non-deterministic functions must NOT be deduped (two calls give different values)
SELECT count() FROM numbers(100) WHERE rand() % 256 < 256 AND rand() % 256 < 256;

-- Nested subquery with same predicate on different alias chains. Without alias
-- look-through (dropped because some functions like `formatRow` consume names),
-- the two equals stay distinct; only the shared Const(10) dedups
DROP TABLE IF EXISTS u_4263;
CREATE TABLE u_4263 (uid Int16) ENGINE = MergeTree ORDER BY uid;
INSERT INTO u_4263 VALUES (10), (20);

SELECT count() FROM (SELECT * FROM u_4263 WHERE uid = 10) WHERE uid = 10;

SELECT 'equals + const dedup',
       countIf(explain LIKE '%FUNCTION equals(uid%'),
       countIf(explain LIKE '%COLUMN Const(UInt8) -> 10_UInt8%')
FROM (
    EXPLAIN actions = 1
    SELECT * FROM (SELECT * FROM u_4263 WHERE uid = 10) WHERE uid = 10
    SETTINGS optimize_move_to_prewhere = 0
);

DROP TABLE u_4263;

-- and/or are order-sensitive under short_circuit; dedup must NOT canonicalize them.
-- Outer `or(intDiv(1, n) > 0, n = 0)` evaluates intDiv first → throws on n=0
SELECT count() FROM (
    SELECT number FROM numbers(3) WHERE or(number = 0, intDiv(1, number) > 0)
) WHERE or(intDiv(1, number) > 0, number = 0)
SETTINGS short_circuit_function_evaluation = 'enable'; -- { serverError ILLEGAL_DIVISION }

-- Output name preservation: a projected output named `f` and the filter predicate share
-- structure. Dedup must not rewrite the filter output to `f` (would lose its name)
SELECT * FROM (SELECT number, equals(number, 0) AS f FROM numbers(3))
WHERE equals(number, 0) ORDER BY number;

-- IN-set constants are backed by ColumnSet (a dummy column whose `get` throws).
-- Dedup must skip them instead of calling get and exploding during optimization
SELECT count() FROM (
    SELECT number FROM numbers(10) WHERE number IN (1, 2, 3, 4)
) WHERE number IN (3, 4, 5, 6);

-- FunctionCapture identity is its inner lambda DAG, not its name/children.
-- Two arrayMap with different lambdas but same signature must not collapse.
-- Outer filter uses `x + 2`; correct answer is `number = 3` (3 + 2 = 5)
SELECT number FROM (
    SELECT number, arrayMap(x -> x + 1, [number])[1] AS a FROM numbers(5)
) WHERE arrayMap(x -> x + 2, [number])[1] = 5;

-- Globally non-deterministic functions (rand, randConstant) must not be deduped.
-- If the outer `rand()` were merged with inner `x`, the predicate would collapse to
-- `x = x` and return 100; correct behavior is two independent rands → ~0 matches
SELECT count() FROM (SELECT rand() AS x FROM numbers(100)) WHERE rand() = x;

-- Some deterministic functions consume their argument names (`formatRow*` uses them
-- as JSON keys). Looking through aliases would falsely equate two such calls over
-- different aliases of the same input value
SELECT count() FROM (
    SELECT number AS a, number AS b, formatRowNoNewline('JSONEachRow', a) AS fa FROM numbers(1)
) WHERE formatRowNoNewline('JSONEachRow', b) != fa;
