-- Shared `x > 0` predicate from both join sides becomes the same canonical node
-- after dedup; runtime evaluates the greater only once per side

SET query_plan_merge_filters=1; -- required for this optimization, happens on filter merges
SET enable_analyzer = 1;

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

-- Non-deterministic functions in different scopes must NOT be deduped. The inner rand
-- aliased as `x` and the outer rand are separate FUNCTION nodes; merging them would
-- collapse the predicate to `x = x` and return 100 instead of ~0
SELECT 'rand not deduped across scopes', countIf(explain LIKE '%FUNCTION rand()%') = 2
FROM (
    EXPLAIN actions = 1
    SELECT count() FROM (SELECT rand() AS x FROM numbers(100)) WHERE rand() = x
);

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

-- name-sensitive parent over a name-insensitive child, `plus(a,1)` and `plus(b,1)` get
-- merged by value-only dedup, but the formatRow parents must stay distinct (they observe
-- the original child names in their JSON keys)
SELECT count() FROM (
    SELECT number AS a, number AS b,
           formatRowNoNewline('JSONEachRow', plus(a, 1)) AS fa
    FROM numbers(1)
) WHERE formatRowNoNewline('JSONEachRow', plus(b, 1)) != fa;

SELECT 'nested formatRow stays distinct', countIf(explain LIKE '%FUNCTION formatRowNoNewline%') = 2
FROM (
    EXPLAIN actions = 1
    SELECT count() FROM (
        SELECT number AS a, number AS b,
               formatRowNoNewline('JSONEachRow', plus(a, 1)) AS fa
        FROM numbers(1)
    ) WHERE formatRowNoNewline('JSONEachRow', plus(b, 1)) != fa
);

-- Some deterministic functions consume their argument names (`formatRow*` uses them
-- as JSON keys). Looking through aliases would falsely equate two such calls over
-- different aliases of the same input value
SELECT count() FROM (
    SELECT number AS a, number AS b, formatRowNoNewline('JSONEachRow', a) AS fa FROM numbers(1)
) WHERE formatRowNoNewline('JSONEachRow', b) != fa;

-- Two propagated-constant `COLUMN` outputs with the same value but different names
-- (`SELECT 1 AS a, 1 AS b`) must not collapse - a name-sensitive parent would otherwise
-- see the same canonical and produce identical JSON
SELECT * FROM (SELECT 1 AS a, 1 AS b)
WHERE formatRowNoNewline('JSONEachRow', a) != formatRowNoNewline('JSONEachRow', b);
