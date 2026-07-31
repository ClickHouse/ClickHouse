-- The old (non-analyzer) rewriter duplicates the predicate into ON and keeps the whole WHERE, so it
-- answers correctly and every live row below would be vacuous with enable_analyzer = 0.
-- A session SET also survives `compatibility` randomization, which can flip the analyzer off.
SET enable_analyzer = 1;
-- Keep the sibling optimizer pass `tryMergeFilterIntoJoinCondition` out, so this test measures only
-- CrossToInnerJoinPass.
SET query_plan_enable_optimizations = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
DROP TABLE IF EXISTS m;

CREATE TABLE l (a UInt64, b UInt64) ENGINE = Log;
CREATE TABLE r (a UInt64, b UInt64) ENGINE = Log;
CREATE TABLE m (a UInt64) ENGINE = Log;

INSERT INTO l SELECT number % 16, number % 4 FROM numbers(500);
INSERT INTO r SELECT number % 16, number % 4 FROM numbers(500);
INSERT INTO m SELECT number % 4 FROM numbers(40);

-- The second conjunct is single-side, so it is never extracted into the join and pins the key to 3.
-- Every surviving r.a must therefore be 3. Row counts are not asserted: they are random here.

SELECT '-- comma join';
SELECT uniqExact(r.a) = 1 AND min(r.a) = 3 AND max(r.a) = 3 AND count() > 0
FROM l, r WHERE rand(l.a) % 16 = r.a AND rand(l.a) % 16 = 3
SETTINGS cross_to_inner_join_rewrite = 1;

SELECT '-- explicit CROSS JOIN';
SELECT uniqExact(r.a) = 1 AND min(r.a) = 3 AND max(r.a) = 3 AND count() > 0
FROM l CROSS JOIN r WHERE rand(l.a) % 16 = r.a AND rand(l.a) % 16 = 3
SETTINGS cross_to_inner_join_rewrite = 1;

SELECT '-- non-deterministic on the right side of the equality';
SELECT uniqExact(l.a) = 1 AND min(l.a) = 3 AND max(l.a) = 3 AND count() > 0
FROM l, r WHERE l.a = rand(r.a) % 16 AND 3 = rand(r.a) % 16
SETTINGS cross_to_inner_join_rewrite = 1;

SELECT '-- three tables, one non-deterministic edge';
SELECT uniqExact(r.a) = 1 AND min(r.a) = 3 AND max(r.a) = 3 AND count() > 0
FROM l, r, m WHERE rand(l.a) % 16 = r.a AND rand(l.a) % 16 = 3 AND m.a = l.a % 4
SETTINGS cross_to_inner_join_rewrite = 1;

SELECT '-- generateUUIDv4';
SELECT uniqExact(r.a) = 1 AND min(r.a) = 3 AND max(r.a) = 3 AND count() > 0
FROM l, r
WHERE toUInt64(reinterpretAsUInt128(generateUUIDv4(l.a))) % 16 = r.a
  AND toUInt64(reinterpretAsUInt128(generateUUIDv4(l.a))) % 16 = 3
SETTINGS cross_to_inner_join_rewrite = 1;

SELECT '-- forced rewrite, non-deterministic and deterministic edge';
SELECT uniqExact(r.a) = 1 AND min(r.a) = 3 AND max(r.a) = 3 AND count() > 0
FROM l, r WHERE rand(l.a) % 16 = r.a AND rand(l.a) % 16 = 3 AND l.b = r.b
SETTINGS cross_to_inner_join_rewrite = 2;

-- The rewrite is not attempted for a non-deterministic condition, so with no other equi condition a
-- forced rewrite reports the pre-existing error instead of silently returning wrong results.
SELECT '-- forced rewrite, only a non-deterministic edge';
SELECT count() FROM l, r WHERE rand(l.a) % 16 = r.a
SETTINGS cross_to_inner_join_rewrite = 2; -- { serverError INCORRECT_QUERY }

SELECT '-- forced rewrite, no equi condition at all';
SELECT count() FROM l, r WHERE l.a > r.a
SETTINGS cross_to_inner_join_rewrite = 2; -- { serverError INCORRECT_QUERY }

SELECT '-- deterministic predicate is unaffected';
SELECT uniqExact(r.a) = 1 AND min(r.a) = 3 AND max(r.a) = 3 AND count() > 0
FROM l, r WHERE (l.a * 1) % 16 = r.a AND (l.a * 1) % 16 = 3
SETTINGS cross_to_inner_join_rewrite = 1;

SELECT '-- deterministic predicate is still rewritten';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r WHERE l.a = r.a SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

-- queryID() has one value for the whole query, so it stays eligible even though it reports
-- isDeterministic() = false. This is the row that catches a guard written against isDeterministic()
-- instead of isDeterministicInScopeOfQuery(). now() and currentUser() are constant-folded before
-- this pass runs, so they cannot serve that purpose.
SELECT '-- queryID() is still rewritten';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r
    WHERE concat(toString(l.a), queryID()) = concat(toString(r.a), queryID())
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

SELECT '-- now() is still rewritten';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r WHERE l.a + toUInt64(now()) = r.a SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

SELECT '-- currentUser() is still rewritten';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r
    WHERE concat(toString(l.a), currentUser()) = concat(toString(r.a), currentUser())
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

SELECT '-- rand() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r WHERE rand(l.a) % 16 = r.a SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

SELECT '-- nowInBlock() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r WHERE l.a + toUInt64(nowInBlock()) = r.a
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

DROP TABLE l;
DROP TABLE r;
DROP TABLE m;
