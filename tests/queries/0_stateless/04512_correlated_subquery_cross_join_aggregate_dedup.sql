-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS outer_04512;
DROP TABLE IF EXISTS inner_04512;

CREATE TABLE outer_04512 (id UInt32) ENGINE = Memory;
INSERT INTO outer_04512 VALUES (0), (1), (2);

CREATE TABLE inner_04512 (id UInt32, val String, nz UInt32) ENGINE = Memory;
INSERT INTO inner_04512 VALUES (0, 'a', 100);

-- The correlated aggregate must not be inflated by duplicate outer rows produced
-- by the CROSS JOIN. Both substitution modes must agree with the non-cross-join result.

SELECT '-- substitute=0, no cross join --';
SET correlated_subqueries_substitute_equivalent_expressions = 0;
SELECT o.id, (SELECT count() FROM inner_04512 i WHERE i.id = o.id) AS c
FROM outer_04512 o
ORDER BY o.id;

SELECT '-- substitute=0, cross join (count must match above) --';
SELECT o.id, n.number, (SELECT count() FROM inner_04512 i WHERE i.id = o.id) AS c
FROM outer_04512 o CROSS JOIN numbers(3) n
ORDER BY o.id, n.number;

-- sum over a NON-ZERO column: summing inner_04512.id here would sum zeros only
-- (the single matching inner row has id = 0), so the row would pass even if the
-- sum half of the fix regressed.
SELECT '-- substitute=0, cross join, sum --';
SELECT o.id, n.number, (SELECT sum(nz) FROM inner_04512 i WHERE i.id = o.id) AS s
FROM outer_04512 o CROSS JOIN numbers(3) n
ORDER BY o.id, n.number;

SELECT '-- substitute=1, cross join (count must match) --';
SET correlated_subqueries_substitute_equivalent_expressions = 1;
SELECT o.id, n.number, (SELECT count() FROM inner_04512 i WHERE i.id = o.id) AS c
FROM outer_04512 o CROSS JOIN numbers(3) n
ORDER BY o.id, n.number;

SELECT '-- exists must not be affected by duplicates --';
SET correlated_subqueries_substitute_equivalent_expressions = 0;
SELECT o.id, n.number, EXISTS(SELECT 1 FROM inner_04512 i WHERE i.id = o.id) AS e
FROM outer_04512 o CROSS JOIN numbers(2) n
ORDER BY o.id, n.number;

-- The internal deduplication of the decorrelation domain must NOT inherit the user's DISTINCT
-- size limits. Since the step runs unbounded, all correlated keys are still evaluated and the
-- query succeeds. Use overflow mode 'throw' rather than 'break': DistinctTransform checks the
-- limit only after inserting a whole chunk and still emits every new row from that chunk, so with
-- 'break' a domain that arrives in a single chunk is never actually truncated and the assertion
-- would pass even if the step inherited the user's limits.
INSERT INTO inner_04512 VALUES (1, 'b', 200), (2, 'c', 300);
SELECT '-- internal distinct must ignore user distinct limits --';
SET max_rows_in_distinct = 1;
SET distinct_overflow_mode = 'throw';
SELECT o.id, (SELECT count() FROM inner_04512 i WHERE i.id = o.id) AS c
FROM outer_04512 o CROSS JOIN numbers(3) n
GROUP BY o.id
ORDER BY o.id;
SET max_rows_in_distinct = 0;

-- Duplicate correlated values do not require a join at all: the outer table itself can hold them.
-- Reported separately as issue #112529 with a SUM over a subquery that has its own GROUP BY.
DROP TABLE IF EXISTS lk0_04512;
DROP TABLE IF EXISTS lk1_04512;
CREATE TABLE lk0_04512 (id UInt32, c0 Nullable(Int32)) ENGINE = Memory;
CREATE TABLE lk1_04512 (id UInt32, c0 Nullable(Int32), c1 Nullable(Int32)) ENGINE = Memory;
INSERT INTO lk1_04512 VALUES (1, 10, 1000), (2, 10, 3000), (3, 20, 9000), (4, 20, 9500);
INSERT INTO lk0_04512 VALUES (1, 10), (2, 10);

-- The reported shape groups by c1 as well, so the subquery returns one row per distinct c1 and
-- which one the scalar subquery picks is arbitrary. Assert that the value is one of the two valid
-- per-group sums instead of pinning one of them: without the deduplication every candidate is
-- inflated out of that domain (2000 or 6000), so the assertion still fails. The comparison is
-- wrapped in a subquery because a correlated subquery cannot yet be an IN argument.
SELECT '-- duplicate correlated values in the outer table, no join, subquery GROUP BY --';
SELECT c0, sq IN (1000, 3000) AS sum_is_a_valid_group FROM
(
    SELECT id, c0, (SELECT SUM(lk1_04512.c1) FROM lk1_04512 WHERE lk1_04512.c0 = lk0_04512.c0 GROUP BY lk1_04512.c0, lk1_04512.c1) AS sq
    FROM lk0_04512
)
ORDER BY id;

SELECT '-- same shape without GROUP BY: a plain scalar SUM inflates too --';
SELECT c0, (SELECT SUM(lk1_04512.c1) FROM lk1_04512 WHERE lk1_04512.c0 = lk0_04512.c0) AS sq
FROM lk0_04512
ORDER BY id;

-- A SEMI / ANTI join-back does NOT make the deduplication redundant: when the subquery body
-- aggregates, an inflated domain changes the aggregate and therefore flips the EXISTS predicate
-- that the SEMI / ANTI join evaluates. Without the deduplication these return every outer row
-- (count() inflates 2 -> 4, so HAVING count() = 2 is never satisfied).
SELECT '-- WHERE EXISTS with an aggregate body (SEMI join-back) --';
SELECT id, c0 FROM lk0_04512
WHERE EXISTS (SELECT 1 FROM lk1_04512 WHERE lk1_04512.c0 = lk0_04512.c0 HAVING count() = 2)
ORDER BY id;

SELECT '-- WHERE NOT EXISTS with an aggregate body (ANTI join-back) --';
SELECT id, c0 FROM lk0_04512
WHERE NOT EXISTS (SELECT 1 FROM lk1_04512 WHERE lk1_04512.c0 = lk0_04512.c0 HAVING count() = 2)
ORDER BY id;

-- Duplicate-insensitive aggregates were never affected; they must keep their values.
SELECT '-- duplicate-insensitive aggregates are unchanged --';
SELECT c0,
    (SELECT min(lk1_04512.c1) FROM lk1_04512 WHERE lk1_04512.c0 = lk0_04512.c0) AS mn,
    (SELECT max(lk1_04512.c1) FROM lk1_04512 WHERE lk1_04512.c0 = lk0_04512.c0) AS mx,
    (SELECT uniqExact(lk1_04512.c1) FROM lk1_04512 WHERE lk1_04512.c0 = lk0_04512.c0) AS uq
FROM lk0_04512
ORDER BY id;

DROP TABLE lk0_04512;
DROP TABLE lk1_04512;

DROP TABLE outer_04512;
DROP TABLE inner_04512;
