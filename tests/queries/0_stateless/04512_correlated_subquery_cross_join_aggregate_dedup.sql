-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS outer_04512;
DROP TABLE IF EXISTS inner_04512;

CREATE TABLE outer_04512 (id UInt32) ENGINE = Memory;
INSERT INTO outer_04512 VALUES (0), (1), (2);

CREATE TABLE inner_04512 (id UInt32, val String) ENGINE = Memory;
INSERT INTO inner_04512 VALUES (0, 'a');

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

SELECT '-- substitute=0, cross join, sum --';
SELECT o.id, n.number, (SELECT sum(id) FROM inner_04512 i WHERE i.id = o.id) AS s
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
-- size limits. With max_rows_in_distinct=1 + distinct_overflow_mode='break', a user DISTINCT would
-- truncate the domain to a single correlated key, so every other outer key would miss in the final
-- join (NULL / under-counted). Since the step runs unbounded, all keys are still evaluated.
INSERT INTO inner_04512 VALUES (1, 'b'), (2, 'c');
SELECT '-- internal distinct must ignore user distinct limits --';
SET max_rows_in_distinct = 1;
SET distinct_overflow_mode = 'break';
SELECT o.id, (SELECT count() FROM inner_04512 i WHERE i.id = o.id) AS c
FROM outer_04512 o CROSS JOIN numbers(3) n
GROUP BY o.id
ORDER BY o.id;
SET max_rows_in_distinct = 0;
SET distinct_overflow_mode = 'throw';

DROP TABLE outer_04512;
DROP TABLE inner_04512;
