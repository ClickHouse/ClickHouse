-- Verify that ALTER TABLE ... MODIFY QUERY round-trips correctly when trailing
-- SETTINGS are present. The formatter must parenthesize the MODIFY QUERY select
-- so that SETTINGS are not consumed by the inner SELECT during re-parsing,
-- but must NOT propagate that flag into nested subqueries.

-- Basic case: MODIFY QUERY with trailing SETTINGS
SELECT formatQuery('ALTER TABLE v1 MODIFY QUERY SELECT 1 SETTINGS max_threads = 1');

-- Round-trip: formatting twice must produce the same result
SELECT formatQuery('ALTER TABLE v1 MODIFY QUERY SELECT 1 SETTINGS max_threads = 1')
     = formatQuery(formatQuery('ALTER TABLE v1 MODIFY QUERY SELECT 1 SETTINGS max_threads = 1'));

-- Without trailing SETTINGS: no parentheses needed
SELECT formatQuery('ALTER TABLE v1 MODIFY QUERY SELECT 1');

-- MODIFY QUERY with UNION and trailing SETTINGS
SELECT formatQuery('ALTER TABLE v1 MODIFY QUERY SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1');

SELECT formatQuery('ALTER TABLE v1 MODIFY QUERY SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1')
     = formatQuery(formatQuery('ALTER TABLE v1 MODIFY QUERY SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1'));

-- Nested subqueries with INTERSECT inside MODIFY QUERY with trailing SETTINGS:
-- the parenthesization must not propagate into the nested subquery
SELECT formatQuery('ALTER TABLE v1 (MODIFY QUERY SELECT c0 FROM ((SELECT 1 AS c0) INTERSECT DISTINCT (SELECT 2 AS c0))) SETTINGS max_threads = 1');

SELECT formatQuery('ALTER TABLE v1 (MODIFY QUERY SELECT c0 FROM ((SELECT 1 AS c0) INTERSECT DISTINCT (SELECT 2 AS c0))) SETTINGS max_threads = 1')
     = formatQuery(formatQuery('ALTER TABLE v1 (MODIFY QUERY SELECT c0 FROM ((SELECT 1 AS c0) INTERSECT DISTINCT (SELECT 2 AS c0))) SETTINGS max_threads = 1'));
