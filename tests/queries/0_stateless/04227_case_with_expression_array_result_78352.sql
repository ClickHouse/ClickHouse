-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/78352
--
-- `CASE expr WHEN ... THEN ... ELSE ... END` over an `Array(String)`-returning
-- function (`splitByString`) used to be lowered to `transform`, which does not
-- support array results, and the query failed with
-- `Code: 36. DB::Exception: Unexpected type Array(String) in function 'transform'. (BAD_ARGUMENTS)`.
--
-- The materialized-column path was fixed in 25.3 and the all-constant path was
-- fixed in 26.3. This test pins both paths and both analyzer modes.

DROP TABLE IF EXISTS t_case_split_78352;
CREATE TABLE t_case_split_78352 (id Int32, input String) ENGINE = Memory;
INSERT INTO t_case_split_78352 VALUES (1, 'a#b#c#d'), (2, 'x|y'), (3, 'one');

SELECT 'memory table, analyzer=1:';
SELECT id, CASE id WHEN 1 THEN splitByString('#', input) ELSE splitByString('', '') END AS result
FROM t_case_split_78352 ORDER BY id
SETTINGS enable_analyzer = 1;

SELECT 'memory table, analyzer=0:';
SELECT id, CASE id WHEN 1 THEN splitByString('#', input) ELSE splitByString('', '') END AS result
FROM t_case_split_78352 ORDER BY id
SETTINGS enable_analyzer = 0;

DROP TABLE t_case_split_78352;

SELECT 'materialize subquery, analyzer=1:';
SELECT id, CASE id WHEN 1 THEN splitByString('#', input) ELSE splitByString('', '') END AS result
FROM (SELECT materialize(1) AS id, materialize('a#b#c#d') AS input)
SETTINGS enable_analyzer = 1;

SELECT 'materialize subquery, analyzer=0:';
SELECT id, CASE id WHEN 1 THEN splitByString('#', input) ELSE splitByString('', '') END AS result
FROM (SELECT materialize(1) AS id, materialize('a#b#c#d') AS input)
SETTINGS enable_analyzer = 0;

SELECT 'all-constant subquery, analyzer=1:';
SELECT id, CASE id WHEN 1 THEN splitByString('#', input) ELSE splitByString('', '') END AS result
FROM (SELECT 1 AS id, 'a#b#c#d' AS input)
SETTINGS enable_analyzer = 1;

SELECT 'all-constant subquery, analyzer=0:';
SELECT id, CASE id WHEN 1 THEN splitByString('#', input) ELSE splitByString('', '') END AS result
FROM (SELECT 1 AS id, 'a#b#c#d' AS input)
SETTINGS enable_analyzer = 0;
