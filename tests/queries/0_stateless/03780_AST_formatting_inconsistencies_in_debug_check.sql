SET enable_analyzer = 1;

--These tests just need to run, dont care about output
SELECT 'Test repeated alias for literal as 2nd arg to IN operator:';
SELECT ([1] AS foo), [1] IN ([1] AS foo);

SELECT 'Test repeated alias for statement which is not a literal:';
SELECT ([isNaN(1)] AS foo), [1] IN ([isNaN(1)] AS foo);

SELECT 'Test repeated alias for negated col:';
CREATE TABLE tab(c1 int);
SELECT (-((-`c1`) AS `a2`)), NOT (-((-`c1`) AS `a2`)) from tab;
DROP TABLE tab;

SELECT 'Test that large max_query_size doesnt cause overflow:';
SELECT 'test' SETTINGS max_query_size = 9223372036854775309;

SELECT 'Test repeated alias in subquery after IN:';
SELECT ((SELECT [1,2,3]) AS a1), [5] IN ((SELECT [1,2,3]) AS a1);

SELECT 'Test repeated alias in subquery after NOT:';
SELECT ((SELECT 1) AS a1), NOT ((SELECT 1) AS a1);

SELECT 'Test repeated alias for tuple after IN:';
SELECT tuple(1, 'a') AS a1, tuple(1, 'a') IN (tuple(1, 'a') AS a1);

SELECT 'Alias in ON clause of JOIN:';
CREATE TABLE t1 (x int);
CREATE TABLE t2 (x int);
SELECT * FROM t1 JOIN t2 ON ((t1.x = t2.x) AND (t1.x IS NULL) AS e2);
DROP TABLE t1, t2;

SELECT 'Test repeated alias for tuple after NOT fails cleanly:';
SELECT tuple(1, 'a') AS a1, NOT (tuple(1, 'a') AS a1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Test that INSERT INTO with EXCEPT does not crash debug build:';
CREATE TABLE tab (c1 int, c2 int, c3 int);
CREATE TABLE tab2 (c1 int, c2 int, c3 int);
INSERT INTO tab2 SELECT * FROM tab EXCEPT SELECT * FROM tab;
DROP TABLE tab;
DROP TABLE tab2;

SELECT 'Test weird SELECT/EXCEPT/SELECT statement:';
SELECT 1,2,3 EXCEPT SELECT 1,2,3;

SELECT 'Testing complex tuple expressions with indexes:';
CREATE TABLE tab (c1 Tuple(int, int));
INSERT INTO tab VALUES (tuple(1,1));
SELECT (tab.*).2 FROM tab;
DROP TABLE tab;

WITH (((1,1),1),1) AS t1 SELECT t1.1.1.1;
