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

SELECT 'Test that INSERT INTO with EXCEPT does not crash debug build:';
CREATE TABLE tab (c1 int, c2 int, c3 int);
CREATE TABLE tab2 (c1 int, c2 int, c3 int);
INSERT INTO tab2 SELECT * FROM tab EXCEPT SELECT * FROM tab;
DROP TABLE tab;
DROP TABLE tab2;
