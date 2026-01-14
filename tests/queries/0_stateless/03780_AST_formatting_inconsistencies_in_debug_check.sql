SET enable_analyzer = 1;

SELECT 'These tests just need to run, dont care about output';
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
select tuple(1, 'a') as a1, tuple(1, 'a') IN (tuple(1, 'a') as a1);

SELECT 'Test repeated alias for tuple after NOT fails cleanly:';
select tuple(1, 'a') as a1, not (tuple(1, 'a') as a1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
