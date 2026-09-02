SET any_join_distinct_right_table_keys = 1;
SET joined_subquery_requires_alias = 0;
SET enable_analyzer = 1;

SELECT * FROM (SELECT 1 AS a, 'x' AS b) join (SELECT 1 as a, 'y' as b) using a;
SELECT * FROM (SELECT 1 AS a, 'x' AS b) left join (SELECT 1 as a, 'y' as b) using a;
SELECT * FROM (SELECT 1 AS a, 'x' AS b) full join (SELECT 1 as a, 'y' as b) using a;
SELECT * FROM (SELECT 1 AS a, 'x' AS b) right join (SELECT 1 as a, 'y' as b) using a;

SELECT * FROM (SELECT 1 AS a, 'x' AS b) any join (SELECT 1 as a, 'y' as b) using a;
SELECT * FROM (SELECT 1 AS a, 'x' AS b) any left join (SELECT 1 as a, 'y' as b) using a;
SELECT * FROM (SELECT 1 AS a, 'x' AS b) any full join (SELECT 1 as a, 'y' as b) using a;
SELECT * FROM (SELECT 1 AS a, 'x' AS b) any right join (SELECT 1 as a, 'y' as b) using a;

SET any_join_distinct_right_table_keys = 0;
SELECT * FROM (SELECT 1 AS a, 'x' AS b) any join (SELECT 1 as a, 'y' as b) using a;
SELECT * FROM (SELECT 1 AS a, 'x' AS b) left join (SELECT 1 as a, 'y' as b) using a;
SELECT * FROM (SELECT 1 AS a, 'x' AS b) any right join (SELECT 1 as a, 'y' as b) using a;
SELECT * FROM (SELECT 1 AS a, 'x' AS b) any full join (SELECT 1 as a, 'y' as b) using a; -- { serverError NOT_IMPLEMENTED }
