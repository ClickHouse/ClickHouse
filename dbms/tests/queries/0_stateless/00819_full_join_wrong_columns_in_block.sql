SELECT * FROM (SELECT 1 AS a, 'x' AS b) join (SELECT 1 as a, 'y' as b) using a;
SELECT * FROM (SELECT 1 AS a, 'x' AS b) left join (SELECT 1 as a, 'y' as b) using a;
SELECT * FROM (SELECT 1 AS a, 'x' AS b) full join (SELECT 1 as a, 'y' as b) using a;
SELECT * FROM (SELECT 1 AS a, 'x' AS b) right join (SELECT 1 as a, 'y' as b) using a;

SELECT * FROM (SELECT 1 AS a, 'x' AS b) any join (SELECT 1 as a, 'y' as b) using a;
SELECT * FROM (SELECT 1 AS a, 'x' AS b) any left join (SELECT 1 as a, 'y' as b) using a;
SELECT * FROM (SELECT 1 AS a, 'x' AS b) any full join (SELECT 1 as a, 'y' as b) using a;
SELECT * FROM (SELECT 1 AS a, 'x' AS b) any right join (SELECT 1 as a, 'y' as b) using a;
