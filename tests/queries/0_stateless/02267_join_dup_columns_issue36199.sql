set join_algorithm = 'hash';

SELECT *
FROM ( SELECT 2 AS x ) AS t1
RIGHT JOIN ( SELECT count('x'), count('y'), 2 AS x ) AS t2
ON t1.x = t2.x;

set join_algorithm = 'partial_merge';

SELECT *
FROM ( SELECT 2 AS x ) AS t1
RIGHT JOIN ( SELECT count('x'), count('y'), 2 AS x ) AS t2
ON t1.x = t2.x;
