-- The right side carries two columns named `c`, and the block nested loop join condition reads `c`.

SET enable_analyzer = 1;
SET allow_block_nested_loop_join = 1;

SELECT l.a, r.b, r.c
FROM (SELECT number AS a FROM numbers(10) WHERE number % 5 = 0) AS l
RIGHT JOIN (SELECT 1 AS c, number AS b, c FROM numbers(3)) AS r
ON xor(l.a = r.b, r.c = 1)
ORDER BY ALL
SETTINGS query_plan_join_swap_table = 'false';

SELECT l.a, r.b, r.c
FROM (SELECT number AS a FROM numbers(10) WHERE number % 5 = 0) AS l
LEFT JOIN (SELECT 1 AS c, number AS b, c FROM numbers(3)) AS r
ON l.a < r.b + r.c
ORDER BY ALL
SETTINGS query_plan_join_swap_table = 'false';
