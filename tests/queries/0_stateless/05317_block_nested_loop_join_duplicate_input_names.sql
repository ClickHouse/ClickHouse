-- A join with no join key, executed as a block nested loop join, whose condition reads a column
-- that one of its inputs carries twice: a subquery that selects `k` twice.

SET allow_block_nested_loop_join = 1;
SET join_algorithm = 'direct,parallel_hash,hash';
SET query_plan_join_swap_table = 'false';

SELECT count() FROM (EXPLAIN SELECT * FROM (SELECT 1 AS k) AS t1 FULL JOIN (SELECT k, 1 AS k) AS t2 ON t2.k > t1.k) WHERE explain LIKE '%BlockNestedLoopJoin%';

-- { echoOn }

SELECT * FROM (SELECT 1 AS k) AS t1 FULL OUTER JOIN (SELECT k, 1 AS k) AS t2 ON t2.k > t1.k ORDER BY 1 DESC;
SELECT * FROM (SELECT 1 AS k) AS t1 FULL JOIN (SELECT k, 1 AS k) AS t2 ON t2.k > t1.k ORDER BY 1, 2, 3 SETTINGS join_use_nulls = 1;
SELECT t2.k FROM (SELECT 1 AS k) AS t1 FULL JOIN (SELECT k, 1 AS k) AS t2 ON t2.k > t1.k ORDER BY 1;
SELECT count() FROM (SELECT 1 AS k) AS t1 FULL JOIN (SELECT k, 1 AS k) AS t2 ON t2.k > t1.k;

SELECT * FROM (SELECT number AS k FROM numbers(3)) AS t1 LEFT JOIN (SELECT number AS k, k FROM numbers(3)) AS t2 ON t1.k < t2.k ORDER BY 1, 2, 3;
SELECT * FROM (SELECT number AS k, k FROM numbers(3)) AS t1 RIGHT JOIN (SELECT number AS k FROM numbers(3)) AS t2 ON t1.k > t2.k ORDER BY 1, 2, 3;
SELECT * FROM (SELECT number AS k, k FROM numbers(3)) AS t1 FULL JOIN (SELECT number + 1 AS k, k FROM numbers(3)) AS t2 ON t1.k > t2.k ORDER BY 1, 2, 3, 4;

SELECT * FROM (SELECT number AS k FROM numbers(3)) AS t1 LEFT ANY JOIN (SELECT number AS k, k FROM numbers(3)) AS t2 ON t2.k > t1.k AND t2.k < t1.k + 2 ORDER BY 1, 2, 3;
SELECT t1.k FROM (SELECT number AS k FROM numbers(3)) AS t1 LEFT SEMI JOIN (SELECT number AS k, k FROM numbers(3)) AS t2 ON t1.k < t2.k ORDER BY 1;
SELECT t1.k FROM (SELECT number AS k FROM numbers(3)) AS t1 LEFT ANTI JOIN (SELECT number AS k, k FROM numbers(3)) AS t2 ON t1.k < t2.k ORDER BY 1;
