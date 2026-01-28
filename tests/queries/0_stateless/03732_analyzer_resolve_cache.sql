SET enable_analyzer = 1;

EXPLAIN QUERY TREE
WITH a as (SELECT 1 as a)
SELECT a + a FROM a, a;

WITH a as (SELECT 1 as a)
SELECT a + a FROM a, a;