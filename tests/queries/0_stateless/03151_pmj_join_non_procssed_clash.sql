SET join_algorithm = 'partial_merge';
SET max_joined_block_size_rows = 100;


SELECT count(ignore(*)), sum(t1.a), sum(t1.b), sum(t2.a)
FROM ( SELECT number AS a, number AS b FROM numbers(10000) ) t1
JOIN  ( SELECT number + 100 AS a FROM numbers(10000) ) t2
ON t1.a = t2.a AND t1.b > 0;
