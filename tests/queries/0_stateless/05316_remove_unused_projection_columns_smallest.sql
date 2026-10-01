-- When the outer query uses no column of a subquery, the subquery keeps only its column with the smallest type.

DROP TABLE IF EXISTS t_smallest;
CREATE TABLE t_smallest (s String, id UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_smallest SELECT repeat('x', 1000), number % 256 FROM numbers(1000);

SELECT 'subquery';
SELECT trim(explain) FROM (EXPLAIN QUERY TREE SELECT count() FROM (SELECT s, id FROM t_smallest)) WHERE trim(explain) IN ('s String', 'id UInt8');
SELECT 'union all';
SELECT trim(explain) FROM (EXPLAIN QUERY TREE SELECT count() FROM (SELECT s, id FROM t_smallest UNION ALL SELECT s, id FROM t_smallest)) WHERE trim(explain) IN ('s String', 'id UInt8');
SELECT 'nested';
SELECT trim(explain) FROM (EXPLAIN QUERY TREE SELECT count() FROM (SELECT * FROM (SELECT s, id FROM t_smallest))) WHERE trim(explain) IN ('s String', 'id UInt8');

-- The 1 MB column s is not read.
SELECT count() FROM (SELECT s, id FROM t_smallest LIMIT 100000) SETTINGS max_bytes_to_read = 100000;
SELECT count() FROM t_smallest WHERE EXISTS (SELECT s, id FROM t_smallest LIMIT 100000) SETTINGS max_bytes_to_read = 100000;
SELECT count() FROM (SELECT s, id FROM t_smallest WHERE id < 100) SETTINGS query_plan_remove_unused_columns = 0, max_bytes_to_read = 100000;

DROP TABLE t_smallest;
