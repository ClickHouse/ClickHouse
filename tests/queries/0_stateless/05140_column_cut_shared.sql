CREATE TABLE cut_shared (n UInt64, label LowCardinality(String), value Nullable(Int64), arrays Array(Array(UInt64))) ENGINE = Memory;
INSERT INTO cut_shared VALUES (2, 'two', NULL, [[4, 5], []]), (0, 'zero', 10, [[1, 2]]), (1, 'one', 20, [[3]]);

SELECT n, label, value, arrayMap(a -> arrayReverse(a), arrays), arrays
FROM (SELECT * FROM cut_shared ORDER BY n LIMIT 10) ORDER BY n;

WITH x -> x + n AS f
SELECT n, label, arrayMap(f, a), arrays FROM cut_shared ARRAY JOIN arrays AS a ORDER BY n, a;

SELECT l.number, r.label, r.value, r.arrays
FROM numbers(5) AS l LEFT JOIN cut_shared AS r ON l.number = r.n
ORDER BY l.number SETTINGS join_algorithm = 'hash', max_joined_block_size_rows = 1, join_use_nulls = 1,
    query_plan_join_swap_table = false;

SELECT n, label, value, arrays FROM cut_shared ORDER BY n;
DROP TABLE cut_shared;
