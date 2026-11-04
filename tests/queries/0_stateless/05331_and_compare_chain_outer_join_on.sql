-- the compare chain must not add a preserved-side conjunct to outer join ON

SET optimize_and_compare_chain = 1;
SET join_algorithm = 'full_sorting_merge';

SELECT 'left';
SELECT l.number, r.number FROM numbers(3) AS l LEFT JOIN numbers(3) AS r ON l.number = r.number AND r.number < 2 ORDER BY l.number;

SELECT 'right';
SELECT l.number, r.number FROM numbers(3) AS l RIGHT JOIN numbers(3) AS r ON l.number = r.number AND l.number < 2 ORDER BY r.number;

SELECT 'nested and';
SELECT l.number, r.number FROM numbers(3) AS l LEFT JOIN numbers(3) AS r ON (l.number = r.number AND r.number < 2) AND r.number != 5 ORDER BY l.number;

SELECT 'preserved side is a join';
SELECT a.number, c.number FROM numbers(3) AS a INNER JOIN numbers(3) AS b ON a.number = b.number LEFT JOIN numbers(3) AS c ON b.number = c.number AND c.number < 2 ORDER BY a.number;

SELECT 'preserved side has array join';
SELECT x, r.number FROM numbers(1) AS l ARRAY JOIN [0, 1, 2] AS x LEFT JOIN numbers(3) AS r ON x = r.number AND r.number < 2 ORDER BY x;

SELECT 'datetime64';
DROP TABLE IF EXISTS t_chain_outer;
CREATE TABLE t_chain_outer (id UInt64, ts DateTime64(6, 'UTC')) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_chain_outer VALUES (1, '2020-01-01 00:00:01'), (2, '2020-01-01 00:00:03');
SELECT l.id, r.id FROM t_chain_outer AS l LEFT JOIN t_chain_outer AS r ON l.id = r.id AND l.ts = r.ts AND r.ts < toDateTime('2020-01-01 00:00:02', 'UTC') ORDER BY l.id;
DROP TABLE t_chain_outer;

-- 2 if the chain derives a bound in ON, 1 if not
SELECT 'derived bounds';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT 1 FROM numbers(3) AS l INNER JOIN numbers(3) AS r ON l.number = r.number AND r.number < 2) WHERE explain LIKE '%function_name: less,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT 1 FROM numbers(3) AS l LEFT JOIN numbers(3) AS r ON l.number = r.number AND l.number < 2) WHERE explain LIKE '%function_name: less,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT 1 FROM numbers(3) AS l LEFT SEMI JOIN numbers(3) AS r ON l.number = r.number AND r.number < 2) WHERE explain LIKE '%function_name: less,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT 1 FROM numbers(3) AS l FULL JOIN numbers(3) AS r ON l.number = r.number AND l.number < 2) WHERE explain LIKE '%function_name: less,%';
