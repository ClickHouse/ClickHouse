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

SELECT 'expression over a nested join';
SELECT a.number, c.number FROM numbers(3) AS a INNER JOIN numbers(3) AS b ON a.number = b.number LEFT JOIN numbers(3) AS c ON a.number + b.number = c.number AND c.number < 2 ORDER BY a.number;

SELECT 'full join using key';
SELECT id, c.k FROM (SELECT number AS id FROM numbers(3)) AS a FULL JOIN (SELECT number AS id FROM numbers(3)) AS b USING (id) LEFT JOIN (SELECT number AS k FROM numbers(3)) AS c ON id = c.k AND c.k < 2 ORDER BY id;

SELECT 'expression over array join';
SELECT l.number, x, r.number FROM numbers(2) AS l ARRAY JOIN [0, 1] AS x LEFT JOIN numbers(3) AS r ON l.number + x = r.number AND r.number < 2 ORDER BY l.number, x;

SELECT 'expression with a lambda';
SELECT a.number, c.v FROM (SELECT number, [number] AS arr FROM numbers(3)) AS a INNER JOIN numbers(3) AS b ON a.number = b.number LEFT JOIN (SELECT number AS k, number + 10 AS v FROM numbers(3)) AS c ON arraySum(x -> x * 2, a.arr) + b.number = c.k AND c.k < 2 ORDER BY a.number;

SELECT 'lambda capturing the preserved side';
SELECT a.number, c.v FROM numbers(3) AS a INNER JOIN numbers(3) AS b ON a.number = b.number LEFT JOIN (SELECT number AS k, number + 10 AS v FROM numbers(3)) AS c ON arraySum(x -> x + a.number + b.number, [0]) = c.k AND c.k < 2 ORDER BY a.number;

SELECT 'column-less key';
SELECT l.number, r.number FROM numbers(3) AS l LEFT JOIN numbers(3) AS r ON l.number = r.number AND materialize(0) < r.number AND r.number < 2 ORDER BY l.number;

SELECT 'key with indexHint';
SELECT l.number, r.number FROM numbers(3) AS l LEFT JOIN numbers(3) AS r ON l.number + indexHint(r.number < 2) = r.number AND r.number < 2 ORDER BY l.number;

SELECT 'key with a constant IN set';
SELECT l.number, r.v FROM numbers(3) AS l LEFT JOIN (SELECT number AS k, number + 10 AS v FROM numbers(3)) AS r ON l.number + (r.k IN [] AND r.k > 0) = r.k AND r.k < 2 ORDER BY l.number SETTINGS query_plan_remove_unused_columns = 1;

SELECT 'key with a folded conditional';
SELECT l.number, r.v FROM numbers(3) AS l LEFT JOIN (SELECT number AS k, number + 10 AS v FROM numbers(3)) AS r ON l.number + multiIf(1, 0, r.k) = r.k AND r.k < 2 ORDER BY l.number SETTINGS optimize_multiif_to_if = 1, query_plan_remove_unused_columns = 1;

SELECT 'key without columns under a lambda';
SELECT l.number, r.v FROM numbers(3) AS l LEFT JOIN (SELECT number AS k, number + 10 AS v FROM numbers(3)) AS r ON l.number = r.k AND arraySum(x -> x, [indexHint(r.v > 0)]) = r.k AND r.k < 2 ORDER BY l.number;

SELECT 'bound within the other side';
SELECT l.number, r.b FROM numbers(3) AS l LEFT JOIN (SELECT number AS a, number + 1 AS b FROM numbers(3)) AS r ON l.number = r.a AND r.a < r.b AND r.b < 2 ORDER BY l.number;

SELECT 'contradiction with a skipped bound';
SELECT l.number, r.number FROM numbers(3) AS l LEFT JOIN (SELECT number, number AS b FROM numbers(10)) AS r ON l.number = r.number AND r.number < 2 AND l.number = r.b AND r.b > 5 ORDER BY l.number SETTINGS optimize_redundant_comparisons = 0;

SELECT 'impossible bound';
SELECT l.x, r.y FROM (SELECT toUInt8(number) AS x FROM numbers(3)) AS l LEFT JOIN (SELECT toUInt16(number) AS y FROM numbers(3)) AS r ON l.x = r.y AND r.y > 300 ORDER BY l.x SETTINGS optimize_redundant_comparisons = 0;

SELECT 'contradiction on the preserved side';
SELECT l.k, r.number FROM (SELECT number AS k, number AS x, number AS z FROM numbers(3)) AS l LEFT JOIN numbers(3) AS r ON l.k = r.number AND l.x < 2 AND l.x = l.z AND l.z > 5 ORDER BY l.k SETTINGS optimize_redundant_comparisons = 1;

-- 2 if the chain derives a bound in ON, 1 if not
SELECT 'derived bounds';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT 1 FROM numbers(3) AS l INNER JOIN numbers(3) AS r ON l.number = r.number AND r.number < 2) WHERE explain LIKE '%function_name: less,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT 1 FROM numbers(3) AS l LEFT JOIN numbers(3) AS r ON l.number = r.number AND l.number < 2) WHERE explain LIKE '%function_name: less,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT 1 FROM numbers(3) AS l LEFT SEMI JOIN numbers(3) AS r ON l.number = r.number AND r.number < 2) WHERE explain LIKE '%function_name: less,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT 1 FROM numbers(3) AS l FULL JOIN numbers(3) AS r ON l.number = r.number AND l.number < 2) WHERE explain LIKE '%function_name: less,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT 1 FROM numbers(3) AS a INNER JOIN numbers(3) AS b ON a.number = b.number INNER JOIN numbers(3) AS c ON a.number + b.number = c.number AND c.number < 2) WHERE explain LIKE '%function_name: less,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT 1 FROM numbers(3) AS l LEFT JOIN numbers(3) AS r ON l.number = r.number AND arraySum(x -> x + r.number, [0]) = r.number AND r.number < 2) WHERE explain LIKE '%function_name: less,%';

SELECT 'hint for the non-preserved side';
DROP TABLE IF EXISTS t_chain_rj;
CREATE TABLE t_chain_rj (a UInt64, b UInt64, k UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 1024, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_chain_rj SELECT number, number + 1, number FROM numbers(65536);
SELECT l.a, r.number FROM t_chain_rj AS l RIGHT JOIN numbers(3) AS r ON l.k = r.number AND l.a < l.b AND l.b < 1000 ORDER BY r.number;
SELECT count() FROM (EXPLAIN QUERY TREE SELECT 1 FROM t_chain_rj AS l RIGHT JOIN numbers(3) AS r ON l.k = r.number AND l.a < l.b AND l.b < 1000) WHERE explain LIKE '%function_name: indexHint%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT l.a FROM t_chain_rj AS l RIGHT JOIN numbers(3) AS r ON l.k = r.number AND l.a < l.b AND l.b < 1000) WHERE explain LIKE '%Granules: 1/64%';

SELECT 'hint in a subquery in ON';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT 1 FROM numbers(3) AS l LEFT JOIN numbers(3) AS r ON l.number = r.number AND r.number IN (SELECT a FROM t_chain_rj WHERE a < b AND b < 1000)) WHERE explain LIKE '%function_name: indexHint%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT l.number, r.number FROM numbers(3) AS l LEFT JOIN numbers(3) AS r ON l.number = r.number AND r.number IN (SELECT a FROM t_chain_rj WHERE a < b AND b < 1000)) WHERE explain LIKE '%Granules: 1/64%' SETTINGS parallel_replicas_local_plan = 1;

SELECT 'global right join swapped to left';
SELECT l.a, r.number FROM remote('127.0.0.{1,2}', currentDatabase(), t_chain_rj) AS l GLOBAL RIGHT JOIN numbers(3) AS r ON l.k = r.number AND l.a < l.b AND l.b < 1000 ORDER BY r.number, l.a;
DROP TABLE t_chain_rj;
