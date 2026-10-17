-- INTERSECT ALL and EXCEPT ALL are executed as a multiset SEMI or ANTI LEFT JOIN on all columns:
-- each right row matches at most one left row.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_all_left;
DROP TABLE IF EXISTS t_all_right;
DROP TABLE IF EXISTS t_all_third;
CREATE TABLE t_all_left (a UInt64, b Nullable(String), c LowCardinality(String), d Array(UInt8)) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_all_right (a UInt64, b Nullable(String), c LowCardinality(String), d Array(UInt8)) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_all_third (a UInt64, b Nullable(String), c LowCardinality(String), d Array(UInt8)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_all_left SELECT number % 300, if(number % 7 = 0, NULL, toString(number % 11)), toString(number % 3), [number % 2] FROM numbers(3000);
INSERT INTO t_all_right SELECT number % 200 + 100, if(number % 5 = 0, NULL, toString(number % 11)), toString(number % 2), [number % 2] FROM numbers(3000);
INSERT INTO t_all_third SELECT number % 100 + 150, if(number % 3 = 0, NULL, toString(number % 11)), toString(number % 2), [number % 2] FROM numbers(3000);

SELECT 'same multiset as the set-operation step';
SELECT count(), sum(a), countIf(b IS NULL) FROM (SELECT * FROM t_all_left INTERSECT ALL SELECT * FROM t_all_right);
SELECT arraySort(groupArray(tuple(*))) = (SELECT arraySort(groupArray(tuple(*))) FROM (SELECT * FROM t_all_left INTERSECT ALL SELECT * FROM t_all_right) SETTINGS optimize_rewrite_intersect_except_to_join = 0)
FROM (SELECT * FROM t_all_left INTERSECT ALL SELECT * FROM t_all_right);

SELECT count(), sum(a), countIf(b IS NULL) FROM (SELECT * FROM t_all_left EXCEPT ALL SELECT * FROM t_all_right);
SELECT arraySort(groupArray(tuple(*))) = (SELECT arraySort(groupArray(tuple(*))) FROM (SELECT * FROM t_all_left EXCEPT ALL SELECT * FROM t_all_right) SETTINGS optimize_rewrite_intersect_except_to_join = 0)
FROM (SELECT * FROM t_all_left EXCEPT ALL SELECT * FROM t_all_right);

SELECT 'each right row matches at most one left row';
SELECT x, count() FROM (SELECT arrayJoin([1, 1, 1, 2, 2, 3]) AS x INTERSECT ALL SELECT arrayJoin([1, 1, 2, 4])) GROUP BY x ORDER BY x;
SELECT x, count() FROM (SELECT arrayJoin([1, 1, 1, 2, 2, 3]) AS x EXCEPT ALL SELECT arrayJoin([1, 1, 2, 4])) GROUP BY x ORDER BY x;
SELECT count() FROM (SELECT 1 FROM numbers(5) INTERSECT ALL SELECT 1 FROM numbers(3));
SELECT count() FROM (SELECT 1 FROM numbers(5) EXCEPT ALL SELECT 1 FROM numbers(3));

SELECT 'hash and parallel hash';
SELECT count(), sum(x) FROM (SELECT number % 1000 AS x FROM numbers(100000) INTERSECT ALL SELECT number % 700 AS x FROM numbers(50000)) SETTINGS join_algorithm = 'hash';
SELECT count(), sum(x) FROM (SELECT number % 1000 AS x FROM numbers(100000) INTERSECT ALL SELECT number % 700 AS x FROM numbers(50000)) SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(x) FROM (SELECT number % 1000 AS x FROM numbers(100000) INTERSECT ALL SELECT number % 700 AS x FROM numbers(50000)) SETTINGS optimize_rewrite_intersect_except_to_join = 0;
SELECT count(), sum(x) FROM (SELECT number % 1000 AS x FROM numbers(100000) EXCEPT ALL SELECT number % 700 AS x FROM numbers(50000)) SETTINGS join_algorithm = 'hash';
SELECT count(), sum(x) FROM (SELECT number % 1000 AS x FROM numbers(100000) EXCEPT ALL SELECT number % 700 AS x FROM numbers(50000)) SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(x) FROM (SELECT number % 1000 AS x FROM numbers(100000) EXCEPT ALL SELECT number % 700 AS x FROM numbers(50000)) SETTINGS optimize_rewrite_intersect_except_to_join = 0;

SELECT 'three arms';
SELECT count(), sum(a) FROM (SELECT * FROM t_all_left INTERSECT ALL SELECT * FROM t_all_right INTERSECT ALL SELECT * FROM t_all_third);
SELECT count(), sum(a) FROM (SELECT * FROM t_all_left INTERSECT ALL SELECT * FROM t_all_right INTERSECT ALL SELECT * FROM t_all_third) SETTINGS optimize_rewrite_intersect_except_to_join = 0;
SELECT count(), sum(a) FROM (SELECT * FROM t_all_left EXCEPT ALL SELECT * FROM t_all_right EXCEPT ALL SELECT * FROM t_all_third);
SELECT count(), sum(a) FROM (SELECT * FROM t_all_left EXCEPT ALL SELECT * FROM t_all_right EXCEPT ALL SELECT * FROM t_all_third) SETTINGS optimize_rewrite_intersect_except_to_join = 0;
SELECT count(), sum(a) FROM (SELECT * FROM t_all_left EXCEPT ALL SELECT * FROM t_all_right INTERSECT ALL SELECT * FROM t_all_third);
SELECT count(), sum(a) FROM (SELECT * FROM t_all_left EXCEPT ALL SELECT * FROM t_all_right INTERSECT ALL SELECT * FROM t_all_third) SETTINGS optimize_rewrite_intersect_except_to_join = 0;
SELECT count(), sum(a) FROM (SELECT * FROM t_all_left INTERSECT DISTINCT SELECT * FROM t_all_right EXCEPT ALL SELECT * FROM t_all_third);
SELECT count(), sum(a) FROM (SELECT * FROM t_all_left INTERSECT DISTINCT SELECT * FROM t_all_right EXCEPT ALL SELECT * FROM t_all_third) SETTINGS optimize_rewrite_intersect_except_to_join = 0;

SELECT 'NULL matches NULL';
SELECT x FROM (SELECT arrayJoin(CAST([NULL, NULL, 1], 'Array(Nullable(UInt8))')) AS x INTERSECT ALL SELECT arrayJoin(CAST([NULL, NULL, NULL], 'Array(Nullable(UInt8))'))) ORDER BY x;
SELECT x FROM (SELECT arrayJoin(CAST([NULL, NULL, 1], 'Array(Nullable(UInt8))')) AS x EXCEPT ALL SELECT arrayJoin(CAST([NULL, NULL, NULL], 'Array(Nullable(UInt8))'))) ORDER BY x;
SELECT * FROM (SELECT (NULL, 1)::Tuple(Nullable(UInt8), UInt8) AS x INTERSECT ALL SELECT (NULL, 1)::Tuple(Nullable(UInt8), UInt8));
SELECT * FROM (SELECT [NULL]::Array(Nullable(UInt8)) AS x EXCEPT ALL SELECT [NULL]::Array(Nullable(UInt8)));

SELECT 'floats are compared bitwise like in the set operation';
SELECT x FROM (SELECT arrayJoin([0.0, 0.0]) AS x INTERSECT ALL SELECT arrayJoin([-0.0, 0.0, 0.0])) ORDER BY x;
SELECT x FROM (SELECT arrayJoin([-0.0, 0.0]) AS x EXCEPT ALL SELECT 0.0) ORDER BY x;
SELECT x FROM (SELECT arrayJoin([nan, nan]) AS x INTERSECT ALL SELECT nan);

SELECT 'types are converted to the common type';
SELECT x, toTypeName(x) FROM (SELECT 1::UInt8 AS x INTERSECT ALL SELECT 1::Int64);
SELECT x, toTypeName(x) FROM (SELECT 1::UInt8 AS x EXCEPT ALL SELECT 2::Int64);
SELECT x, toTypeName(x) FROM (SELECT 'a'::LowCardinality(String) AS x INTERSECT ALL SELECT 'a');
SELECT x, toTypeName(x) FROM (SELECT 1 AS x INTERSECT ALL SELECT NULL::Nullable(UInt8));

SELECT 'constants and empty inputs';
SELECT * FROM (SELECT 1 INTERSECT ALL SELECT 1);
SELECT * FROM (SELECT 1 INTERSECT ALL SELECT 2);
SELECT * FROM (SELECT 1 AS x, 'a' AS y EXCEPT ALL SELECT 1, 'b');
SELECT count() FROM (SELECT number FROM numbers(0) INTERSECT ALL SELECT number FROM numbers(10));
SELECT count() FROM (SELECT number FROM numbers(10) INTERSECT ALL SELECT number FROM numbers(0));
SELECT count() FROM (SELECT number FROM numbers(10) EXCEPT ALL SELECT number FROM numbers(0));
SELECT count() FROM (SELECT number FROM numbers(0) EXCEPT ALL SELECT number FROM numbers(10));

SELECT 'the result is used by the outer query';
SELECT x + 1 AS y FROM (SELECT number AS x FROM numbers(5) INTERSECT ALL SELECT number FROM numbers(2, 5)) ORDER BY y;
SELECT count() FROM (SELECT number AS x FROM numbers(5) INTERSECT ALL SELECT number FROM numbers(3)) WHERE x > 0;
SELECT number FROM numbers(5) WHERE number IN (SELECT number FROM numbers(3) INTERSECT ALL SELECT number FROM numbers(1, 5)) ORDER BY number;
WITH cte AS (SELECT number AS x FROM numbers(6) EXCEPT ALL SELECT number FROM numbers(2)) SELECT sum(x) FROM cte;
SELECT count() FROM remote('127.0.0.1', view(SELECT number FROM numbers(4) INTERSECT ALL SELECT number FROM numbers(2)));

SELECT 'the plan';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT number FROM numbers(3) INTERSECT ALL SELECT number FROM numbers(2))
WHERE explain LIKE '%Join%' OR explain LIKE '%Strictness%' OR explain LIKE '%Multiset%' OR explain LIKE '%IntersectOrExcept%';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT number FROM numbers(3) EXCEPT ALL SELECT number FROM numbers(2))
WHERE explain LIKE '%Join%' OR explain LIKE '%Strictness%' OR explain LIKE '%Multiset%' OR explain LIKE '%IntersectOrExcept%';

SELECT 'the set-operation step stays for what the join cannot take';
SET optimize_rewrite_intersect_except_to_join = 0;
SELECT trim(explain) FROM (EXPLAIN SELECT number FROM numbers(3) INTERSECT ALL SELECT number FROM numbers(2))
WHERE explain LIKE '%Join%' OR explain LIKE '%IntersectOrExcept%';
SET optimize_rewrite_intersect_except_to_join = 1;
SET join_algorithm = 'full_sorting_merge';
SELECT trim(explain) FROM (EXPLAIN SELECT number FROM numbers(3) INTERSECT ALL SELECT number FROM numbers(2))
WHERE explain LIKE '%Join%' OR explain LIKE '%IntersectOrExcept%';
SELECT count(), sum(x) FROM (SELECT number % 1000 AS x FROM numbers(100000) EXCEPT ALL SELECT number % 700 AS x FROM numbers(50000));
SET join_algorithm = 'grace_hash';
SELECT trim(explain) FROM (EXPLAIN SELECT number FROM numbers(3) INTERSECT ALL SELECT number FROM numbers(2))
WHERE explain LIKE '%Join%' OR explain LIKE '%IntersectOrExcept%';
SET join_algorithm = 'grace_hash,hash';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT number FROM numbers(3) INTERSECT ALL SELECT number FROM numbers(2))
WHERE explain LIKE '%Algorithm%' OR explain LIKE '%IntersectOrExcept%';
SET join_algorithm = DEFAULT;
SET enable_join_key_only_hash_tables = 0;
SELECT trim(explain) FROM (EXPLAIN SELECT number FROM numbers(3) INTERSECT ALL SELECT number FROM numbers(2))
WHERE explain LIKE '%Join%' OR explain LIKE '%IntersectOrExcept%';
SET enable_join_key_only_hash_tables = DEFAULT;
SELECT trim(explain) FROM (EXPLAIN SELECT number::Dynamic AS x FROM numbers(3) INTERSECT ALL SELECT number::Dynamic FROM numbers(2) SETTINGS allow_experimental_dynamic_type = 1)
WHERE explain LIKE '%Join%' OR explain LIKE '%IntersectOrExcept%';
SELECT trim(explain) FROM (EXPLAIN SELECT number, number FROM numbers(3) INTERSECT ALL SELECT number, number FROM numbers(2))
WHERE explain LIKE '%Join%' OR explain LIKE '%IntersectOrExcept%';
SELECT count() FROM (SELECT number::Dynamic AS x FROM numbers(4) INTERSECT ALL SELECT number::Dynamic FROM numbers(2)) SETTINGS allow_experimental_dynamic_type = 1;
SELECT * FROM (SELECT number, number FROM numbers(3) INTERSECT ALL SELECT number, number FROM numbers(2)) ORDER BY ALL;

DROP TABLE t_all_left;
DROP TABLE t_all_right;
DROP TABLE t_all_third;
