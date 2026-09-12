-- INTERSECT DISTINCT and EXCEPT DISTINCT are executed as a SEMI or ANTI LEFT JOIN on all columns followed by DISTINCT.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_set_left;
DROP TABLE IF EXISTS t_set_right;
DROP TABLE IF EXISTS t_set_third;
CREATE TABLE t_set_left (a UInt64, b Nullable(String), c LowCardinality(String), d Array(UInt8)) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_set_right (a UInt64, b Nullable(String), c LowCardinality(String), d Array(UInt8)) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_set_third (a UInt64, b Nullable(String), c LowCardinality(String), d Array(UInt8)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_set_left SELECT number % 300, if(number % 7 = 0, NULL, toString(number % 11)), toString(number % 3), [number % 2] FROM numbers(3000);
INSERT INTO t_set_right SELECT number % 200 + 100, if(number % 5 = 0, NULL, toString(number % 11)), toString(number % 2), [number % 2] FROM numbers(3000);
INSERT INTO t_set_third SELECT number % 100 + 150, if(number % 3 = 0, NULL, toString(number % 11)), toString(number % 2), [number % 2] FROM numbers(3000);

SELECT 'same result as the set-operation step';
SELECT count(), sum(a), countIf(b IS NULL) FROM (SELECT * FROM t_set_left INTERSECT DISTINCT SELECT * FROM t_set_right);
SELECT arraySort(groupArray(tuple(*))) = (SELECT arraySort(groupArray(tuple(*))) FROM (SELECT * FROM t_set_left INTERSECT DISTINCT SELECT * FROM t_set_right) SETTINGS optimize_rewrite_intersect_except_to_join = 0)
FROM (SELECT * FROM t_set_left INTERSECT DISTINCT SELECT * FROM t_set_right);

SELECT count(), sum(a), countIf(b IS NULL) FROM (SELECT * FROM t_set_left EXCEPT DISTINCT SELECT * FROM t_set_right);
SELECT arraySort(groupArray(tuple(*))) = (SELECT arraySort(groupArray(tuple(*))) FROM (SELECT * FROM t_set_left EXCEPT DISTINCT SELECT * FROM t_set_right) SETTINGS optimize_rewrite_intersect_except_to_join = 0)
FROM (SELECT * FROM t_set_left EXCEPT DISTINCT SELECT * FROM t_set_right);

SELECT 'three arms';
SELECT count(), sum(a) FROM (SELECT * FROM t_set_left INTERSECT DISTINCT SELECT * FROM t_set_right INTERSECT DISTINCT SELECT * FROM t_set_third);
SELECT count(), sum(a) FROM (SELECT * FROM t_set_left INTERSECT DISTINCT SELECT * FROM t_set_right INTERSECT DISTINCT SELECT * FROM t_set_third) SETTINGS optimize_rewrite_intersect_except_to_join = 0;
SELECT count(), sum(a) FROM (SELECT * FROM t_set_left EXCEPT DISTINCT SELECT * FROM t_set_right EXCEPT DISTINCT SELECT * FROM t_set_third);
SELECT count(), sum(a) FROM (SELECT * FROM t_set_left EXCEPT DISTINCT SELECT * FROM t_set_right EXCEPT DISTINCT SELECT * FROM t_set_third) SETTINGS optimize_rewrite_intersect_except_to_join = 0;
SELECT count(), sum(a) FROM (SELECT * FROM t_set_left EXCEPT DISTINCT SELECT * FROM t_set_right INTERSECT DISTINCT SELECT * FROM t_set_third);
SELECT count(), sum(a) FROM (SELECT * FROM t_set_left EXCEPT DISTINCT SELECT * FROM t_set_right INTERSECT DISTINCT SELECT * FROM t_set_third) SETTINGS optimize_rewrite_intersect_except_to_join = 0;

SELECT 'NULL matches NULL';
SELECT * FROM (SELECT NULL::Nullable(UInt8) AS x INTERSECT DISTINCT SELECT NULL::Nullable(UInt8));
SELECT * FROM (SELECT NULL::Nullable(UInt8) AS x EXCEPT DISTINCT SELECT NULL::Nullable(UInt8));
SELECT * FROM (SELECT (NULL, 1)::Tuple(Nullable(UInt8), UInt8) AS x INTERSECT DISTINCT SELECT (NULL, 1)::Tuple(Nullable(UInt8), UInt8));

SELECT 'duplicates and constants';
SELECT * FROM (SELECT number % 3 AS x FROM numbers(10) INTERSECT DISTINCT SELECT 1);
SELECT * FROM (SELECT number % 3 AS x FROM numbers(10) EXCEPT DISTINCT SELECT 1) ORDER BY x;
SELECT * FROM (SELECT 1 INTERSECT DISTINCT SELECT 1);
SELECT count() FROM (SELECT 1 EXCEPT DISTINCT SELECT 1);
SELECT * FROM (SELECT 1, 1 INTERSECT DISTINCT SELECT 1, 1);
SELECT * FROM (SELECT 1 AS x INTERSECT DISTINCT SELECT 1.0);
SELECT * FROM (SELECT 1 AS x EXCEPT DISTINCT SELECT 1.5);
SELECT * FROM (SELECT 1::Dynamic AS x INTERSECT DISTINCT SELECT 1::Dynamic);

SELECT 'several generated subqueries';
SELECT * FROM (SELECT NULL AS x INTERSECT DISTINCT SELECT 1);
SELECT * FROM (SELECT toLowCardinality(toNullable('a')) AS x INTERSECT DISTINCT SELECT 'a');
SELECT * FROM (SELECT 1::UInt64 AS x INTERSECT DISTINCT SELECT -1::Int64);
SELECT * FROM (SELECT 1 AS x INTERSECT DISTINCT SELECT 'a' SETTINGS use_variant_as_common_type = 1);
SELECT * FROM (SELECT number AS x FROM numbers(3) INTERSECT DISTINCT SELECT 1 INTERSECT DISTINCT SELECT 1);
SELECT * FROM ((SELECT 1 AS x INTERSECT DISTINCT SELECT 1.0) INTERSECT DISTINCT (SELECT 1 AS x INTERSECT DISTINCT SELECT 1.0));
SELECT * FROM (SELECT * FROM (SELECT 1 AS x INTERSECT DISTINCT SELECT 1.0) UNION ALL SELECT * FROM (SELECT 1 AS x INTERSECT DISTINCT SELECT 1.0));

SELECT 'join algorithms without semi joins keep the set-operation step';
SELECT * FROM (SELECT number AS x FROM numbers(5) INTERSECT DISTINCT SELECT number FROM numbers(3)) ORDER BY x SETTINGS join_algorithm = 'full_sorting_merge';
SELECT * FROM (SELECT number AS x FROM numbers(5) EXCEPT DISTINCT SELECT number FROM numbers(3)) ORDER BY x SETTINGS join_algorithm = 'partial_merge';

SELECT 'inside IN and a CTE';
SELECT count() FROM t_set_left WHERE a IN (SELECT a FROM t_set_left INTERSECT DISTINCT SELECT a FROM t_set_right);
WITH both AS (SELECT a FROM t_set_left INTERSECT DISTINCT SELECT a FROM t_set_right) SELECT count(), min(a), max(a) FROM both;
-- The rewritten CTE keeps its CTE name and flags, so the query tree dump and the rebuilt AST refer to it by name.
SELECT explain FROM (EXPLAIN QUERY TREE dump_ast = 1 WITH both AS MATERIALIZED (SELECT a FROM t_set_left INTERSECT DISTINCT SELECT a FROM t_set_right) SELECT count() FROM both SETTINGS enable_materialized_cte = 1)
WHERE explain LIKE '%cte_name%' OR explain LIKE '%both AS%';

SELECT 'ALL modes keep the set-operation step';
SELECT * FROM (SELECT number % 3 AS x FROM numbers(6) INTERSECT ALL SELECT number % 3 FROM numbers(3)) ORDER BY x;
SELECT * FROM (SELECT number % 3 AS x FROM numbers(6) EXCEPT ALL SELECT number % 3 FROM numbers(3)) ORDER BY x;

-- The join algorithm depends on the settings, and the optimizer may swap the join sides, which also moves the tree
-- drawing, so only the strictness and the conditions are kept.
-- Parallel replicas execute the whole join remotely, which changes the plan shape.
SET enable_parallel_replicas = 0;
SELECT 'plan';
SELECT replaceRegexpOne(replaceRegexpOne(explain, '^[ │├└─]+', ''), '^Type: \\w+ \\| (Strictness: \\w+).*$', '\\1') FROM (EXPLAIN SELECT a, b FROM t_set_left INTERSECT DISTINCT SELECT a, b FROM t_set_right)
WHERE explain LIKE '%Join%' OR explain LIKE '%Distinct%' OR explain LIKE '%IntersectOrExcept%';
SELECT replaceRegexpOne(replaceRegexpOne(explain, '^[ │├└─]+', ''), '^Type: \\w+ \\| (Strictness: \\w+).*$', '\\1') FROM (EXPLAIN SELECT a, b FROM t_set_left EXCEPT DISTINCT SELECT a, b FROM t_set_right)
WHERE explain LIKE '%Join%' OR explain LIKE '%Distinct%' OR explain LIKE '%IntersectOrExcept%';
SELECT replaceRegexpOne(replaceRegexpOne(explain, '^[ │├└─]+', ''), '^Type: \\w+ \\| (Strictness: \\w+).*$', '\\1') FROM (EXPLAIN SELECT * FROM (SELECT a, b FROM t_set_left INTERSECT DISTINCT SELECT a, b FROM t_set_right) SETTINGS optimize_rewrite_intersect_except_to_join = 0)
WHERE explain LIKE '%Join%' OR explain LIKE '%Distinct%' OR explain LIKE '%IntersectOrExcept%';
SELECT replaceRegexpOne(replaceRegexpOne(explain, '^[ │├└─]+', ''), '^Type: \\w+ \\| (Strictness: \\w+).*$', '\\1') FROM (EXPLAIN SELECT a, b FROM t_set_left INTERSECT ALL SELECT a, b FROM t_set_right)
WHERE explain LIKE '%Join%' OR explain LIKE '%Distinct%' OR explain LIKE '%IntersectOrExcept%';
-- A rewritten arm of a rewritten set operation drops its own DISTINCT.
SELECT replaceRegexpOne(replaceRegexpOne(explain, '^[ │├└─]+', ''), '^Type: \\w+ \\| (Strictness: \\w+).*$', '\\1') FROM (EXPLAIN SELECT a FROM t_set_left INTERSECT DISTINCT SELECT a FROM t_set_right INTERSECT DISTINCT SELECT a FROM t_set_third)
WHERE explain LIKE '%Join%' OR explain LIKE '%Distinct%' OR explain LIKE '%IntersectOrExcept%';

DROP TABLE t_set_left;
DROP TABLE t_set_right;
DROP TABLE t_set_third;
