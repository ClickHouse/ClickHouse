-- Tags: no-random-merge-tree-settings
-- Test for applying join runtime filters to RIGHT ANY joins

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;

CREATE TABLE nation(n_nationkey Int32, n_name String) ENGINE MergeTree ORDER BY n_nationkey;
CREATE TABLE customer(c_custkey Int32, c_nationkey Int32) ENGINE MergeTree ORDER BY c_custkey;

INSERT INTO nation VALUES (5,'ETHIOPIA'),(6,'FRANCE'),(7,'GERMANY');
INSERT INTO customer SELECT number, 5 - (number % 2) FROM numbers(500);

SET enable_parallel_replicas=0;
SET query_plan_join_swap_table=0;
SET join_algorithm='hash';

SELECT REGEXP_REPLACE(trimLeft(explain), '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID')
FROM (
    EXPLAIN actions=1
    SELECT count()
    FROM customer RIGHT ANY JOIN nation
    ON c_nationkey = n_nationkey
)
WHERE (explain ILIKE '%Filter column%') OR (explain LIKE '%BuildRuntimeFilter%') OR (explain LIKE '% Type:%') OR (explain LIKE '% Strictness:%');

SELECT count()
FROM customer RIGHT ANY JOIN nation
ON c_nationkey = n_nationkey
SETTINGS enable_join_runtime_filters = 0;

SELECT count()
FROM customer RIGHT ANY JOIN nation
ON c_nationkey = n_nationkey
SETTINGS enable_join_runtime_filters = 1;

-- No runtime filter for LEFT ANY
SELECT 'No runtime filter for LEFT ANY';
SELECT count()
FROM (
    EXPLAIN actions=1
    SELECT count()
    FROM nation LEFT ANY JOIN customer
    ON c_nationkey = n_nationkey
)
WHERE (explain ILIKE '%Filter column%') OR (explain LIKE '%BuildRuntimeFilter%');

-- 1 element in filter
SELECT '1 element in filter';
SELECT count()
FROM customer RIGHT ANY JOIN nation
ON c_nationkey = n_nationkey
WHERE n_name = 'ETHIOPIA';

-- 0 elements in filter
-- 'WAKANDA' is not present in `nations` table
SELECT '0 elements in filter';
SELECT count()
FROM customer RIGHT ANY JOIN nation
ON c_nationkey = n_nationkey
WHERE n_name = 'WAKANDA';

-- Again 1 element in filter
SELECT 'Again 1 element in filter';
SELECT count()
FROM customer RIGHT ANY JOIN nation
ON c_nationkey = n_nationkey
WHERE n_name IN ('WAKANDA', 'GERMANY');

-- 2 elements in filter
SELECT '2 elements in filter';
SELECT count()
FROM customer RIGHT ANY JOIN nation
ON c_nationkey = n_nationkey
WHERE n_name IN ('ETHIOPIA', 'GERMANY');

-- 2 elements in filter in bloom filter
SELECT '2 elements in filter in bloom filter';
SET join_runtime_filter_exact_values_limit = 1;

SELECT count()
FROM customer RIGHT ANY JOIN nation
ON c_nationkey = n_nationkey
WHERE n_name IN ('ETHIOPIA', 'GERMANY');