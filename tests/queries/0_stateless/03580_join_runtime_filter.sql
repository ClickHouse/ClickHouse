CREATE TABLE nation(n_nationkey Int32, n_name String) ENGINE MergeTree ORDER BY n_nationkey;
CREATE TABLE customer(c_custkey Int32, c_nationkey Int32) ENGINE MergeTree ORDER BY c_custkey;
CREATE TABLE orders(o_orderkey Int32, o_custkey Int32, o_totalprice Decimal(15, 2)) ENGINE MergeTree ORDER BY o_orderkey;

INSERT INTO nation VALUES (5,'ETHIOPIA'),(6,'FRANCE'),(7,'GERMANY'),(100,'UNKNOWN');

INSERT INTO customer SELECT number, 5 FROM numbers(500);
INSERT INTO customer SELECT number, 6 FROM numbers(6000);
INSERT INTO customer SELECT number, 7 FROM numbers(70000);
INSERT INTO customer SELECT number, 201 FROM numbers(1);
INSERT INTO customer SELECT number, 202 FROM numbers(2);

INSERT INTO orders SELECT number, number/10000, number%1000 FROM numbers(1000000);

SET enable_analyzer=1;
SET enable_join_runtime_filters=1;
SET enable_parallel_replicas=0;
SET join_algorithm = 'hash,parallel_hash';

SELECT avg(o_totalprice)
FROM orders, customer, nation
WHERE c_custkey = o_custkey AND c_nationkey = n_nationkey AND n_name = 'FRANCE';

-- 1 element in filter
SELECT count()
FROM customer, nation
WHERE c_nationkey = n_nationkey AND n_name = 'FRANCE';

-- 0 elements in filter ('WAKANDA' is not present in `nations` table)
SELECT count()
FROM customer, nation
WHERE c_nationkey = n_nationkey AND n_name = 'WAKANDA';

-- again 1 element in filter
SELECT count()
FROM customer, nation
WHERE c_nationkey = n_nationkey AND n_name IN ('WAKANDA', 'FRANCE');

-- 2 elements in filter
SELECT count()
FROM customer, nation
WHERE c_nationkey = n_nationkey AND n_name IN ('GERMANY', 'FRANCE');

-- 2 elements in filter store in a bloom filter
SELECT count()
FROM customer, nation
WHERE c_nationkey = n_nationkey AND n_name IN ('GERMANY', 'FRANCE')
SETTINGS join_runtime_filter_exact_values_limit=1;

SELECT count()
FROM customer, nation
WHERE c_nationkey = n_nationkey+1 AND n_name = 'FRANCE';

SELECT count()
FROM customer, nation
WHERE c_nationkey+1 = n_nationkey AND n_name = 'FRANCE';

SELECT count()
FROM customer, nation
WHERE c_nationkey+1 = n_nationkey+1 AND n_name = 'FRANCE';

SELECT count()
FROM customer, nation
WHERE c_nationkey+1 = n_nationkey+1 AND n_name = 'FRANCE'
SETTINGS enable_join_runtime_filters = 1, join_runtime_bloom_filter_bytes = 100500000; -- {serverError PARAMETER_OUT_OF_BOUND}

SELECT count()
FROM customer, nation
WHERE c_nationkey+1 = n_nationkey+1 AND n_name = 'FRANCE'
SETTINGS enable_join_runtime_filters = 1, join_runtime_bloom_filter_hash_functions = 20; -- {serverError PARAMETER_OUT_OF_BOUND}

SELECT count()
FROM customer, nation
WHERE c_nationkey = n_nationkey AND n_name = 'FRANCE'
SETTINGS enable_join_runtime_filters = 1, join_runtime_bloom_filter_bytes = 4096, join_runtime_bloom_filter_hash_functions = 2;

-- Join algorithm that doesn't support runtime filters
SELECT REGEXP_REPLACE(trimLeft(explain), '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID')
FROM (
    EXPLAIN actions=1
    SELECT count()
    FROM customer, nation
    WHERE c_nationkey = n_nationkey AND n_nationkey%10 = c_custkey%100
    SETTINGS query_plan_join_swap_table=0, join_algorithm='full_sorting_merge'
)
WHERE (explain ILIKE '%Filter column%') OR (explain LIKE '%BuildRuntimeFilter%') OR (explain LIKE '%FullSorting%');

-- Filters on multiple join predicates
SELECT REGEXP_REPLACE(trimLeft(explain), '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID')
FROM (
    EXPLAIN actions=1
    SELECT count()
    FROM customer, nation
    WHERE c_nationkey = n_nationkey AND n_nationkey%10 = c_custkey%100
    SETTINGS query_plan_join_swap_table=0
)
WHERE (explain ILIKE '%Filter column%') OR (explain LIKE '%BuildRuntimeFilter%');

SELECT count()
FROM customer, nation
WHERE c_nationkey = n_nationkey AND n_nationkey%10 = c_custkey%100;


-- ANY JOIN
SELECT REGEXP_REPLACE(trimLeft(explain), '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID')
FROM (
    EXPLAIN actions=1
    SELECT count()
    FROM customer ANY JOIN nation
    ON c_nationkey = n_nationkey
    SETTINGS query_plan_join_swap_table=0
)
WHERE (explain ILIKE '%Filter column%') OR (explain LIKE '%BuildRuntimeFilter%') OR (explain LIKE '% Type:%') OR (explain LIKE '% Strictness:%');

SELECT count()
FROM customer ANY JOIN nation
ON c_nationkey = n_nationkey;


-- LEFT SEMI JOIN
SELECT REGEXP_REPLACE(trimLeft(explain), '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID')
FROM (
    EXPLAIN actions=1
    SELECT count()
    FROM customer LEFT SEMI JOIN nation
    ON c_nationkey = n_nationkey
    SETTINGS query_plan_join_swap_table=0
)
WHERE (explain ILIKE '%Filter column%') OR (explain LIKE '%BuildRuntimeFilter%') OR (explain LIKE '% Type:%') OR (explain LIKE '% Strictness:%');

SELECT count()
FROM customer LEFT SEMI JOIN nation
ON c_nationkey = n_nationkey;


-- RIGHT SEMI JOIN
SELECT REGEXP_REPLACE(trimLeft(explain), '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID')
FROM (
    EXPLAIN actions=1
    SELECT n_name
    FROM customer RIGHT SEMI JOIN nation
    ON c_nationkey = n_nationkey
    ORDER BY ALL
    SETTINGS query_plan_join_swap_table=0
)
WHERE (explain ILIKE '%Filter column%') OR (explain LIKE '%BuildRuntimeFilter%') OR (explain LIKE '% Type:%') OR (explain LIKE '% Strictness:%');

SELECT n_name
FROM customer RIGHT SEMI JOIN nation
ON c_nationkey = n_nationkey
ORDER BY ALL;
