CREATE TABLE nation(n_nationkey Int32, n_name String) ENGINE MergeTree ORDER BY n_nationkey;
CREATE TABLE customer(c_custkey Int32, c_nationkey Int32) ENGINE MergeTree ORDER BY c_custkey;

INSERT INTO nation VALUES (5,'ETHIOPIA'),(6,'FRANCE'),(7,'GERMANY');

INSERT INTO customer SELECT number, 5 FROM numbers(500);

SET enable_analyzer=1;
SET enable_parallel_replicas=0;
SET join_algorithm = 'hash,parallel_hash';

SELECT '-- Check that filter on c_nationkey is moved to PREWHERE';
SELECT REGEXP_REPLACE(trimLeft(explain), '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID')
FROM
(
    SELECT *
    FROM viewExplain('EXPLAIN', 'actions = 1', (
        SELECT
            count(),
            max(c_custkey)
        FROM customer, nation
        WHERE (c_nationkey = n_nationkey) AND (n_name = 'FRANCE')
        SETTINGS enable_join_runtime_filters = 1, optimize_move_to_prewhere = 1, query_plan_join_swap_table = 0
    ))
)
WHERE (explain LIKE '%ReadFromMergeTree%') OR (explain LIKE '%Prewhere filter column:%') OR (explain LIKE '%Build%');


SELECT '-- Check the same query but with swapped tables';
SELECT REGEXP_REPLACE(trimLeft(explain), '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID')
FROM
(
    SELECT *
    FROM viewExplain('EXPLAIN', 'actions = 1', (
        SELECT
            count(),
            max(c_custkey)
        FROM nation, customer
        WHERE (c_nationkey = n_nationkey) AND (n_name = 'FRANCE')
        SETTINGS enable_join_runtime_filters = 1, optimize_move_to_prewhere = 1, query_plan_join_swap_table = 1
    ))
)
WHERE (explain LIKE '%ReadFromMergeTree%') OR (explain LIKE '%Prewhere filter column:%') OR (explain LIKE '%Build%');
