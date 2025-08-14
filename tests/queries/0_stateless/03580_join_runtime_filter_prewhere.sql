CREATE TABLE nation(n_nationkey Int32, n_name String) ENGINE MergeTree ORDER BY n_nationkey;
CREATE TABLE customer(c_custkey Int32, c_nationkey Int32) ENGINE MergeTree ORDER BY c_custkey;

INSERT INTO nation VALUES (5,'ETHIOPIA'),(6,'FRANCE'),(7,'GERMANY');

INSERT INTO customer SELECT number, 5 FROM numbers(500);


SELECT '-- Check that filter on c_nationkey is moved to PREWHERE';
SELECT REGEXP_REPLACE(explain, '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID')
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
WHERE (explain LIKE '%ReadFromMergeTree%') OR (explain LIKE '%Prewhere filter column:%');
