-- Tags: no-random-merge-tree-settings

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;

CREATE TABLE nation(n_nationkey Int32, n_name String) ENGINE MergeTree ORDER BY n_nationkey;
CREATE TABLE customer(c_custkey Int32, c_nationkey Int32) ENGINE MergeTree ORDER BY c_custkey;

INSERT INTO nation VALUES (5,'ETHIOPIA'),(6,'FRANCE'),(7,'GERMANY');
INSERT INTO customer SELECT number, 5 - (number % 2) FROM numbers(500);

SET enable_parallel_replicas=0;
SET query_plan_join_swap_table=0;
SET join_algorithm='hash'; -- to make plan stable

-- RIGHT ANTI JOIN
SELECT REGEXP_REPLACE(explain, '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID')
FROM (
    EXPLAIN actions=1
    SELECT count()
    FROM customer
    WHERE NOT EXISTS (
        SELECT *
        FROM nation
        WHERE c_nationkey = n_nationkey
    )
)
SETTINGS correlated_subqueries_default_join_kind = 'right';

SELECT count()
FROM customer
WHERE NOT EXISTS (
    SELECT *
    FROM nation
    WHERE c_nationkey = n_nationkey
)
SETTINGS correlated_subqueries_default_join_kind = 'right';

-- LEFT ANTI JOIN
SELECT REGEXP_REPLACE(explain, '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID')
FROM (
    EXPLAIN actions=1
    SELECT count()
    FROM customer
    WHERE NOT EXISTS (
        SELECT *
        FROM nation
        WHERE c_nationkey = n_nationkey
    )
)
SETTINGS correlated_subqueries_default_join_kind = 'left';

SELECT count()
FROM customer
WHERE NOT EXISTS (
    SELECT *
    FROM nation
    WHERE c_nationkey = n_nationkey
)
SETTINGS correlated_subqueries_default_join_kind = 'left';

