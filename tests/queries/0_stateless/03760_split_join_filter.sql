CREATE TABLE nation(n_nationkey Int32, n_name String) ENGINE MergeTree ORDER BY n_nationkey;
CREATE TABLE customer(c_custkey Int32, c_nationkey Int32, c_nationkey_copy Int32) ENGINE MergeTree ORDER BY c_custkey SETTINGS index_granularity=10;

INSERT INTO nation VALUES (5,'ETHIOPIA'),(6,'FRANCE'),(7,'GERMANY'),(100,'UNKNOWN');

INSERT INTO customer SELECT number, 5, 5 FROM numbers(10000);
INSERT INTO customer SELECT number, 6, 6 FROM numbers(10000);
INSERT INTO customer SELECT number, 7, 7 FROM numbers(10000);
INSERT INTO customer SELECT number, 100, 100 FROM numbers(10);


SET enable_analyzer=1;
SET enable_parallel_replicas=0;
SET enable_join_runtime_filters=0;
SET query_plan_join_swap_table = 0;

SELECT explain FROM (
        EXPLAIN actions = 1
        SELECT count()
        FROM customer LEFT ANTI JOIN nation
        ON c_nationkey = n_nationkey AND n_name = 'FRANCE' AND c_custkey = 1
    )
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Prewhere filter column%' OR explain LIKE '%Type%' OR explain LIKE '%Strictness%';

SELECT explain FROM (
        EXPLAIN actions = 1
        SELECT count()
        FROM customer RIGHT ANTI JOIN nation
        ON c_nationkey = n_nationkey AND n_name = 'FRANCE' AND c_custkey = 1
    )
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Prewhere filter column%' OR explain LIKE '%Type%' OR explain LIKE '%Strictness%';

SELECT explain FROM (
        EXPLAIN actions = 1
        SELECT count()
        FROM customer LEFT SEMI JOIN nation
        ON c_nationkey = n_nationkey AND n_name = 'FRANCE' AND c_custkey = 1
    )
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Prewhere filter column%' OR explain LIKE '%Type%' OR explain LIKE '%Strictness%';

SELECT explain FROM (
        EXPLAIN actions = 1
        SELECT count()
        FROM customer RIGHT SEMI JOIN nation
        ON c_nationkey = n_nationkey AND n_name = 'FRANCE' AND c_custkey = 1
    )
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Prewhere filter column%' OR explain LIKE '%Type%' OR explain LIKE '%Strictness%';

SELECT explain FROM (
        EXPLAIN actions = 1
        SELECT count()
        FROM customer LEFT ANY JOIN nation
        ON c_nationkey = n_nationkey AND n_name = 'FRANCE' AND c_custkey = 1
    )
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Prewhere filter column%' OR explain LIKE '%Type%' OR explain LIKE '%Strictness%';

SELECT explain FROM (
        EXPLAIN actions = 1
        SELECT count()
        FROM customer RIGHT ANY JOIN nation
        ON c_nationkey = n_nationkey AND n_name = 'FRANCE' AND c_custkey = 1
    )
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Prewhere filter column%' OR explain LIKE '%Type%' OR explain LIKE '%Strictness%';
