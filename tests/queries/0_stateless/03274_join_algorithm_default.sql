SET query_plan_join_swap_table = false;
SET allow_experimental_analyzer = 1;
SET enable_parallel_replicas=0;
SET query_plan_optimize_join_order_limit = 0;

-- Test that with default join_algorithm setting, we are doing a parallel hash join

SELECT value == 'direct,parallel_hash,hash' FROM system.settings WHERE name = 'join_algorithm';

EXPLAIN PIPELINE
SELECT
    *
FROM
    (
        SELECT * FROM system.numbers LIMIT 100000
    ) t1
    JOIN
    (
        SELECT * FROM system.numbers LIMIT 100000
    ) t2
USING number
SETTINGS max_threads=16;

-- Test that join_algorithm = default does a hash join

SET join_algorithm='default';

SELECT value == 'default' FROM system.settings WHERE name = 'join_algorithm';

EXPLAIN PIPELINE
SELECT
    *
FROM
    (
        SELECT * FROM system.numbers LIMIT 100000
    ) t1
    JOIN
    (
        SELECT * FROM system.numbers LIMIT 100000
    ) t2
USING number
SETTINGS max_threads=16;

SET join_algorithm=DEFAULT; -- reset

-- Check that compat setting also achieves a hash join

SET compatibility='24.11';

EXPLAIN PIPELINE
SELECT
    *
FROM
    (
        SELECT * FROM system.numbers LIMIT 100000
    ) t1
    JOIN
    (
        SELECT * FROM system.numbers LIMIT 100000
    ) t2
USING number
SETTINGS max_threads=16;
