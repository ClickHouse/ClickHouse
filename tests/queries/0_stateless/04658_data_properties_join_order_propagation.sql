SET send_logs_level = 'fatal';
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET enable_join_transitive_predicates = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'greedy';
-- Fixed randomized statistics make the ordinary estimates deterministic and deliberately imprecise.
SET query_plan_optimize_join_order_randomize = 42;
SET query_plan_join_swap_table = 0;

DROP TABLE IF EXISTS data_properties_first;
DROP TABLE IF EXISTS data_properties_second;
DROP TABLE IF EXISTS data_properties_third;

CREATE TABLE data_properties_first (first_id UInt64)
ENGINE = MergeTree
ORDER BY first_id
SETTINGS auto_statistics_types = '';

CREATE TABLE data_properties_second (second_first_id UInt64)
ENGINE = MergeTree
ORDER BY second_first_id
SETTINGS auto_statistics_types = '';

CREATE TABLE data_properties_third (third_first_id UInt64)
ENGINE = MergeTree
ORDER BY third_first_id
SETTINGS auto_statistics_types = '';

INSERT INTO data_properties_first VALUES (1), (2), (2);
INSERT INTO data_properties_second VALUES (2), (3), (3);
INSERT INTO data_properties_third VALUES (2), (2), (3);

-- The two `GROUP BY` subqueries expose unique grouping keys; the third relation remains non-unique.
-- The enabled case must preserve a key through the lower join before it can cap the top join.
SELECT '-- unique-key cardinality caps disabled --';
SET query_plan_optimize_join_order_use_proven_uniqueness = 0;
SELECT trimLeft(explain)
FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT *
    FROM (SELECT first_id FROM data_properties_first GROUP BY first_id) AS first
    INNER JOIN (SELECT second_first_id FROM data_properties_second GROUP BY second_first_id) AS second
        ON first_id = second_first_id
    INNER JOIN data_properties_third ON first_id = third_first_id
)
WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

SELECT '-- unique-key cardinality caps enabled: greedy --';
SET query_plan_optimize_join_order_use_proven_uniqueness = 1;
SELECT trimLeft(explain)
FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT *
    FROM (SELECT first_id FROM data_properties_first GROUP BY first_id) AS first
    INNER JOIN (SELECT second_first_id FROM data_properties_second GROUP BY second_first_id) AS second
        ON first_id = second_first_id
    INNER JOIN data_properties_third ON first_id = third_first_id
)
WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

-- A one-to-many lower join must not preserve the grouped key for the top join.
SELECT '-- one-to-many lower join drops the key --';
SET query_plan_optimize_join_order_algorithm = 'greedy';
SELECT trimLeft(explain)
FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT *
    FROM (SELECT first_id FROM data_properties_first GROUP BY first_id) AS first
    INNER JOIN data_properties_second AS second ON first_id = second_first_id
    INNER JOIN data_properties_third AS third ON first_id = third_first_id
)
WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

-- Identity aliases over grouped keys must retain source-qualified identity.
SELECT '-- identity aliases preserve the key --';
SET query_plan_optimize_join_order_algorithm = 'greedy';
SELECT trimLeft(explain)
FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT *
    FROM
    (
        SELECT first_id AS first_alias
        FROM (SELECT first_id FROM data_properties_first GROUP BY first_id)
    ) AS first
    INNER JOIN
    (
        SELECT second_first_id AS second_alias
        FROM (SELECT second_first_id FROM data_properties_second GROUP BY second_first_id)
    ) AS second ON first_alias = second_alias
    INNER JOIN data_properties_third ON first_alias = third_first_id
)
WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

-- Populated result multisets must remain identical across the feature gate and all algorithms.
SELECT '-- populated result equivalence --';
SET query_plan_optimize_join_order_use_proven_uniqueness = 0;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SELECT 'feature-off', count(), sum(first_id), sum(second_first_id), sum(third_first_id)
FROM (SELECT first_id FROM data_properties_first GROUP BY first_id) AS first
INNER JOIN (SELECT second_first_id FROM data_properties_second GROUP BY second_first_id) AS second ON first_id = second_first_id
INNER JOIN data_properties_third ON first_id = third_first_id;

SET query_plan_optimize_join_order_use_proven_uniqueness = 1;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SELECT 'greedy', count(), sum(first_id), sum(second_first_id), sum(third_first_id)
FROM (SELECT first_id FROM data_properties_first GROUP BY first_id) AS first
INNER JOIN (SELECT second_first_id FROM data_properties_second GROUP BY second_first_id) AS second ON first_id = second_first_id
INNER JOIN data_properties_third ON first_id = third_first_id;

DROP TABLE data_properties_first;
DROP TABLE data_properties_second;
DROP TABLE data_properties_third;
