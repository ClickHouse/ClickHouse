SET allow_experimental_analyzer = 1;
SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET use_statistics = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_transitive_predicates = 0;
SET query_plan_merge_filter_into_join_condition = 1;
SET cross_to_inner_join_rewrite = 0;
SET explain_query_plan_default = 'legacy';

CREATE TABLE dpsub_state_a (k UInt32, x UInt32 STATISTICS(uniq), one UInt8 STATISTICS(uniq)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE dpsub_state_b (k UInt32, x UInt32 STATISTICS(uniq), one UInt8 STATISTICS(uniq)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE dpsub_state_c (k UInt32, x UInt32 STATISTICS(uniq), one UInt8 STATISTICS(uniq)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE dpsub_state_d (k UInt32, x UInt32 STATISTICS(uniq), one UInt8 STATISTICS(uniq)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE dpsub_state_e (k UInt32, x UInt32 STATISTICS(uniq), one UInt8 STATISTICS(uniq)) ENGINE = MergeTree ORDER BY k;

INSERT INTO dpsub_state_a SELECT number, number % 100, 0 FROM numbers(1000);
INSERT INTO dpsub_state_b SELECT number, number % 10, 0 FROM numbers(100);
INSERT INTO dpsub_state_c SELECT number, number % 5, 0 FROM numbers(50);
INSERT INTO dpsub_state_d SELECT number, number % 20, 0 FROM numbers(200);
INSERT INTO dpsub_state_e SELECT number, number % 2, 0 FROM numbers(20);

SELECT 'dpsub';
SELECT explain
FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count()
    FROM dpsub_state_a AS a, dpsub_state_b AS b, dpsub_state_c AS c, dpsub_state_d AS d, dpsub_state_e AS e
    WHERE a.one = b.one AND a.one = c.one AND a.one = d.one AND a.one = e.one
        AND b.one = c.one AND b.one = d.one AND b.one = e.one
        AND c.one = d.one AND c.one = e.one AND d.one = e.one
        AND (a.x + b.x) = c.x
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub',
        query_plan_optimize_join_order_max_searched_plans = 20
)
WHERE explain LIKE '% Join:%' OR explain LIKE '% ResultRows:%';

SELECT 'dphyp,dpsub';
SELECT explain
FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count()
    FROM dpsub_state_a AS a, dpsub_state_b AS b, dpsub_state_c AS c, dpsub_state_d AS d, dpsub_state_e AS e
    WHERE a.one = b.one AND a.one = c.one AND a.one = d.one AND a.one = e.one
        AND b.one = c.one AND b.one = d.one AND b.one = e.one
        AND c.one = d.one AND c.one = e.one AND d.one = e.one
        AND (a.x + b.x) = c.x
    SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp,dpsub',
        query_plan_optimize_join_order_max_searched_plans = 20
)
WHERE explain LIKE '% Join:%' OR explain LIKE '% ResultRows:%';

SELECT 'dpsize,dpsub';
SELECT explain
FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count()
    FROM dpsub_state_a AS a, dpsub_state_b AS b, dpsub_state_c AS c, dpsub_state_d AS d, dpsub_state_e AS e
    WHERE a.one = b.one AND a.one = c.one AND a.one = d.one AND a.one = e.one
        AND b.one = c.one AND b.one = d.one AND b.one = e.one
        AND c.one = d.one AND c.one = e.one AND d.one = e.one
        AND (a.x + b.x) = c.x
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize,dpsub',
        query_plan_optimize_join_order_max_searched_plans = 20
)
WHERE explain LIKE '% Join:%' OR explain LIKE '% ResultRows:%';

DROP TABLE dpsub_state_a;
DROP TABLE dpsub_state_b;
DROP TABLE dpsub_state_c;
DROP TABLE dpsub_state_d;
DROP TABLE dpsub_state_e;
