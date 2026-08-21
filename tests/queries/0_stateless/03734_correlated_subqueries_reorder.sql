SET enable_analyzer = 1;

SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 1;

SET correlated_subqueries_substitute_equivalent_expressions = 0;
SET correlated_subqueries_use_in_memory_buffer = 1;

SET enable_parallel_replicas = 0;

SET query_plan_optimize_join_order_algorithm = 'dpsize';
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;

CREATE TABLE lineitem (
    l_orderkey       Int32,
    l_partkey        Int32,
    l_quantity       Decimal(15,2),
    l_extendedprice  Decimal(15,2),)
ORDER BY (l_orderkey)
SETTINGS auto_statistics_types = 'uniq';
INSERT INTO lineitem SELECT 1 as l_orderkey, number % 10 as l_partkey, toDecimal64(number % 100, 2) as l_quantity, toDecimal64(number % 1000, 2) as l_extendedprice FROM numbers(100);

CREATE TABLE part (p_partkey Int32) ORDER BY (p_partkey) SETTINGS auto_statistics_types = 'uniq';
INSERT INTO part SELECT * FROM numbers(10000);

EXPLAIN actions = 1, keep_logical_steps = 1
SELECT
    sum(l_extendedprice) / 7.0 AS avg_yearly
FROM
    lineitem,
    part
WHERE
    p_partkey = l_partkey
    AND l_quantity < (
        SELECT
            0.2 * avg(l_quantity)
        FROM
            lineitem
        WHERE
            l_partkey = p_partkey
    );

SELECT explain FROM (
EXPLAIN actions = 1, keep_logical_steps = 1
SELECT
    sum(l_extendedprice) / 7.0 AS avg_yearly
FROM
    (SELECT l_quantity, p_partkey, l_extendedprice FROM lineitem, part WHERE p_partkey = l_partkey) AS lp
WHERE
    l_quantity < (
        SELECT
            0.2 * avg(l_quantity)
        FROM
            lineitem
        WHERE
            l_partkey = p_partkey
    )
)
WHERE explain ilike '%ReadFrom%' or explain ilike '%JoinLogical%' or explain ilike '% Type: %' or explain ilike '%Save%';

SET query_plan_optimize_join_order_limit = 0;
SELECT '-- No join order optimization --';

SELECT explain FROM (
EXPLAIN actions = 1, keep_logical_steps = 1
SELECT
    sum(l_extendedprice) / 7.0 AS avg_yearly
FROM
    (SELECT l_quantity, p_partkey, l_extendedprice FROM lineitem, part WHERE p_partkey = l_partkey) AS lp
WHERE
    l_quantity < (
        SELECT
            0.2 * avg(l_quantity)
        FROM
            lineitem
        WHERE
            l_partkey = p_partkey
    )
)
WHERE explain ilike '%ReadFrom%' or explain ilike '%JoinLogical%' or explain ilike '% Type: %' or explain ilike '%Save%';


-- Test output now

CREATE VIEW v_query1 AS 
SELECT
    sum(l_extendedprice) / 7.0 AS avg_yearly
FROM
    lineitem,
    part
WHERE
    p_partkey = l_partkey
    AND l_quantity < (
        SELECT
            0.2 * avg(l_quantity)
        FROM
            lineitem
        WHERE
            l_partkey = p_partkey
    );

CREATE VIEW v_query2 AS 
SELECT
    sum(l_extendedprice) / 7.0 AS avg_yearly
FROM
    (SELECT l_quantity, p_partkey, l_extendedprice FROM lineitem, part WHERE p_partkey = l_partkey) AS lp
WHERE
    l_quantity < (
        SELECT
            0.2 * avg(l_quantity)
        FROM
            lineitem
        WHERE
            l_partkey = p_partkey
    );
    
-------------------------------------------
SET correlated_subqueries_use_in_memory_buffer = 1;

SELECT * FROM v_query1;
SELECT * FROM v_query2;

SET query_plan_optimize_join_order_limit = 0;

SELECT * FROM v_query1;
SELECT * FROM v_query2;

-------------------------------------------
SET query_plan_optimize_join_order_limit = 10;
SET correlated_subqueries_use_in_memory_buffer = 0;

SELECT * FROM v_query1;
SELECT * FROM v_query2;

SET query_plan_optimize_join_order_limit = 0;

SELECT * FROM v_query1;
SELECT * FROM v_query2;

DROP VIEW IF EXISTS v_query1;
DROP VIEW IF EXISTS v_query2;
DROP TABLE IF EXISTS lineitem;
DROP TABLE IF EXISTS part;