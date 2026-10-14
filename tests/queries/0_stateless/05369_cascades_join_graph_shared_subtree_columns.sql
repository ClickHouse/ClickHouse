-- The join orders of a graph share the plans of their common subtrees. With transitive predicates the join above a
-- subtree can use a different column of it in each order, so a shared plan is used only when it keeps every column
-- the order needs from it; otherwise building the plan of an order fails with a logical error. Random join
-- statistics give different orders, and these seeds give orders that need such a column.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET param__internal_cascades_cluster_node_count = 4;
SET query_plan_optimize_join_order_algorithm = 'dphyp';
SET cascades_join_order_alternatives = 4;
SET enable_join_transitive_predicates = 1;
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_jgs_sales;
DROP TABLE IF EXISTS t_jgs_customer;
DROP TABLE IF EXISTS t_jgs_date;
CREATE TABLE t_jgs_sales (c UInt64, d UInt64, v UInt64) ENGINE = MergeTree ORDER BY c;
CREATE TABLE t_jgs_customer (c UInt64, name String) ENGINE = MergeTree ORDER BY c;
CREATE TABLE t_jgs_date (d UInt64, y UInt64) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_jgs_sales SELECT number % 100, number % 20, number FROM numbers(10000);
INSERT INTO t_jgs_customer SELECT number, toString(number) FROM numbers(100);
INSERT INTO t_jgs_date SELECT number, 2000 + number % 2 FROM numbers(20);

WITH totals AS (
    SELECT cu.name AS name, dt.y AS y, sum(s.v) AS total
    FROM t_jgs_sales AS s JOIN t_jgs_customer AS cu ON s.c = cu.c JOIN t_jgs_date AS dt ON s.d = dt.d GROUP BY name, y
    UNION ALL
    SELECT cu.name AS name, dt.y AS y, sum(s.v) AS total
    FROM t_jgs_sales AS s JOIN t_jgs_customer AS cu ON s.c = cu.c JOIN t_jgs_date AS dt ON s.d = dt.d GROUP BY name, y)
SELECT count() FROM totals AS a JOIN totals AS b ON a.name = b.name JOIN totals AS c ON a.name = c.name JOIN totals AS d ON a.name = d.name
WHERE a.y = b.y AND b.y = c.y AND c.y = d.y
SETTINGS query_plan_optimize_join_order_randomize = 6;

WITH totals AS (
    SELECT cu.name AS name, dt.y AS y, sum(s.v) AS total
    FROM t_jgs_sales AS s JOIN t_jgs_customer AS cu ON s.c = cu.c JOIN t_jgs_date AS dt ON s.d = dt.d GROUP BY name, y
    UNION ALL
    SELECT cu.name AS name, dt.y AS y, sum(s.v) AS total
    FROM t_jgs_sales AS s JOIN t_jgs_customer AS cu ON s.c = cu.c JOIN t_jgs_date AS dt ON s.d = dt.d GROUP BY name, y)
SELECT count() FROM totals AS a JOIN totals AS b ON a.name = b.name JOIN totals AS c ON a.name = c.name JOIN totals AS d ON a.name = d.name
WHERE a.y = b.y AND b.y = c.y AND c.y = d.y
SETTINGS query_plan_optimize_join_order_randomize = 10;

WITH totals AS (
    SELECT cu.name AS name, dt.y AS y, sum(s.v) AS total
    FROM t_jgs_sales AS s JOIN t_jgs_customer AS cu ON s.c = cu.c JOIN t_jgs_date AS dt ON s.d = dt.d GROUP BY name, y
    UNION ALL
    SELECT cu.name AS name, dt.y AS y, sum(s.v) AS total
    FROM t_jgs_sales AS s JOIN t_jgs_customer AS cu ON s.c = cu.c JOIN t_jgs_date AS dt ON s.d = dt.d GROUP BY name, y)
SELECT count() FROM totals AS a JOIN totals AS b ON a.name = b.name JOIN totals AS c ON a.name = c.name JOIN totals AS d ON a.name = d.name
WHERE a.y = b.y AND b.y = c.y AND c.y = d.y
SETTINGS query_plan_optimize_join_order_randomize = 16;

WITH totals AS (
    SELECT cu.name AS name, dt.y AS y, sum(s.v) AS total
    FROM t_jgs_sales AS s JOIN t_jgs_customer AS cu ON s.c = cu.c JOIN t_jgs_date AS dt ON s.d = dt.d GROUP BY name, y
    UNION ALL
    SELECT cu.name AS name, dt.y AS y, sum(s.v) AS total
    FROM t_jgs_sales AS s JOIN t_jgs_customer AS cu ON s.c = cu.c JOIN t_jgs_date AS dt ON s.d = dt.d GROUP BY name, y)
SELECT count() FROM totals AS a JOIN totals AS b ON a.name = b.name JOIN totals AS c ON a.name = c.name JOIN totals AS d ON a.name = d.name
WHERE a.y = b.y AND b.y = c.y AND c.y = d.y
SETTINGS query_plan_optimize_join_order_randomize = 22;

WITH totals AS (
    SELECT cu.name AS name, dt.y AS y, sum(s.v) AS total
    FROM t_jgs_sales AS s JOIN t_jgs_customer AS cu ON s.c = cu.c JOIN t_jgs_date AS dt ON s.d = dt.d GROUP BY name, y
    UNION ALL
    SELECT cu.name AS name, dt.y AS y, sum(s.v) AS total
    FROM t_jgs_sales AS s JOIN t_jgs_customer AS cu ON s.c = cu.c JOIN t_jgs_date AS dt ON s.d = dt.d GROUP BY name, y)
SELECT count() FROM totals AS a JOIN totals AS b ON a.name = b.name JOIN totals AS c ON a.name = c.name JOIN totals AS d ON a.name = d.name
WHERE a.y = b.y AND b.y = c.y AND c.y = d.y
SETTINGS query_plan_optimize_join_order_randomize = 101099;

DROP TABLE t_jgs_sales;
DROP TABLE t_jgs_customer;
DROP TABLE t_jgs_date;
