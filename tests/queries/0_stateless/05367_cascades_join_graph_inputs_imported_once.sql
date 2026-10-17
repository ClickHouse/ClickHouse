-- The join order optimizer gives Cascades several orders of a join graph. Each input of the graph is imported once,
-- and every order is built over the same inputs, so the search does not grow with the size of the inputs. Here the
-- inputs are copies of a subquery with joins and aggregations; with a copy of every input per order the search
-- needs about 52000 tasks, with inputs imported once about 9000, so the query fits a budget of 25000 tasks.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET param__internal_cascades_cluster_node_count = 4;
SET query_plan_optimize_join_order_algorithm = 'dphyp';
SET cascades_join_order_alternatives = 4;
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_jg_sales;
DROP TABLE IF EXISTS t_jg_customer;
DROP TABLE IF EXISTS t_jg_date;
CREATE TABLE t_jg_sales (c UInt64, d UInt64, v UInt64) ENGINE = MergeTree ORDER BY c;
CREATE TABLE t_jg_customer (c UInt64, name String) ENGINE = MergeTree ORDER BY c;
CREATE TABLE t_jg_date (d UInt64, y UInt64) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_jg_sales SELECT number % 100, number % 20, number FROM numbers(10000);
INSERT INTO t_jg_customer SELECT number, toString(number) FROM numbers(100);
INSERT INTO t_jg_date SELECT number, 2000 + number % 2 FROM numbers(20);

WITH totals AS (
    SELECT cu.name AS name, dt.y AS y, sum(s.v) AS total
    FROM t_jg_sales AS s JOIN t_jg_customer AS cu ON s.c = cu.c JOIN t_jg_date AS dt ON s.d = dt.d GROUP BY name, y
    UNION ALL
    SELECT cu.name AS name, dt.y AS y, sum(s.v) AS total
    FROM t_jg_sales AS s JOIN t_jg_customer AS cu ON s.c = cu.c JOIN t_jg_date AS dt ON s.d = dt.d GROUP BY name, y)
SELECT count() FROM totals AS a JOIN totals AS b ON a.name = b.name JOIN totals AS c ON a.name = c.name JOIN totals AS d ON a.name = d.name
WHERE a.y = b.y AND b.y = c.y AND c.y = d.y
SETTINGS param__internal_cascades_task_limit = 25000;

WITH totals AS (
    SELECT cu.name AS name, dt.y AS y, sum(s.v) AS total
    FROM t_jg_sales AS s JOIN t_jg_customer AS cu ON s.c = cu.c JOIN t_jg_date AS dt ON s.d = dt.d GROUP BY name, y
    UNION ALL
    SELECT cu.name AS name, dt.y AS y, sum(s.v) AS total
    FROM t_jg_sales AS s JOIN t_jg_customer AS cu ON s.c = cu.c JOIN t_jg_date AS dt ON s.d = dt.d GROUP BY name, y)
SELECT count() FROM totals AS a JOIN totals AS b ON a.name = b.name JOIN totals AS c ON a.name = c.name JOIN totals AS d ON a.name = d.name
WHERE a.y = b.y AND b.y = c.y AND c.y = d.y
SETTINGS make_distributed_plan = 0;

DROP TABLE t_jg_sales;
DROP TABLE t_jg_customer;
DROP TABLE t_jg_date;
