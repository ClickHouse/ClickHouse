-- Lazy materialization over joins prunes the joins that the join order optimization rebuilt. Such a join can return
-- an input and output an alias of it, as for the columns of a subquery: pruning the alias removes the input too, and
-- the join must not return it any more.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_lazy_materialization_for_join = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS num_1;
DROP TABLE IF EXISTS num_2;

CREATE TABLE num_1 (key UInt64, value String) ENGINE = MergeTree ORDER BY key;
CREATE TABLE num_2 (key UInt64, value Int64) ENGINE = MergeTree ORDER BY key;

INSERT INTO num_1 SELECT number * 2, toString(number * 2) FROM numbers(100000);
INSERT INTO num_2 SELECT number * 3, -number FROM numbers(15000);

SELECT '-- all columns';
SELECT * FROM num_1 AS l INNER JOIN (SELECT num_2.key, value FROM num_2 INNER JOIN numbers(10000) AS nn ON num_2.key = nn.number) AS r ON l.key = r.key ORDER BY l.key LIMIT 5;

SELECT '-- the value of each side';
SELECT l.value, r.value FROM num_1 AS l INNER JOIN (SELECT num_2.key, value FROM num_2 INNER JOIN numbers(10000) AS nn ON num_2.key = nn.number) AS r ON l.key = r.key ORDER BY l.key LIMIT 5;

SELECT '-- the key of the subquery only';
SELECT r.key FROM num_1 AS l INNER JOIN (SELECT num_2.key, value FROM num_2 INNER JOIN numbers(10000) AS nn ON num_2.key = nn.number) AS r ON l.key = r.key ORDER BY l.key LIMIT 5;

SELECT '-- with an offset, as in 02967_parallel_replicas_join_algo_and_analyzer_2';
SELECT * FROM (SELECT key, value FROM num_1) AS l INNER JOIN (SELECT key, value FROM num_2 INNER JOIN (SELECT number * 7 AS key FROM numbers(10000)) AS nn ON num_2.key = nn.key) AS r ON l.key = r.key ORDER BY l.key LIMIT 5 OFFSET 50;

DROP TABLE num_1;
DROP TABLE num_2;
