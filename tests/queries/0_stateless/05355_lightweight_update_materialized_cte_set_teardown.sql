-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: a materialized CTE referenced twice is sent to a remote replica as the initiator's `_materialized_cte_*` table, `UNKNOWN_TABLE`, https://github.com/ClickHouse/ClickHouse/issues/112642

-- A lightweight UPDATE / DELETE whose predicate is a constant `ignore(...)` is torn down before its `IN` set is built.
-- Nothing in the set's plan may start reading on that teardown: the MATERIALIZED CTE it reads is never written.

SET enable_materialized_cte = 1;
SET enable_lightweight_update = 1;
SET lightweight_delete_mode = 'lightweight_update_force';
SET max_threads = 3;

DROP TABLE IF EXISTS t_lwu_cte_teardown;

CREATE TABLE t_lwu_cte_teardown (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_cte_teardown VALUES (0, 0), (1, 1), (2, 2);

-- Negative LIMIT after GROUP BY and after UNION ALL.
DELETE FROM t_lwu_cte_teardown WHERE ignore(id IN (WITH c AS MATERIALIZED (SELECT number * 2 AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b GROUP BY ALL LIMIT -2));
UPDATE t_lwu_cte_teardown SET v = 100 WHERE ignore(id IN (WITH c AS MATERIALIZED (SELECT number * 2 AS x FROM numbers(3)) SELECT x FROM (SELECT x FROM c UNION ALL SELECT x FROM c) LIMIT -2));
-- Negative OFFSET.
DELETE FROM t_lwu_cte_teardown WHERE ignore(id IN (WITH c AS MATERIALIZED (SELECT number * 2 AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b GROUP BY a.x LIMIT 2 OFFSET -1));
-- Fractional LIMIT and OFFSET.
UPDATE t_lwu_cte_teardown SET v = 100 WHERE ignore(id IN (WITH c AS MATERIALIZED (SELECT number * 2 AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b GROUP BY a.x LIMIT 0.5));
DELETE FROM t_lwu_cte_teardown WHERE ignore(id IN (WITH c AS MATERIALIZED (SELECT number * 2 AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b GROUP BY a.x OFFSET 0.5));
-- GROUPING SETS.
UPDATE t_lwu_cte_teardown SET v = 100 WHERE ignore(id IN (WITH c AS MATERIALIZED (SELECT number * 2 AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b GROUP BY GROUPING SETS ((a.x), (b.x))));
-- A window function with PARTITION BY.
DELETE FROM t_lwu_cte_teardown WHERE ignore(id IN (WITH c AS MATERIALIZED (SELECT number * 2 AS x FROM numbers(3)) SELECT x FROM (SELECT x, count() OVER (PARTITION BY x) AS n FROM (SELECT x FROM c UNION ALL SELECT x FROM c)) WHERE n > 0));

SELECT * FROM t_lwu_cte_teardown ORDER BY id;

-- The same sets are still built when the predicate needs them: both select {0, 2, 4}.
UPDATE t_lwu_cte_teardown SET v = 7 WHERE id IN (WITH c AS MATERIALIZED (SELECT number * 2 AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b GROUP BY GROUPING SETS ((a.x), (b.x)));
SELECT * FROM t_lwu_cte_teardown ORDER BY id;
DELETE FROM t_lwu_cte_teardown WHERE id IN (WITH c AS MATERIALIZED (SELECT number * 2 AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b GROUP BY a.x LIMIT -3);
SELECT * FROM t_lwu_cte_teardown ORDER BY id;

DROP TABLE t_lwu_cte_teardown;
