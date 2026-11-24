-- { echo }

SET enable_analyzer = 1;
SET optimize_limit_by_function_keys = 1;
SET optimize_injective_functions_in_limit_by = 1;
SET group_by_use_nulls = 1;

DROP TABLE IF EXISTS test;
CREATE TABLE test (g UInt32, x UInt32) ENGINE = MergeTree ORDER BY (g, x);
INSERT INTO test SELECT number % 3 AS g, number AS x FROM numbers(10);

-- Injective unwrap fires under group_by_use_nulls.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT g, count() FROM test GROUP BY g ORDER BY g LIMIT 2 BY toString(g);

-- Function-keys elimination fires under WITH ROLLUP.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT g, count() FROM test GROUP BY g WITH ROLLUP ORDER BY g LIMIT 2 BY g, g + 1;

-- Injective unwrap fires under WITH CUBE.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT g, count() FROM test GROUP BY g WITH CUBE ORDER BY g LIMIT 2 BY toString(g);

-- Injective unwrap fires under GROUPING SETS.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT g, count() FROM test GROUP BY GROUPING SETS ((g), ()) ORDER BY g LIMIT 2 BY toString(g);

-- The kept rows are unchanged with the optimizations on and off (ROLLUP with group_by_use_nulls).
SELECT g, count() AS c FROM test GROUP BY g WITH ROLLUP ORDER BY g, c LIMIT 2 BY toString(g);
SELECT g, count() AS c FROM test GROUP BY g WITH ROLLUP ORDER BY g, c LIMIT 2 BY toString(g) SETTINGS optimize_limit_by_function_keys = 0, optimize_injective_functions_in_limit_by = 0;

DROP TABLE test;
