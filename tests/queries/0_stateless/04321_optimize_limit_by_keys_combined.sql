-- { echo }

SET enable_analyzer = 1;
SET optimize_limit_by_function_keys = 1;
SET optimize_injective_functions_in_limit_by = 1;

DROP TABLE IF EXISTS test;
CREATE TABLE test (g UInt32, x UInt32) ENGINE = MergeTree ORDER BY (g, x);
INSERT INTO test SELECT number % 3 AS g, number AS x FROM numbers(10);

-- Both passes act in one clause: function-keys drops `x + 1`, injective unwraps `toString(g)` to `g`.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT g FROM test ORDER BY x LIMIT 2 BY toString(g), x, x + 1;
SELECT g FROM test ORDER BY x LIMIT 2 BY toString(g), x, x + 1;

-- Function-keys removes an injective wrapper directly, since it is a function of `g`.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT g FROM test ORDER BY x LIMIT 2 BY g, toString(g);
SELECT g FROM test ORDER BY x LIMIT 2 BY g, toString(g);

-- A chain of redundant keys collapses to a single key.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT g FROM test ORDER BY x LIMIT 2 BY g, toString(g), g + 1, (g + 1) * 2;
SELECT g FROM test ORDER BY x LIMIT 2 BY g, toString(g), g + 1, (g + 1) * 2;

-- With both optimizations disabled nothing is removed and the result is unchanged.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT g FROM test ORDER BY x LIMIT 2 BY g, toString(g) SETTINGS optimize_limit_by_function_keys = 0, optimize_injective_functions_in_limit_by = 0;
SELECT g FROM test ORDER BY x LIMIT 2 BY g, toString(g) SETTINGS optimize_limit_by_function_keys = 0, optimize_injective_functions_in_limit_by = 0;

-- Aggregate keys are allowed and kept, while reducible keys around them are still optimized:
-- toString(g) and sum(x) + 1 are removed, g and sum(x) remain.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT g, sum(x) FROM test GROUP BY g ORDER BY g LIMIT 1 BY g, toString(g), sum(x), sum(x) + 1;
SELECT g, sum(x) FROM test GROUP BY g ORDER BY g LIMIT 1 BY g, toString(g), sum(x), sum(x) + 1;

DROP TABLE test;
