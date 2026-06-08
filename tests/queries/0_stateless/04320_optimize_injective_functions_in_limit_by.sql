-- { echo }

SET enable_analyzer = 1;
SET optimize_limit_by_function_keys = 0;
SET optimize_injective_functions_in_limit_by = 1;

DROP TABLE IF EXISTS test;
CREATE TABLE test (g UInt32, x UInt32) ENGINE = MergeTree ORDER BY (g, x);
INSERT INTO test SELECT number % 3 AS g, number AS x FROM numbers(10);

-- An injective function is replaced by its argument.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT g FROM test ORDER BY x LIMIT 2 BY toString(g);
SELECT g FROM test ORDER BY x LIMIT 2 BY toString(g);

-- Nested injective functions are fully unwrapped.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT g FROM test ORDER BY x LIMIT 2 BY toString(toString(g));
SELECT g FROM test ORDER BY x LIMIT 2 BY toString(toString(g));

-- A multi-argument injective function is replaced by all of its arguments.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT g FROM test ORDER BY x LIMIT 2 BY tuple(g, x);
SELECT g FROM test ORDER BY x LIMIT 2 BY tuple(g, x);

-- A non-injective function is kept.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT g FROM test ORDER BY x LIMIT 2 BY abs(x - 5);
SELECT g FROM test ORDER BY x LIMIT 2 BY abs(x - 5);

-- Constant arguments of an injective function are dropped; the remaining argument becomes the key.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT g FROM test ORDER BY x LIMIT 2 BY tuple(g, 1);
SELECT g FROM test ORDER BY x LIMIT 2 BY tuple(g, 1);

-- After unwrapping, toString(g) coincides with g and the duplicate is removed by the duplicate-elimination pass.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT g FROM test ORDER BY x LIMIT 2 BY toString(g), g;
SELECT g FROM test ORDER BY x LIMIT 2 BY toString(g), g;

-- A Variant argument is never exposed as a key, so the wrapper is kept.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT g FROM test ORDER BY x LIMIT 2 BY toString(CAST(g, 'Variant(UInt32)'));
SELECT g FROM test ORDER BY x LIMIT 2 BY toString(CAST(g, 'Variant(UInt32)'));

-- LIMIT BY inside a subquery is optimized.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT g FROM (SELECT g, x FROM test ORDER BY x LIMIT 2 BY toString(g));
SELECT g FROM (SELECT g, x FROM test ORDER BY x LIMIT 2 BY toString(g));

-- OFFSET and a negative limit are preserved while the wrapper is unwrapped.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT g FROM test ORDER BY x LIMIT 2 OFFSET 3 BY toString(g);
SELECT g FROM test ORDER BY x LIMIT 2 OFFSET 3 BY toString(g);
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT g FROM test ORDER BY x LIMIT -2 BY toString(g);
SELECT g FROM test ORDER BY x LIMIT -2 BY toString(g);

-- With the optimization disabled the wrapper is kept and the result is unchanged.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT g FROM test ORDER BY x LIMIT 2 BY toString(g) SETTINGS optimize_injective_functions_in_limit_by = 0;
SELECT g FROM test ORDER BY x LIMIT 2 BY toString(g) SETTINGS optimize_injective_functions_in_limit_by = 0;

-- A key that would unwrap to only constants is kept, so the LIMIT BY is never emptied: the single
-- constant partition still keeps three rows.
SELECT count() FROM (SELECT x FROM test ORDER BY x LIMIT 3 BY toString(1));

DROP TABLE test;
