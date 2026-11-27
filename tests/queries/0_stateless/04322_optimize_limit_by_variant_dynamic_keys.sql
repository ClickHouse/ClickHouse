-- { echo }

-- Injectivity is not guaranteed over Dynamic and Variant arguments: for a multi-type Variant, two
-- distinct values can share a string (42::UInt64 and '42'::String both stringify to '42'), so
-- toString is not injective there. The suspicious-type guard keeps such wrappers rather than
-- unwrapping them, which would change the partitioning and the result.

SET enable_analyzer = 1;
SET optimize_limit_by_function_keys = 0;
SET optimize_injective_functions_in_limit_by = 1;
SET allow_experimental_variant_type = 1;
SET allow_experimental_dynamic_type = 1;

DROP TABLE IF EXISTS test;
CREATE TABLE test (v Variant(UInt64, String), d Dynamic) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test VALUES (42, 42), ('42', '42');

-- A toString around a Variant key is kept, not unwrapped to the raw Variant.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT v FROM test LIMIT 1 BY toString(v);

-- The two distinct Variant values share the string '42', so keeping toString(v) groups them into one
-- partition. The result is the same with the optimization on and off.
SELECT count() FROM (SELECT v FROM test LIMIT 1 BY toString(v));
SELECT count() FROM (SELECT v FROM test LIMIT 1 BY toString(v)) SETTINGS optimize_injective_functions_in_limit_by = 0;

-- The same guard applies to a Dynamic key.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT d FROM test LIMIT 1 BY toString(d);
SELECT count() FROM (SELECT d FROM test LIMIT 1 BY toString(d));
SELECT count() FROM (SELECT d FROM test LIMIT 1 BY toString(d)) SETTINGS optimize_injective_functions_in_limit_by = 0;

DROP TABLE test;
