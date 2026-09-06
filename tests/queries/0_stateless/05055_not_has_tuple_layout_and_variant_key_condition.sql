-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ with random settings.

SET explain_query_plan_default = 'legacy';
SET optimize_rewrite_has_to_in = 0;

-- { echo }

-- `has` compares two `Tuple` `Field`s positionally, but the cast of the set element to the key type
-- follows named-tuple semantics: between two *named* tuples it matches elements by name. A named
-- element whose names are ordered differently therefore stands for a different value in the set than
-- the one the runtime predicate compares, and an exact set atom built from it would prune rows that
-- satisfy `notHas`. Here the constant `(a = 1, b = 2)` matches the row `(b = 1, a = 2)` at runtime,
-- while the cast maps it onto `(b = 2, a = 1)`.
DROP TABLE IF EXISTS test_not_has_named_tuple;
CREATE TABLE test_not_has_named_tuple (k Tuple(b UInt8, a UInt8)) ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_not_has_named_tuple VALUES ((1, 2)), ((2, 1)), ((2, 1)), ((3, 3));

SELECT count() FROM test_not_has_named_tuple WHERE notHas([CAST((1, 2), 'Tuple(a UInt8, b UInt8)')], k);
SELECT count() FROM test_not_has_named_tuple WHERE notHas([CAST((1, 2), 'Tuple(a UInt8, b UInt8)')], k) SETTINGS use_primary_key = 0;
SELECT count() FROM test_not_has_named_tuple WHERE has([CAST((1, 2), 'Tuple(a UInt8, b UInt8)')], k) SETTINGS use_primary_key = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_not_has_named_tuple WHERE notHas([CAST((1, 2), 'Tuple(a UInt8, b UInt8)')], k)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';

-- A tuple without explicit names is always cast positionally, which is exactly what the runtime
-- comparison does, so the exact set atom stays and prunes.
SELECT count() FROM test_not_has_named_tuple WHERE notHas([tuple(toUInt8(2), toUInt8(1))], k);
SELECT count() FROM test_not_has_named_tuple WHERE notHas([tuple(toUInt8(2), toUInt8(1))], k) SETTINGS use_primary_key = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_not_has_named_tuple WHERE notHas([tuple(toUInt8(2), toUInt8(1))], k)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';

-- The same names in the same order describe the same layout, so they are admitted as well.
SELECT count() FROM test_not_has_named_tuple WHERE notHas([CAST((2, 1), 'Tuple(b UInt8, a UInt8)')], k);
SELECT count() FROM test_not_has_named_tuple WHERE notHas([CAST((2, 1), 'Tuple(b UInt8, a UInt8)')], k) SETTINGS use_primary_key = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_not_has_named_tuple WHERE notHas([CAST((2, 1), 'Tuple(b UInt8, a UInt8)')], k)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';

DROP TABLE test_not_has_named_tuple;

-- A `Dynamic` element carries no floating-point type at the type level, so the floating-point guard
-- has to look at the alternatives the constant column actually holds. `has([nan], nan)` is false
-- while the set index considers the two NaNs equal, so no set atom must be built.
DROP TABLE IF EXISTS test_not_has_dynamic_float;
CREATE TABLE test_not_has_dynamic_float (k Float64) ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_not_has_dynamic_float VALUES (1), (nan), (nan), (3);

SELECT count() FROM test_not_has_dynamic_float WHERE notHas(CAST([nan], 'Array(Dynamic)'), k);
SELECT count() FROM test_not_has_dynamic_float WHERE notHas(CAST([nan], 'Array(Dynamic)'), k) SETTINGS use_primary_key = 0;
SELECT count() FROM test_not_has_dynamic_float WHERE has(CAST([nan], 'Array(Dynamic)'), k) SETTINGS use_primary_key = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_not_has_dynamic_float WHERE notHas(CAST([nan], 'Array(Dynamic)'), k)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';

SELECT count() FROM test_not_has_dynamic_float WHERE notHas(CAST([2.0], 'Array(Dynamic)'), k);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_not_has_dynamic_float WHERE notHas(CAST([2.0], 'Array(Dynamic)'), k)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';

DROP TABLE test_not_has_dynamic_float;

-- A `Dynamic` element without floating-point values keeps its exact atom.
DROP TABLE IF EXISTS test_not_has_dynamic_int;
CREATE TABLE test_not_has_dynamic_int (k Int64) ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_not_has_dynamic_int VALUES (1), (2), (2), (3);

SELECT count() FROM test_not_has_dynamic_int WHERE notHas(CAST([2], 'Array(Dynamic)'), k);
SELECT count() FROM test_not_has_dynamic_int WHERE notHas(CAST([2], 'Array(Dynamic)'), k) SETTINGS use_primary_key = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_not_has_dynamic_int WHERE notHas(CAST([2], 'Array(Dynamic)'), k)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';

DROP TABLE test_not_has_dynamic_int;
