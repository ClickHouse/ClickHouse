-- Tags: no-parallel-replicas
--
-- Pins that `optimize_redundant_comparisons` and the `notEquals` -> `notIn` merge both decline a
-- comparison of a value that carries its own type.
--
-- Every arm pins `optimize_redundant_comparisons` (the setting under test) and
-- `optimize_and_compare_chain` (randomized by the test runner, and its call site enables pruning
-- unconditionally), so no arm depends on the randomization.
--
-- `no-parallel-replicas`: a `Variant` nested inside a container does not survive the query-text
-- round trip to a secondary server. The folded `Field` prints as a bare literal, which re-parses as
-- a different type, and the internal `_CAST` back to the `Variant` then refuses it. #111136 fixed
-- that for a scalar `Variant` constant and records the nested case as not covered; the
-- `Array(Variant(...))` arms here need exactly the nested form. Independent of the pass under test:
-- it reproduces on master with `optimize_redundant_comparisons = 0`.

-- The pass runs only under the analyzer, and `EXPLAIN QUERY TREE` requires it.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_redundant_json;
DROP TABLE IF EXISTS t_redundant_array_dynamic;
DROP TABLE IF EXISTS t_redundant_array_variant;
DROP TABLE IF EXISTS t_redundant_variant_shared_field;
DROP TABLE IF EXISTS t_redundant_plain;
DROP TABLE IF EXISTS t_redundant_variant_top;

-- A top-level `JSON` comparison resolves to a plain `UInt8`, so the nullable-result screen does not
-- hold it aside; a top-level `Variant` or `Dynamic` resolves to `Nullable(UInt8)` and is held aside.
CREATE TABLE t_redundant_json (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_redundant_json VALUES (1, '{"k":1.0}');
SELECT 'json, excluding bound alone', count() FROM t_redundant_json
WHERE j >= '{"k":2}'::JSON
SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;
SELECT 'json, chain pruned', count() FROM t_redundant_json
WHERE j >= '{"k":1.0}'::JSON AND j >= '{"k":2}'::JSON
SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1;
SELECT 'json, chain kept', count() FROM t_redundant_json
WHERE j >= '{"k":1.0}'::JSON AND j >= '{"k":2}'::JSON
SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;

-- A `Dynamic` reached through a container: the elements are compared by `compareAt`.
CREATE TABLE t_redundant_array_dynamic (id UInt32, a Array(Dynamic)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_redundant_array_dynamic VALUES (1, [1.0::Float64]);
SELECT 'array(dynamic), excluding bound alone', count() FROM t_redundant_array_dynamic
WHERE a >= [2::Int64]::Array(Dynamic)
SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;
SELECT 'array(dynamic), chain pruned', count() FROM t_redundant_array_dynamic
WHERE a >= [1.0::Float64]::Array(Dynamic) AND a >= [2::Int64]::Array(Dynamic)
SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1;
SELECT 'array(dynamic), chain kept', count() FROM t_redundant_array_dynamic
WHERE a >= [1.0::Float64]::Array(Dynamic) AND a >= [2::Int64]::Array(Dynamic)
SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;

-- A `Variant` of static alternatives has no dynamic structure, and `Variant(Float64, Int64)` is the
-- pair whose name order and `Field` tag order are reversed.
CREATE TABLE t_redundant_array_variant (id UInt32, a Array(Variant(Float64, Int64))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_redundant_array_variant VALUES (1, [1.0::Float64]);
SELECT 'array(variant), excluding bound alone', count() FROM t_redundant_array_variant
WHERE a >= [2::Int64]::Array(Variant(Float64, Int64))
SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;
SELECT 'array(variant), chain pruned', count() FROM t_redundant_array_variant
WHERE a >= [1.0::Float64]::Array(Variant(Float64, Int64)) AND a >= [2::Int64]::Array(Variant(Float64, Int64))
SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1;
SELECT 'array(variant), chain kept', count() FROM t_redundant_array_variant
WHERE a >= [1.0::Float64]::Array(Variant(Float64, Int64)) AND a >= [2::Int64]::Array(Variant(Float64, Int64))
SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;

-- `Date` and `UInt64` are one `Field` type, so the two `notEquals` constants below are one key in the
-- map that drops exact duplicates, while `compareAt` separates them by discriminator.
CREATE TABLE t_redundant_variant_shared_field (id UInt32, a Array(Variant(Date, UInt64))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_redundant_variant_shared_field VALUES (1, [1::UInt64]);
SELECT 'shared field, chain pruned', count() FROM t_redundant_variant_shared_field
WHERE a != [toDate(1)]::Array(Variant(Date, UInt64)) AND a != [1::UInt64]::Array(Variant(Date, UInt64))
SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1;
SELECT 'shared field, chain kept', count() FROM t_redundant_variant_shared_field
WHERE a != [toDate(1)]::Array(Variant(Date, UInt64)) AND a != [1::UInt64]::Array(Variant(Date, UInt64))
SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;

-- Only the constant carries its own type here: `Array(UInt64)` is an ordinary type, and the
-- comparison is evaluated at the self-typed supertype, where `UInt64` sorts before `UInt8` by name.
CREATE TABLE t_redundant_plain (id UInt32, a Array(UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_redundant_plain VALUES (1, [1]);
SELECT 'self-typed constant, excluding bound alone', count() FROM t_redundant_plain
WHERE a >= [1::UInt8]::Array(Dynamic)
SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;
SELECT 'self-typed constant, chain pruned', count() FROM t_redundant_plain
WHERE a >= [toDate(5)]::Array(Dynamic) AND a >= [1::UInt8]::Array(Dynamic)
SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1;
SELECT 'self-typed constant, chain kept', count() FROM t_redundant_plain
WHERE a >= [toDate(5)]::Array(Dynamic) AND a >= [1::UInt8]::Array(Dynamic)
SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;

-- A chain long enough to be merged into `notIn`, which rebuilds its constant from a `Field` and so
-- cannot carry a discriminator. Both constant orders are pinned: which of the two the duplicate-key
-- map happens to keep decides the answer without the guard.
SELECT 'shared field 3-chain, chain kept', count() FROM t_redundant_variant_shared_field
WHERE a != [1::UInt64]::Array(Variant(Date, UInt64)) AND a != [toDate(1)]::Array(Variant(Date, UInt64)) AND a != [5::UInt64]::Array(Variant(Date, UInt64))
SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1, optimize_min_inequality_conjunction_chain_length = 100;
SELECT 'shared field 3-chain, excluding constant first', count() FROM t_redundant_variant_shared_field
WHERE a != [1::UInt64]::Array(Variant(Date, UInt64)) AND a != [toDate(1)]::Array(Variant(Date, UInt64)) AND a != [5::UInt64]::Array(Variant(Date, UInt64))
SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1, optimize_min_inequality_conjunction_chain_length = 3;
SELECT 'shared field 3-chain, excluding constant second', count() FROM t_redundant_variant_shared_field
WHERE a != [toDate(1)]::Array(Variant(Date, UInt64)) AND a != [1::UInt64]::Array(Variant(Date, UInt64)) AND a != [5::UInt64]::Array(Variant(Date, UInt64))
SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1, optimize_min_inequality_conjunction_chain_length = 3;
-- The merge is not gated by `optimize_redundant_comparisons`, so the chain reaches it with pruning off.
SELECT 'shared field 3-chain, merge armed with pruning off', count() FROM t_redundant_variant_shared_field
WHERE a != [1::UInt64]::Array(Variant(Date, UInt64)) AND a != [toDate(1)]::Array(Variant(Date, UInt64)) AND a != [5::UInt64]::Array(Variant(Date, UInt64))
SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1, optimize_min_inequality_conjunction_chain_length = 3;

-- With `use_variant_default_implementation_for_comparisons = 0` a top-level `Variant` comparison
-- resolves to plain `UInt8`, so the nullable-result screen does not hold it aside. Every `Float64`
-- alternative sorts before every `Int64` one whatever the values are, so the two orders disagree here.
CREATE TABLE t_redundant_variant_top (id UInt32, b Variant(Float64, Int64), f Float64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_redundant_variant_top VALUES (1, 9e9::Float64, 9e9);
SELECT 'top-level variant, chain pruned', count() FROM t_redundant_variant_top
WHERE b >= 5.0::Float64::Variant(Float64, Int64) AND b >= 2::Int64::Variant(Float64, Int64)
SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1, use_variant_default_implementation_for_comparisons = 0;

-- Plan oracles. The counts above are all `0`, so they alone cannot tell a working guard from a pass
-- that never ran; each assertion below is paired with the same shape on an ordinary type, which is
-- what fails if the optimization stops firing altogether.
SELECT 'plan, self-typed range conjuncts kept', count() FROM
    (EXPLAIN QUERY TREE SELECT count() FROM t_redundant_array_dynamic
     WHERE a >= [1.0::Float64]::Array(Dynamic) AND a >= [2::Int64]::Array(Dynamic)
     SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1)
WHERE explain ILIKE '%function_name: greaterOrEquals,%';
SELECT 'plan, ordinary range conjuncts pruned', count() FROM
    (EXPLAIN QUERY TREE SELECT count() FROM t_redundant_plain
     WHERE a >= [1::UInt64] AND a >= [2::UInt64]
     SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1)
WHERE explain ILIKE '%function_name: greaterOrEquals,%';
SELECT 'plan, self-typed notIn merges', count() FROM
    (EXPLAIN QUERY TREE SELECT count() FROM t_redundant_variant_shared_field
     WHERE a != [1::UInt64]::Array(Variant(Date, UInt64)) AND a != [toDate(1)]::Array(Variant(Date, UInt64)) AND a != [5::UInt64]::Array(Variant(Date, UInt64))
     SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1, optimize_min_inequality_conjunction_chain_length = 3)
WHERE explain ILIKE '%function_name: notIn,%';
SELECT 'plan, ordinary notIn merges', count() FROM
    (EXPLAIN QUERY TREE SELECT count() FROM t_redundant_plain
     WHERE a != [1::UInt64] AND a != [2::UInt64] AND a != [3::UInt64]
     SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1, optimize_min_inequality_conjunction_chain_length = 3)
WHERE explain ILIKE '%function_name: notIn,%';
SELECT 'plan, self-typed notIn skipped with pruning off', count() FROM
    (EXPLAIN QUERY TREE SELECT count() FROM t_redundant_variant_shared_field
     WHERE a != [1::UInt64]::Array(Variant(Date, UInt64)) AND a != [toDate(1)]::Array(Variant(Date, UInt64)) AND a != [5::UInt64]::Array(Variant(Date, UInt64))
     SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1, optimize_min_inequality_conjunction_chain_length = 3)
WHERE explain ILIKE '%function_name: notIn,%';
SELECT 'plan, ordinary notIn merges with pruning off', count() FROM
    (EXPLAIN QUERY TREE SELECT count() FROM t_redundant_plain
     WHERE a != [1::UInt64] AND a != [2::UInt64] AND a != [3::UInt64]
     SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1, optimize_min_inequality_conjunction_chain_length = 3)
WHERE explain ILIKE '%function_name: notIn,%';
SELECT 'plan, top-level variant range conjuncts kept', count() FROM
    (EXPLAIN QUERY TREE SELECT count() FROM t_redundant_variant_top
     WHERE b >= 5.0::Float64::Variant(Float64, Int64) AND b >= 2::Int64::Variant(Float64, Int64)
     SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1, use_variant_default_implementation_for_comparisons = 0)
WHERE explain ILIKE '%function_name: greaterOrEquals,%';
SELECT 'plan, ordinary scalar range conjuncts pruned', count() FROM
    (EXPLAIN QUERY TREE SELECT count() FROM t_redundant_variant_top
     WHERE f >= 5.0 AND f >= 2
     SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1)
WHERE explain ILIKE '%function_name: greaterOrEquals,%';

DROP TABLE t_redundant_json;
DROP TABLE t_redundant_array_dynamic;
DROP TABLE t_redundant_array_variant;
DROP TABLE t_redundant_variant_shared_field;
DROP TABLE t_redundant_plain;
DROP TABLE t_redundant_variant_top;
