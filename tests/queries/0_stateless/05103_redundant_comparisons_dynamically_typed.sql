-- `optimize_redundant_comparisons` prunes the conditions of an `AND` chain by ordering the two
-- constants' `Field` values by their own type tag. A `Variant`, `Dynamic` or `JSON` value carries its
-- own type, and `IColumn::compareAt` orders such a value by that type's place in the name-sorted
-- alternative list before it looks at the value. For `Variant(Float64, Int64)` the two orders are
-- reversed, so the bound the analysis judged looser is the one that actually excludes the row, and
-- pruning it returned a row the `WHERE` clause excludes.
--
-- Every arm pins `optimize_redundant_comparisons` (the setting under test) and
-- `optimize_and_compare_chain` (randomized by the test runner, and its call site enables pruning
-- unconditionally), so no arm depends on the randomization.

DROP TABLE IF EXISTS t_redundant_json;
DROP TABLE IF EXISTS t_redundant_array_dynamic;
DROP TABLE IF EXISTS t_redundant_array_variant;
DROP TABLE IF EXISTS t_redundant_variant_shared_field;

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

DROP TABLE t_redundant_json;
DROP TABLE t_redundant_array_dynamic;
DROP TABLE t_redundant_array_variant;
DROP TABLE t_redundant_variant_shared_field;
