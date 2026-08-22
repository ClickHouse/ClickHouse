-- Tests for https://github.com/ClickHouse/ClickHouse/issues/43144
-- The `values` table function (and other non-strict Field conversions) must accept decimal
-- literals for floating-point columns. Most decimals are not exactly representable in a
-- narrower float, so requiring exact equality made `values` reject them; the conversion to the
-- nearest representable value is expected (like `CAST`), not an error.

SELECT '-- values() with Float32: lossy decimal literals are accepted (issue #43144)';
SELECT x, y FROM values('x Int8, y Float32', (0, 0.1), (1, 0.34), (2, 0.88), (3, -1.23), (4, -3.3), (5, 5.4)) ORDER BY x;

SELECT '-- single Float32 column';
SELECT x FROM values('x Float32', 0.1, 0.5, 1) ORDER BY x;

SELECT '-- the converted value matches CAST to Float32';
SELECT x = CAST(0.1, 'Float32') FROM values('x Float32', 0.1);

SELECT '-- BFloat16 and Float64 columns';
SELECT x FROM values('x BFloat16', 0.5);
SELECT x FROM values('x Float64', 0.1);

SELECT '-- BFloat16 also accepts inexact decimal literals (the lossy path covers all narrow floats)';
-- `0.5` above is exactly representable in BFloat16, so it does not exercise the lossy conversion. An
-- inexact literal such as `0.1` does: `is_floating_point` in `convertFieldToType` is the ClickHouse
-- concept (`std::is_floating_point_v<T> || std::is_same_v<T, BFloat16>`), so BFloat16 is a floating-point
-- type there and `convert_inexact_floats` rounds `0.1` to the nearest BFloat16 (like CAST) rather than
-- rejecting it. The materialized value must equal `CAST(0.1, 'BFloat16')`, not error.
SELECT x = CAST(0.1, 'BFloat16') FROM values('x BFloat16', 0.1);

SELECT '-- out-of-range values are still rejected (no silent overflow to inf or wrap-around)';
SELECT x FROM values('x Float32', 1e300); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT x FROM values('x Int8', 256); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT x FROM values('x UInt8', -1); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT '-- IN keeps exact value semantics, consistent with the = operator';
-- The Float64 literal 0.1 is not representable in Float32, so it is excluded from the set,
-- exactly like `toFloat32(0.1) = 0.1` is false. The strict conversion used by IN is unchanged.
SELECT toFloat32(0.1) IN (0.1), toFloat32(0.1) = 0.1;
SELECT toFloat32(0.1) IN (toFloat32(0.1)), toFloat32(0.1) = toFloat32(0.1);
SELECT 0.1 IN (toFloat32(0.1)), 0.1 = toFloat32(0.1);
-- 0.5 is exactly representable in Float32, so it matches in both IN and =.
SELECT toFloat32(0.5) IN (0.5), toFloat32(0.5) = 0.5;

-- A strict IN set built for a `Variant(Float64)` target must not round a value that is not
-- exactly representable in Float64 (e.g. the maximum UInt64). The exact value cannot be a set
-- element, so this errors instead of silently matching the rounded Float64: the strict
-- conversion used by IN must not fall back to a lossy alternative for Variant targets either.
SELECT CAST(toFloat64(toUInt64(-1)), 'Variant(Float64)') IN (toUInt64(-1)); -- { serverError TYPE_MISMATCH }

SELECT '-- same IN result without the analyzer';
SELECT toFloat32(0.1) IN (0.1) SETTINGS allow_experimental_analyzer = 0;
SELECT toFloat32(0.5) IN (0.5) SETTINGS allow_experimental_analyzer = 0;

SELECT '-- the fall-through Variant conversion path prefers a lossless alternative, falling back to lossy only when needed';
-- When a field is not directly insertable and has to be converted, the converter tries an exact
-- (strict) conversion across all alternatives first. So the exactly-representable Int64 values stay
-- in the Array(Int64) alternative instead of being stored lossily in the earlier-listed (sorted)
-- Array(Float64) alternative.
-- These cases need the analyzer: the old query analysis eagerly computes a common type for the
-- array literal (mixing huge Int64 with Float64) and rejects it with NO_COMMON_TYPE before the
-- value reaches the Variant column type.
SELECT x FROM values('x Array(Variant(Array(Int64), Array(Float64)))', [[9223372036854775807, -9223372036854775808], [0.9999, 7]]) SETTINGS allow_experimental_analyzer = 1;
-- When no alternative can represent the value exactly, the nearest representable value is used.
SELECT x FROM values('x Array(Variant(Array(Float32), Array(String)))', [[0.1, 0.2]]) SETTINGS allow_experimental_analyzer = 1;

SELECT '-- the exact-first preference does not extend to a directly-insertable field (pre-existing ColumnVariant order, unchanged)';
-- This documents the scope of the contract above: the exact-first preference only governs the
-- fall-through conversion loop. When a field is directly insertable, the alternative is chosen by
-- ColumnVariant's pre-existing insertion order (variants in sorted order: `Float32` before
-- `Float64`), not by `convertFieldToType`. So a Float64 value exact in `Float64` but inexact in
-- `Float32` (16777217 = 2^24 + 1) is stored lossily in the earlier `Float32` alternative and
-- rounded to 16777216. Reworking that selection is a core `ColumnVariant` change outside the scope
-- of this fix. `Variant(Float32, Float64)` is itself a suspicious type (the two float alternatives
-- are ambiguous), so it requires `allow_suspicious_variant_types`.
SELECT x, variantType(x) FROM values('x Variant(Float32, Float64)', toFloat64(16777217)) SETTINGS allow_experimental_analyzer = 1, allow_suspicious_variant_types = 1;

SELECT '-- INSERT ... VALUES with an inexact-float expression rounds to the nearest value, like the streaming literal path';
DROP TABLE IF EXISTS t_values_insert_float;
CREATE TABLE t_values_insert_float (x Float32) ENGINE = Memory;
-- `1 / 3` is an expression (not a literal), so with the expression-template parser disabled the row
-- goes through the single-expression evaluation fallback in `ValuesBlockInputFormat`. That path must
-- convert the inexact Float64 result to the nearest Float32 (like CAST), not reject it.
INSERT INTO t_values_insert_float SETTINGS input_format_values_deduce_templates_of_expressions = 0 VALUES (1 / 3);
SELECT x = CAST(1 / 3, 'Float32') FROM t_values_insert_float;
DROP TABLE t_values_insert_float;

SELECT '-- the default templated INSERT ... VALUES path accepts inexact-float literals in complex types (issue #43144)';
-- With `input_format_values_deduce_templates_of_expressions = 1` (the default), an expression such as
-- `arrayConcat([0.1], [])` makes the parser deduce an expression template. The captured array literal
-- gets its own deduced type `Array(Float64)`, so the per-row `convertFieldToType` in
-- `ConstantExpressionTemplate::parseLiteralAndAssertType` targets `Array(Float64)` (a lossless
-- Float64 -> Float64 conversion); the narrowing to the `Array(Float32)` column happens afterwards in
-- `evaluateAll` via `CAST` (nearest value). So these inexact-float rows are accepted and rounded, not
-- rejected or turned into NULL, even though the template path never passes `convert_inexact_floats`.
DROP TABLE IF EXISTS t_values_template_float;
CREATE TABLE t_values_template_float (x Array(Float32)) ENGINE = Memory;
INSERT INTO t_values_template_float SETTINGS input_format_values_deduce_templates_of_expressions = 1 VALUES (arrayConcat([0.1], [])), (arrayConcat([0.2], [])), (arrayConcat([0.123456789], []));
SELECT x, x = CAST([0.1], 'Array(Float32)') OR x = CAST([0.2], 'Array(Float32)') OR x = CAST([0.123456789], 'Array(Float32)') AS matches_cast, has(x, NULL) AS has_null FROM t_values_template_float ORDER BY x;
DROP TABLE t_values_template_float;

SELECT '-- same for a Tuple(Float32, Float32) column via the templated path';
DROP TABLE IF EXISTS t_values_template_tuple;
CREATE TABLE t_values_template_tuple (x Tuple(Float32, Float32)) ENGINE = Memory;
INSERT INTO t_values_template_tuple SETTINGS input_format_values_deduce_templates_of_expressions = 1 VALUES (tuple(0.1, 0.2)), (tuple(0.3, 0.4));
SELECT x, x = CAST((0.1, 0.2), 'Tuple(Float32, Float32)') OR x = CAST((0.3, 0.4), 'Tuple(Float32, Float32)') AS matches_cast FROM t_values_template_tuple ORDER BY x;
DROP TABLE t_values_template_tuple;

SELECT '-- same for a scalar Float32 column via the templated path';
DROP TABLE IF EXISTS t_values_template_scalar;
CREATE TABLE t_values_template_scalar (x Float32) ENGINE = Memory;
INSERT INTO t_values_template_scalar SETTINGS input_format_values_deduce_templates_of_expressions = 1 VALUES (0.1 + 0), (0.2 + 0);
SELECT x, x = CAST(0.1, 'Float32') OR x = CAST(0.2, 'Float32') AS matches_cast FROM t_values_template_scalar ORDER BY x;
DROP TABLE t_values_template_scalar;
