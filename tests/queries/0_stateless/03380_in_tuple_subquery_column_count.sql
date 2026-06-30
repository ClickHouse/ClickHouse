-- Validate that IN checks column count mismatch between left tuple and right subquery during analysis.
-- The validation lives in the new analyzer, so force it on regardless of the CI variant.
-- https://github.com/ClickHouse/ClickHouse/issues/74442

SET enable_analyzer = 1;

SELECT (1, 1) IN (SELECT 1); -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }
SELECT 1 IN (SELECT 1, 2); -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }
SELECT (1, 1, 1) IN (SELECT 1, 2); -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }

SELECT (1, 1) IN (SELECT 1, 2);

-- Single Tuple column on the right is valid (compared as a single value).
SELECT (1, 2) IN (SELECT CAST((1, 2), 'Tuple(UInt8, UInt8)'));

SET allow_experimental_nullable_tuple_type = 1;
SELECT (1, 2) IN (SELECT CAST((1, 2), 'Nullable(Tuple(UInt8, UInt8))'));

-- A `Nullable(Tuple(...))` left operand is compared as a single key column by `FunctionIn` (it unpacks
-- only a top-level non-nullable `Tuple`), so it must count as one column on the left. Otherwise this
-- real mismatch (1 left column vs 2 right columns) would slip through analysis and be hidden by folding.
SELECT CAST((1, 1), 'Nullable(Tuple(UInt8, UInt8))') IN (SELECT 1, 1); -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }
SELECT 1 FROM (SELECT 2 AS c1 WHERE CAST((1, 1), 'Nullable(Tuple(UInt8, UInt8))') IN (SELECT 1, 1)) AS t WHERE t.c1 = 1; -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }

-- `Tuple` is not allowed inside `LowCardinality` (only types for which `canBeInsideLowCardinality`
-- returns true are accepted), so we can't construct `LowCardinality(Tuple(...))` directly.
-- Instead, exercise the `removeLowCardinalityAndNullable` unwrap on both sides with scalar
-- `LowCardinality` values to make sure the validation does not produce false positives.
SELECT toLowCardinality(1) IN (SELECT 1, 2); -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }
SELECT 1 IN (SELECT toLowCardinality(1));
SELECT (1, 2) IN (SELECT toLowCardinality(1), toLowCardinality(2));

-- Original reproducer from the issue: previously silently returned empty result, now should error.
SELECT 1 FROM (SELECT 2 AS c1 WHERE (1, 1) IN (SELECT 1)) t0 WHERE t0.c1 = 1; -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }

-- A lambda expression as the left side of IN does not have a resolved result type at this point.
-- The validation must skip such cases instead of dereferencing a null type
-- (regression for the AST fuzzer crash from PR #97540).
SELECT (x -> x) IN (SELECT 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT 1 WHERE (x -> -1 * x) GLOBAL NOT IN (SELECT arrayJoin([1])); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A single right column that is not a `Tuple` but can still hold the whole left tuple as one key
-- must NOT be rejected: `FunctionIn` compares the left value against that one column as a single
-- key, so the query is valid as long as the left tuple can be cast to the right column type.
-- These are valid one-key comparisons (regression for the false positive flagged in PR #97540).
SET allow_experimental_dynamic_type = 1;
SET allow_experimental_variant_type = 1;

-- `Dynamic` can store a tuple value, so the whole left tuple is compared against it.
SELECT (1, 2) IN (SELECT CAST((1, 2), 'Dynamic'));
SELECT (1, 2, 3) IN (SELECT CAST((1, 2), 'Dynamic'));

-- `Variant` that can hold the whole `Tuple(...)` value.
SELECT (1, 2) IN (SELECT CAST((1, 2), 'Variant(UInt8, Tuple(UInt8, UInt8))'));
SELECT (1, 2) IN (SELECT CAST(materialize(5), 'Variant(UInt8, Tuple(UInt8, UInt8))'));

-- The left tuple is castable to `String`, so this is a valid (always-false here) comparison.
SELECT (1, 2) IN (SELECT 'x');
SELECT (1, 2) IN (SELECT CAST((1, 2), 'String'));

-- A single scalar column the left tuple cannot be cast to is still rejected (the original issue):
-- a `Tuple` cannot be compared as a single key against a `UInt8`.
SELECT (1, 2) IN (SELECT materialize(1)); -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }

-- A single right `Tuple` column of the same arity as the left tuple is always an arity match and
-- must NOT be rejected at analysis: the whole left tuple is compared against it as one key. Probing
-- only the left default value would wrongly reject this valid query - the default has a `NULL` in the
-- nullable element that cannot be cast to the non-nullable right element, yet the actual values
-- compare fine at runtime. Regression for `03989_set_low_cardinality_in_tuple`.
SELECT CAST((1, 2), 'Tuple(Nullable(UInt8), UInt8)') IN (SELECT CAST((1, 2), 'Tuple(UInt8, UInt8)'));

-- The same nullable-element left tuple, but compared against a single `Nullable(Tuple(...))` right column.
-- The set key type strips the top-level `Nullable` (default `transform_null_in = 0`), so this is a
-- same-arity one-key comparison and must NOT be rejected at analysis. The arity has to be detected after
-- unwrapping the right column's `Nullable`/`LowCardinality`, not only for a raw `Tuple` - otherwise the
-- probe of the left default `(NULL, 0)` fails to cast to `Nullable(Tuple(UInt8, UInt8))` and is misreported
-- as a column-count mismatch. Regression for the false positive flagged in PR #97540.
SELECT CAST((1, 2), 'Tuple(Nullable(UInt8), UInt8)') IN (SELECT CAST((1, 2), 'Nullable(Tuple(UInt8, UInt8))'));
