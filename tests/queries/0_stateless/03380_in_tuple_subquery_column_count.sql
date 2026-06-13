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
