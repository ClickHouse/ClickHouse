-- Validate that IN checks column count mismatch between left tuple and right subquery during analysis.
-- The validation lives in the analyzer, so force it on regardless of the CI variant.
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
-- must NOT be rejected at analysis: the whole left tuple is compared against it as one key, element by
-- element, and element-type compatibility is a runtime question. The check short-circuits on the
-- matching arity here rather than running the structural cast probe, which would otherwise reject this
-- valid same-arity comparison (an element cast may be structurally impossible even when the arity is
-- fine). Regression for `03989_set_low_cardinality_in_tuple`.
SELECT CAST((1, 2), 'Tuple(Nullable(UInt8), UInt8)') IN (SELECT CAST((1, 2), 'Tuple(UInt8, UInt8)'));

-- The same nullable-element left tuple, but compared against a single `Nullable(Tuple(...))` right column.
-- The set key type strips the top-level `Nullable` (default `transform_null_in = 0`), so this is a
-- same-arity one-key comparison and must NOT be rejected at analysis. The arity has to be detected after
-- unwrapping the right column's `Nullable`/`LowCardinality`, not only for a raw `Tuple` - otherwise a
-- `Nullable(Tuple(...))` right column would fall through to the structural probe and be misreported as a
-- column-count mismatch. Regression for the false positive flagged in PR #97540.
SELECT CAST((1, 2), 'Tuple(Nullable(UInt8), UInt8)') IN (SELECT CAST((1, 2), 'Nullable(Tuple(UInt8, UInt8))'));

-- A scalar is not a one-element tuple. Its cast to a single `Tuple` key must be validated during
-- analysis too, so folding the inner predicate cannot hide the runtime `TYPE_MISMATCH`.
SELECT 1 FROM (SELECT 2 AS c1 WHERE 1 IN (SELECT tuple(1))) t0 WHERE t0.c1 = 1; -- { serverError TYPE_MISMATCH }

-- A single non-tuple right column the whole left tuple cannot be cast to at all is a genuine mismatch
-- and is still rejected. The check probes castability at the type level with an empty left column, so a
-- fabricated default value (a `NULL` in a nullable element) is never the reason a query is rejected -
-- only a structurally impossible cast is. This query is not valid at runtime either: `Set::execute`
-- runs the same `Tuple(Nullable(UInt8), UInt8)` -> `Variant(UInt8, Tuple(UInt8, UInt8))` cast, which
-- throws `CANNOT_CONVERT_TYPE` because that tuple type is not one of the Variant's alternatives.
-- Regression for the probe-oracle concern flagged in PR #97540.
SELECT CAST((1, 2), 'Tuple(Nullable(UInt8), UInt8)') IN (SELECT CAST((1, 2), 'Variant(UInt8, Tuple(UInt8, UInt8))')); -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }

-- `x IN table` is a documented equivalent of `x IN (SELECT * FROM table)`, so the same column-count
-- validation must apply to a table (or `Set`) right-hand side. The right columns are the table's
-- ordinary columns, taken from the storage snapshot. In particular an empty one-column table (or `Set`)
-- previously let `(1, 1) IN table` reach `FunctionIn`'s empty-set fast path and silently return 0 instead
-- of erroring. Regression for the `IN table` gap flagged in PR #97540.
DROP TABLE IF EXISTS t_in_one_col;
DROP TABLE IF EXISTS t_in_two_col;
DROP TABLE IF EXISTS t_in_tuple_col;
DROP TABLE IF EXISTS s_in_one_col;

CREATE TABLE t_in_one_col (x UInt8) ENGINE = Memory;
CREATE TABLE t_in_two_col (x UInt8, y UInt8) ENGINE = Memory;
CREATE TABLE t_in_tuple_col (x Tuple(UInt8, UInt8)) ENGINE = Memory;
CREATE TABLE s_in_one_col (x UInt8) ENGINE = Set;

-- Empty tables: the arity mismatch must be caught during analysis, not folded into a silent 0.
SELECT (1, 1) IN t_in_one_col; -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }
SELECT 1 IN t_in_two_col; -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }
SELECT (1, 1, 1) IN t_in_two_col; -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }
SELECT (1, 1) IN s_in_one_col; -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }

-- Matching arity against a table is valid (the empty set returns 0).
SELECT (1, 1) IN t_in_two_col;
SELECT 1 IN t_in_one_col;

-- A single `Tuple` column of the same arity as the left tuple is a one-key comparison and must NOT be
-- rejected at analysis: the whole left tuple is compared against it as one key.
SELECT (1, 1) IN t_in_tuple_col;

DROP TABLE t_in_one_col;
DROP TABLE t_in_two_col;
DROP TABLE t_in_tuple_col;
DROP TABLE s_in_one_col;

-- A right-hand side that resolves to no columns must still be validated. A parameterized view keeps
-- no stored columns in its metadata, so its storage snapshot reports zero ordinary columns. When such
-- a shape is turned into a set, the set builder (like `buildQueryToReadColumnsFromTableExpression`)
-- synthesizes a single constant column to preserve the row count, so the effective right-hand side has
-- one column. The analysis-time check mirrors that: a multi-column left side against a zero-column
-- right-hand side is a genuine arity mismatch and must be caught here, not left to slip past the
-- `right_columns_count > 0` guard and reach `FunctionIn`'s empty-set fast path. Regression for the
-- zero-column RHS gap flagged in PR #97540.
DROP VIEW IF EXISTS v_in_param;
CREATE VIEW v_in_param AS SELECT number AS n FROM numbers(10) WHERE number = {pn:UInt64};
SELECT (1, 1) IN v_in_param; -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }
SELECT (1, 1, 1) IN v_in_param; -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }

-- The same zero-column shape reached through a table function goes through the analyzer's
-- `TableFunctionNode` right-hand-side rewrite, which now injects the same single constant column that
-- `buildQueryToReadColumnsFromTableExpression` appends when the storage exposes no ordinary columns, so
-- analysis and execution validate the same arity. Note that a table function cannot appear directly as
-- an IN right-hand side in the analyzer: it is parsed as an ordinary (scalar) function, so it is
-- rejected as `UNKNOWN_FUNCTION` before the rewrite, and a parameterized view reached through
-- `cluster()` / `remote()` is turned into a plain table node earlier in analysis (so it flows through
-- the `TableNode` branch validated above). These assertions pin that boundary. Regression for the
-- zero-column table-function RHS gap flagged in PR #97540.
SELECT (1, 1) IN cluster('test_shard_localhost', currentDatabase(), 'v_in_param'); -- { serverError UNKNOWN_FUNCTION }
SELECT (1, 1) IN remote('127.0.0.1', currentDatabase(), 'v_in_param'); -- { serverError UNKNOWN_FUNCTION }
DROP VIEW v_in_param;

-- A single right `Tuple` column is compared against the whole left value as one set key, so the left
-- side is only a genuine mismatch when it cannot be cast to that tuple at all. A scalar cannot
-- (`1 IN (SELECT tuple(1))` above), but a `String` is parsed into the tuple by the accurate cast, and
-- a tuple wrapped into `Nullable` is still a tuple key - `FunctionIn` keeps it as one key and `Set`
-- strips that wrapper (and `LowCardinality`) before comparing. None of these may be rejected during
-- analysis.
-- Regression for the over-broad scalar-versus-tuple rejection flagged in PR #97540.
SELECT '(1,2)' IN (SELECT CAST((1, 2), 'Tuple(UInt8, UInt8)'));
SELECT materialize('(1,3)') IN (SELECT CAST((1, 2), 'Tuple(UInt8, UInt8)'));
SET enable_nullable_tuple_type = 1;
SELECT CAST((1, 2), 'Nullable(Tuple(UInt8, UInt8))') IN (SELECT CAST((1, 2), 'Tuple(UInt8, UInt8)'));
SELECT CAST((1, 3), 'Nullable(Tuple(UInt8, UInt8))') IN (SELECT CAST((1, 2), 'Tuple(UInt8, UInt8)'));
SELECT CAST('(1,2)', 'LowCardinality(String)') IN (SELECT CAST((1, 2), 'Tuple(UInt8, UInt8)'));

-- A left operand whose type has a dynamic structure is rejected by `FunctionIn` itself with
-- `ILLEGAL_TYPE_OF_ARGUMENT`, before the set arity is ever considered - and that happens during
-- analysis, so constant folding cannot hide it either. The dynamic structure may come from a nested
-- member, as in `Tuple(Dynamic, UInt8)`, where the left side does look like a two-column tuple. The
-- analysis-time column-count check must not run for such types, otherwise it would replace that
-- existing unsupported-type contract with `NUMBER_OF_COLUMNS_DOESNT_MATCH`.
-- Regression for the nested dynamic-structure carrier flagged in PR #97540.
SELECT CAST((1, 2), 'Tuple(Dynamic, UInt8)') IN (SELECT 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT CAST((1, 2), 'Tuple(Dynamic, UInt8)') IN (SELECT 1, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT [CAST(1, 'Dynamic')] IN (SELECT 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
