-- Regression test for the bare-key index optimization (#89222) on a `Nullable` primary key.
--
-- Since #89222 a bare numeric key column used in a boolean context (`WHERE flag`) is read as
-- `flag != 0` (a `FUNCTION_NOT_IN_RANGE` over `[0, 0]`) for primary-key and skip-index analysis.
-- For a `Nullable` key, primary-key analysis maps a NULL key value to `+Inf` (`NULLS LAST`), so a
-- granule that holds only NULL looks definitely outside `[0, 0]` and the atom would report it as an
-- exact, definite match. But `WHERE flag` is NULL for those rows and filters them out, so the
-- exact-count / implicit-projection optimization (`SELECT count() ... WHERE flag` with
-- `optimize_use_implicit_projections = 1`) must not count such NULL-only granules without reading
-- them. The fix disables the bare-key atom for `Nullable` keys (it is left `FUNCTION_UNKNOWN`), so
-- those rows are read and filtered, which is correct.
--
-- The key type must itself be a valid filter type (`UInt8` / `Bool`): a bare `Nullable(Int32)` /
-- `Nullable(Float64)` in a boolean context is rejected by the projection filter check with
-- `ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER` (an orthogonal, pre-existing limitation), so those types
-- never reach the exact-count path at all.
--
-- Every assertion compares the optimized `count()` against a full-scan ground truth (`countIf`,
-- which has no WHERE and cannot prune) and must yield 1. Before the fix the NULL-only part was
-- counted as a definite match, so the implicit-projection form returned a larger count.

DROP TABLE IF EXISTS t_04490;

CREATE TABLE t_04490 (flag Nullable(UInt8))
ENGINE = MergeTree ORDER BY flag
SETTINGS index_granularity = 8192, allow_nullable_key = 1;

-- A part that is entirely NULL (a NULL-only granule, min = max = NULL -> +Inf).
INSERT INTO t_04490 SELECT NULL FROM numbers(100);
-- A part with non-null values, including a zero.
INSERT INTO t_04490 SELECT number % 2 FROM numbers(10);

SELECT '--- Nullable(UInt8) key: WHERE flag must not count NULL-only granules';
-- Implicit-projection / exact-count optimization explicitly enabled.
SELECT (SELECT count() FROM t_04490 WHERE flag SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1)
     = (SELECT countIf(flag != 0) FROM t_04490);
-- Negated form: `WHERE NOT flag` keeps only flag = 0; NULL stays filtered.
SELECT (SELECT count() FROM t_04490 WHERE NOT flag SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1)
     = (SELECT countIf(NOT (flag != 0)) FROM t_04490);
-- Same query with pruning, skip indexes and projections disabled must agree (sanity cross-check).
SELECT (SELECT count() FROM t_04490 WHERE flag SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1)
     = (SELECT count() FROM t_04490 WHERE flag SETTINGS use_partition_pruning = 0, use_skip_indexes = 0, optimize_use_projections = 0);

DROP TABLE t_04490;

SELECT '--- Nullable(Bool) key: same invariant';

DROP TABLE IF EXISTS t_04490_bool;

CREATE TABLE t_04490_bool (flag Nullable(Bool))
ENGINE = MergeTree ORDER BY flag
SETTINGS index_granularity = 8192, allow_nullable_key = 1;

INSERT INTO t_04490_bool SELECT NULL FROM numbers(100);
INSERT INTO t_04490_bool SELECT number % 2 = 1 FROM numbers(10);

SELECT (SELECT count() FROM t_04490_bool WHERE flag SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1)
     = (SELECT countIf(flag != 0) FROM t_04490_bool);

DROP TABLE t_04490_bool;
