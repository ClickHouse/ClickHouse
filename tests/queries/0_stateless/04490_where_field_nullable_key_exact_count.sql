-- Regression test for the bare-key index optimization (#89222) on a Nullable primary key.
--
-- `WHERE id` is treated as `id != 0` for primary-key pruning. For a Nullable key, primary-key
-- analysis maps a NULL key value to `+Inf` (NULLS LAST), so a granule that holds only NULL looks
-- definitely outside `[0, 0]` and `id != 0` would report it as an exact, definite match. But
-- `WHERE id` is NULL for those rows and filters them out, so the exact-count / implicit-projection
-- optimization must not count such NULL-only granules without reading them.
--
-- Every assertion compares the optimized `count()` against a full-scan ground truth (`countIf`,
-- which has no WHERE and cannot use pruning) and must yield 1. Before the fix the NULL-only part
-- was counted as a definite match, so the implicit-projection forms returned a larger count.

DROP TABLE IF EXISTS t_04490;

CREATE TABLE t_04490 (id Nullable(Int32), value String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, allow_nullable_key = 1;

-- A part that is entirely NULL (a NULL-only granule, min = max = NULL -> +Inf).
INSERT INTO t_04490 SELECT NULL, toString(number) FROM numbers(100);
-- A part with non-null values, including a zero.
INSERT INTO t_04490 SELECT number - 5, toString(number) FROM numbers(10);

SELECT '--- Nullable(Int32) key: WHERE id must not count NULL-only granules';
-- Implicit-projection / exact-count optimization explicitly enabled.
SELECT (SELECT count() FROM t_04490 WHERE id SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1)
     = (SELECT countIf(id != 0) FROM t_04490);
-- Negated form: `WHERE NOT id` keeps only id = 0; NULL stays filtered.
SELECT (SELECT count() FROM t_04490 WHERE NOT id SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1)
     = (SELECT countIf(NOT (id != 0)) FROM t_04490);
-- Same query with pruning and skip indexes disabled must agree (sanity cross-check).
SELECT (SELECT count() FROM t_04490 WHERE id SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1)
     = (SELECT count() FROM t_04490 WHERE id SETTINGS use_partition_pruning = 0, use_skip_indexes = 0, optimize_use_projections = 0);

DROP TABLE t_04490;

SELECT '--- Nullable(Float64) key: same invariant';

DROP TABLE IF EXISTS t_04490_float;

CREATE TABLE t_04490_float (x Nullable(Float64), value String)
ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 8192, allow_nullable_key = 1;

INSERT INTO t_04490_float SELECT NULL, toString(number) FROM numbers(100);
INSERT INTO t_04490_float SELECT number - 5, toString(number) FROM numbers(10);

SELECT (SELECT count() FROM t_04490_float WHERE x SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1)
     = (SELECT countIf(x != 0) FROM t_04490_float);

DROP TABLE t_04490_float;
