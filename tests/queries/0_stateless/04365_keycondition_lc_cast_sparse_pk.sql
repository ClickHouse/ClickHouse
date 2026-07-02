-- Regression test for a `Bad cast from type DB::ColumnLowCardinality to DB::ColumnNullable`
-- LOGICAL_ERROR in primary-key range pruning. A `LowCardinality(...)` key column with an
-- executing monotonic CAST chain (e.g. `CAST(s, 'LowCardinality(String)')`) made
-- `applyFunction` feed the raw dictionary column to a `FunctionCast` wrapper that was built
-- for a `LowCardinality`-stripped source, which then `assert_cast`-ed it to `ColumnNullable`.
-- Both the lightweight (sparse) and the regular primary-key analysis paths are exercised.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_lc_cast_pk;

-- Two non-overlapping parts and a small granule so the index has single-value granules
-- (single_point), which is what makes the non-monotonic CAST-to-String chain actually execute.
CREATE TABLE t_lc_cast_pk (s LowCardinality(Nullable(Int32)))
ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 8, allow_nullable_key = 1;
INSERT INTO t_lc_cast_pk SELECT number FROM numbers(20);
INSERT INTO t_lc_cast_pk SELECT number + 1000 FROM numbers(20);

-- The original AST-fuzzer crash shape. Just running it must not abort.
SELECT '-- fuzzer crash shape (lightweight PK analysis)';
SELECT count() FROM t_lc_cast_pk
WHERE CAST(s, 'LowCardinality(String)') < toLowCardinality('(')
SETTINGS use_lightweight_primary_key_index_analysis = 1;

-- Correctness: the CAST-chain pruning result must match a full scan, on both PK-analysis paths.
-- toString(int) is lexicographic, so the answers below are non-trivial (neither 0 nor all rows).
SELECT '-- CAST chain pruning matches full scan (lightweight vs regular)';
SELECT
    countIf(CAST(s, 'String') < '5')  AS full_scan,
    (SELECT count() FROM t_lc_cast_pk WHERE CAST(s, 'String') < '5' SETTINGS use_lightweight_primary_key_index_analysis = 1) AS sparse,
    (SELECT count() FROM t_lc_cast_pk WHERE CAST(s, 'String') < '5' SETTINGS use_lightweight_primary_key_index_analysis = 0) AS regular
FROM t_lc_cast_pk;

SELECT '-- another comparison shape';
SELECT
    countIf(CAST(s, 'String') >= '10') AS full_scan,
    (SELECT count() FROM t_lc_cast_pk WHERE CAST(s, 'String') >= '10' SETTINGS use_lightweight_primary_key_index_analysis = 1) AS sparse,
    (SELECT count() FROM t_lc_cast_pk WHERE CAST(s, 'String') >= '10' SETTINGS use_lightweight_primary_key_index_analysis = 0) AS regular
FROM t_lc_cast_pk;

-- Nested CAST chain: the first CAST re-introduces a `LowCardinality(String)` argument type for the
-- second CAST, so a fix that just strips the wrapper would feed a full `ColumnString` to a function
-- built for an LC source, hitting the mirror `Bad cast ColumnString -> ColumnLowCardinality`. The
-- chain must coerce each cached column to its function's declared argument type. Just running it
-- must not abort, on both PK-analysis paths.
SELECT '-- nested CAST chain crash shape (lightweight PK analysis)';
SELECT count() FROM t_lc_cast_pk
WHERE CAST(CAST(s, 'LowCardinality(String)'), 'String') < '5'
SETTINGS use_lightweight_primary_key_index_analysis = 1;

SELECT '-- nested CAST chain pruning matches full scan (lightweight vs regular)';
SELECT
    countIf(CAST(CAST(s, 'LowCardinality(String)'), 'String') < '5') AS full_scan,
    (SELECT count() FROM t_lc_cast_pk WHERE CAST(CAST(s, 'LowCardinality(String)'), 'String') < '5' SETTINGS use_lightweight_primary_key_index_analysis = 1) AS sparse,
    (SELECT count() FROM t_lc_cast_pk WHERE CAST(CAST(s, 'LowCardinality(String)'), 'String') < '5' SETTINGS use_lightweight_primary_key_index_analysis = 0) AS regular
FROM t_lc_cast_pk;

DROP TABLE t_lc_cast_pk;
