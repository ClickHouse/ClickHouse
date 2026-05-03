-- Test NullCount statistics: Prewhere column ordering and fallback selectivity
-- All checks rely on `extractAll(explain, 'Prewhere filter column: ...')`,
-- so the test stays robust to EXPLAIN formatting and indentation changes.

SET allow_statistics = 1;
SET use_statistics = 1;
SET mutations_sync = 1;
SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET optimize_functions_to_subcolumns = 1;
SET materialize_statistics_on_insert = 1;
SET allow_reorder_prewhere_conditions = 1; -- CI may inject False, preventing statistics-based reordering of prewhere conditions

DROP TABLE IF EXISTS test_nullcount_prewhere;

-- Table with Nullable columns for IS NULL/IS NOT NULL prewhere ordering:
--   col_low_null:  10% NULL (10 rows NULL, 90 rows non-NULL)
--   col_high_null: 90% NULL (90 rows NULL, 10 rows non-NULL)
-- Plus c Int64 for mixed predicate tests (range + IS NULL)
CREATE TABLE test_nullcount_prewhere
(
    id UInt64,
    col_low_null Nullable(Int64),
    col_high_null Nullable(Int64),
    c Int64 STATISTICS(tdigest),
    range_probe Int64 STATISTICS(tdigest)
) ENGINE = MergeTree()
ORDER BY id
SETTINGS auto_statistics_types = '';

INSERT INTO test_nullcount_prewhere SELECT
    number,
    if(number % 10 = 0, NULL, number),
    if(number % 10 != 0, NULL, number),
    number,
    number
FROM numbers(100);

-- Add nullcount statistics for prewhere ordering tests
ALTER TABLE test_nullcount_prewhere ADD STATISTICS col_low_null TYPE nullcount;
ALTER TABLE test_nullcount_prewhere ADD STATISTICS col_high_null TYPE nullcount;
ALTER TABLE test_nullcount_prewhere MATERIALIZE STATISTICS col_low_null, col_high_null;

-- Test 1: IS NULL with nullcount (optimal: high_null first — more NULL rows → higher selectivity)
SELECT 'Test 1: IS NULL with nullcount (optimal: high_null first)';
SELECT position(prewhere_line, 'col_high_null') < position(prewhere_line, 'col_low_null') AS high_null_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_prewhere
        WHERE col_low_null IS NULL AND col_high_null IS NULL
    ) WHERE explain LIKE '%Prewhere filter column%'
);

-- Test 2: IS NOT NULL with nullcount (optimal: high_null first — fewer non-NULL rows pass)
SELECT 'Test 2: IS NOT NULL with nullcount (optimal: high_null first)';
SELECT position(prewhere_line, 'col_high_null') < position(prewhere_line, 'col_low_null') AS high_null_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_prewhere
        WHERE col_low_null IS NOT NULL AND col_high_null IS NOT NULL
    ) WHERE explain LIKE '%Prewhere filter column%'
);

-- Test 3: IS NULL + range with nullcount (range moved before IS NULL)
ALTER TABLE test_nullcount_prewhere ADD STATISTICS col_low_null TYPE minmax;
ALTER TABLE test_nullcount_prewhere MATERIALIZE STATISTICS col_low_null, col_high_null;

SELECT 'Test 3: IS NULL + range with nullcount';
SELECT position(prewhere_line, 'col_high_null') > position(prewhere_line, 'col_low_null') AS range_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_prewhere
        WHERE col_high_null IS NULL AND col_low_null < 5
    ) WHERE explain LIKE '%Prewhere filter column%'
);

-- Mixed predicates: IS NULL + range using c column (range moved before IS NULL)
SELECT 'Mixed predicates: IS NULL + range (col_low_null IS NULL AND c < 100)';
SELECT position(prewhere_line, 'less(') > 0 AS range_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_prewhere WHERE col_low_null IS NULL AND c < 100
    ) WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'Mixed predicates: IS NULL + range (col_high_null IS NULL AND c < 100)';
SELECT position(prewhere_line, 'less(') > 0 AS range_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_prewhere WHERE col_high_null IS NULL AND c < 100
    ) WHERE explain LIKE '%Prewhere filter column%'
);

-- Test 4: Nullable greater-than uses non-null row count (fewer matches → comes first)
SELECT 'Test 4: Nullable greater-than uses non-null row count';
SELECT
    position(prewhere_line, 'col_low_null') > 0
    AND position(prewhere_line, 'range_probe') > 0
    AND position(prewhere_line, 'col_low_null') < position(prewhere_line, 'range_probe')
FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_prewhere
        WHERE col_low_null > 95 AND range_probe < 5
    ) WHERE explain LIKE '%Prewhere filter column%'
);
SELECT count() FROM test_nullcount_prewhere WHERE col_low_null > 95;

DROP TABLE test_nullcount_prewhere;

-- =============================================================================
-- Fallback selectivity (no nullcount): IS NULL / IS NOT NULL still participate
-- in prewhere reordering correctly when only tdigest stats are available.
-- =============================================================================
DROP TABLE IF EXISTS test_fallback_no_nullcount;

CREATE TABLE test_fallback_no_nullcount (
    a Int64 STATISTICS(tdigest),
    b Nullable(Int64) STATISTICS(tdigest)  -- only tdigest, NO nullcount
) Engine = MergeTree() ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = '';

INSERT INTO test_fallback_no_nullcount
SELECT number, if(number % 2 = 0, NULL, number)
FROM numbers(10000);

-- Fallback (no nullcount): IS NOT NULL must not dominate prewhere ordering;
-- both range ('less') and IS NOT NULL ('not') should appear in the merged prewhere.
SELECT 'Fallback: IS NOT NULL without nullcount keeps both conditions in prewhere';
SELECT position(prewhere_line, 'less') > 0 AS has_range, position(prewhere_line, 'not') > 0 AS has_not FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_fallback_no_nullcount
        WHERE a < 100 AND b IS NOT NULL
    ) WHERE explain LIKE '%Prewhere filter column%'
);

-- Same for IS NULL: fallback (~0.01) keeps it as a normal participant.
SELECT 'Fallback: IS NULL without nullcount keeps both conditions in prewhere';
SELECT position(prewhere_line, 'greater') > 0 AS has_range, position(prewhere_line, '.null') > 0 AS has_null_check FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_fallback_no_nullcount
        WHERE a > 9900 AND b IS NULL
    ) WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'Actual counts for validation';
SELECT count() FROM test_fallback_no_nullcount WHERE b IS NULL;
SELECT count() FROM test_fallback_no_nullcount WHERE b IS NOT NULL;

DROP TABLE test_fallback_no_nullcount;

-- =============================================================================
-- Fallback selectivity uses non-null row count (nullcount-only columns):
-- column with more NULLs (b: 90% NULL) must be evaluated first because fewer
-- non-NULL rows pass. Without the fix, ordering would be driven by total rows.
-- =============================================================================
DROP TABLE IF EXISTS test_fallback_selectivity;

CREATE TABLE test_fallback_selectivity (
    a Nullable(Int64),
    b Nullable(Int64)
) Engine = MergeTree() ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = '';

-- a: 10% NULL (9000 non-null), b: 90% NULL (1000 non-null)
INSERT INTO test_fallback_selectivity
SELECT
    if(number % 10 = 0, NULL, number),
    if(number % 10 != 0, NULL, number)
FROM numbers(10000);

ALTER TABLE test_fallback_selectivity ADD STATISTICS a TYPE nullcount;
ALTER TABLE test_fallback_selectivity ADD STATISTICS b TYPE nullcount;
ALTER TABLE test_fallback_selectivity MATERIALIZE STATISTICS a, b SETTINGS mutations_sync = 1;

SELECT 'Fallback selectivity uses non-null row count (b before a)';
SELECT count() FROM (
    EXPLAIN actions=1 SELECT count() FROM test_fallback_selectivity WHERE a = 1 AND b = 1
) WHERE explain LIKE '%Prewhere filter column%b%a%';

DROP TABLE test_fallback_selectivity;
