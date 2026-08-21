-- Test Basic statistics: Prewhere column ordering and fallback selectivity.
-- This mirrors the prewhere-ordering coverage that an earlier `nullcount`
-- prototype (see https://github.com/ClickHouse/ClickHouse/pull/102356) had,
-- now exercised through the `basic` statistic, which on a Nullable numeric
-- column populates both numeric min/max and the null count in one declaration.
-- All checks rely on `extractAll(explain, 'Prewhere filter column: ...')`,
-- so the test stays robust to EXPLAIN formatting and indentation changes.

SET explain_query_plan_default = 'legacy';
SET allow_statistics = 1;
SET use_statistics = 1;
SET mutations_sync = 1;
SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET optimize_functions_to_subcolumns = 1;
SET materialize_statistics_on_insert = 1;
SET allow_reorder_prewhere_conditions = 1; -- CI may inject False, preventing statistics-based reordering of prewhere conditions

DROP TABLE IF EXISTS test_basic_prewhere;

-- Table with Nullable columns for IS NULL/IS NOT NULL prewhere ordering:
--   col_low_null:  10% NULL (10 rows NULL, 90 rows non-NULL)
--   col_high_null: 90% NULL (90 rows NULL, 10 rows non-NULL)
-- Plus c Int64 for mixed predicate tests (range + IS NULL).
CREATE TABLE test_basic_prewhere
(
    id UInt64,
    col_low_null Nullable(Int64),
    col_high_null Nullable(Int64),
    c Int64 STATISTICS(tdigest),
    range_probe Int64 STATISTICS(tdigest)
) ENGINE = MergeTree()
ORDER BY id
-- Pin compact parts (per-column sizes = 0) so PREWHERE ordering is by selectivity alone, stable under CI-randomized part-type/serialization settings.
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 1000000000000, min_rows_for_wide_part = 1000000000000;

INSERT INTO test_basic_prewhere SELECT
    number,
    if(number % 10 = 0, NULL, number),
    if(number % 10 != 0, NULL, number),
    number,
    number
FROM numbers(100);

-- Add basic statistics for prewhere ordering tests. `basic` on a Nullable
-- numeric column tracks both numeric min/max and the null count in a single
-- declaration.
ALTER TABLE test_basic_prewhere ADD STATISTICS col_low_null TYPE basic;
ALTER TABLE test_basic_prewhere ADD STATISTICS col_high_null TYPE basic;
ALTER TABLE test_basic_prewhere MATERIALIZE STATISTICS col_low_null, col_high_null;

-- Test 1: IS NULL with basic — col_low_null (10 NULLs) is more selective than
-- col_high_null (90 NULLs), so col_low_null is moved to prewhere first.
SELECT 'Test 1: IS NULL with basic (col_low_null more selective)';
SELECT position(prewhere_line, 'col_high_null') < position(prewhere_line, 'col_low_null') AS high_null_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_basic_prewhere
        WHERE col_low_null IS NULL AND col_high_null IS NULL
    ) WHERE explain LIKE '%Prewhere filter column%'
);

-- Test 2: IS NOT NULL with basic — col_high_null (10 non-NULLs) is more
-- selective than col_low_null (90 non-NULLs), so col_high_null comes first.
SELECT 'Test 2: IS NOT NULL with basic (col_high_null more selective)';
SELECT position(prewhere_line, 'col_high_null') < position(prewhere_line, 'col_low_null') AS high_null_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_basic_prewhere
        WHERE col_low_null IS NOT NULL AND col_high_null IS NOT NULL
    ) WHERE explain LIKE '%Prewhere filter column%'
);

-- Test 3: IS NULL + range with basic — the range filter is much more selective
-- than IS NULL, so the range filter moves to prewhere first. No extra minmax
-- declaration is needed because `basic` already provides numeric min/max.
SELECT 'Test 3: IS NULL + range with basic';
SELECT position(prewhere_line, 'col_high_null') > position(prewhere_line, 'col_low_null') AS range_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_basic_prewhere
        WHERE col_high_null IS NULL AND col_low_null < 5
    ) WHERE explain LIKE '%Prewhere filter column%'
);

-- Mixed predicates: IS NULL + range using c column (range moved before IS NULL).
SELECT 'Mixed predicates: IS NULL + range (col_low_null IS NULL AND c < 100)';
SELECT position(prewhere_line, 'less(') > 0 AS range_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_basic_prewhere WHERE col_low_null IS NULL AND c < 100
    ) WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'Mixed predicates: IS NULL + range (col_high_null IS NULL AND c < 100)';
SELECT position(prewhere_line, 'less(') > 0 AS range_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_basic_prewhere WHERE col_high_null IS NULL AND c < 100
    ) WHERE explain LIKE '%Prewhere filter column%'
);

-- Test 4: Nullable greater-than uses non-null row count (fewer matches → comes
-- first). `basic` provides both min/max (for interpolation) and null count (so
-- the interpolation domain is the non-NULL row count).
SELECT 'Test 4: Nullable greater-than uses non-null row count';
SELECT
    position(prewhere_line, 'col_low_null') > 0
    AND position(prewhere_line, 'range_probe') > 0
    AND position(prewhere_line, 'col_low_null') < position(prewhere_line, 'range_probe')
FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_basic_prewhere
        WHERE col_low_null > 95 AND range_probe < 5
    ) WHERE explain LIKE '%Prewhere filter column%'
);
SELECT count() FROM test_basic_prewhere WHERE col_low_null > 95;

DROP TABLE test_basic_prewhere;

-- =============================================================================
-- Fallback selectivity (no basic): IS NULL / IS NOT NULL still participate in
-- prewhere reordering correctly when only tdigest stats are available.
-- =============================================================================
DROP TABLE IF EXISTS test_fallback_no_basic;

CREATE TABLE test_fallback_no_basic (
    a Int64 STATISTICS(tdigest),
    b Nullable(Int64) STATISTICS(tdigest)  -- only tdigest, NO basic
) Engine = MergeTree() ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = '';

INSERT INTO test_fallback_no_basic
SELECT number, if(number % 2 = 0, NULL, number)
FROM numbers(10000);

-- Fallback (no basic): IS NOT NULL must not dominate prewhere ordering; both
-- range ('less') and IS NOT NULL ('not') should appear in the merged prewhere.
SELECT 'Fallback: IS NOT NULL without basic keeps both conditions in prewhere';
SELECT position(prewhere_line, 'less') > 0 AS has_range, position(prewhere_line, 'not') > 0 AS has_not FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_fallback_no_basic
        WHERE a < 100 AND b IS NOT NULL
    ) WHERE explain LIKE '%Prewhere filter column%'
);

-- Same for IS NULL: fallback selectivity keeps it as a normal participant.
SELECT 'Fallback: IS NULL without basic keeps both conditions in prewhere';
SELECT position(prewhere_line, 'greater') > 0 AS has_range, position(prewhere_line, '.null') > 0 AS has_null_check FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_fallback_no_basic
        WHERE a > 9900 AND b IS NULL
    ) WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'Actual counts for validation';
SELECT count() FROM test_fallback_no_basic WHERE b IS NULL;
SELECT count() FROM test_fallback_no_basic WHERE b IS NOT NULL;

DROP TABLE test_fallback_no_basic;

-- =============================================================================
-- Fallback selectivity uses non-null row count (basic-only columns): the
-- column with more NULLs (b: 90% NULL) must be evaluated first because fewer
-- non-NULL rows pass. Without the null-aware denominator, ordering would be
-- driven by total rows and the two columns would look indistinguishable.
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

ALTER TABLE test_fallback_selectivity ADD STATISTICS a TYPE basic;
ALTER TABLE test_fallback_selectivity ADD STATISTICS b TYPE basic;
ALTER TABLE test_fallback_selectivity MATERIALIZE STATISTICS a, b SETTINGS mutations_sync = 1;

SELECT 'Fallback selectivity uses non-null row count (b before a)';
SELECT count() FROM (
    EXPLAIN actions=1 SELECT count() FROM test_fallback_selectivity WHERE a = 1 AND b = 1
) WHERE explain LIKE '%Prewhere filter column%b%a%';

DROP TABLE test_fallback_selectivity;

-- =============================================================================
-- Const-null inserts: `INSERT ... SELECT NULL` can hand the statistics builder
-- a `ColumnConst(Nullable(...))`. The basic statistic must still see the rows
-- as NULL, otherwise IS NULL estimates collapse to zero for all-null blocks
-- and prewhere ordering treats the all-NULL column as the most selective one.
-- =============================================================================
DROP TABLE IF EXISTS test_const_null;

CREATE TABLE test_const_null (
    a Nullable(Int64),
    b Nullable(Int64)
) Engine = MergeTree() ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = '';

ALTER TABLE test_const_null ADD STATISTICS a TYPE basic;
ALTER TABLE test_const_null ADD STATISTICS b TYPE basic;

-- a: all NULL via constant expression; b: 50% NULL via per-row expression.
INSERT INTO test_const_null
SELECT NULL, if(number % 2 = 0, NULL, number)
FROM numbers(1000);

-- `a IS NULL` matches every row (least selective) and should be ordered AFTER
-- `b IS NULL` (matches 50%) in prewhere. Without the const-column fix the
-- basic statistic for `a` would report null_count=0, making `a IS NULL` look
-- maximally selective and pushing it first.
SELECT 'Const-null insert: basic null count drives correct prewhere ordering';
SELECT position(prewhere_line, 'b.null') < position(prewhere_line, 'a.null') AS b_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_const_null WHERE a IS NULL AND b IS NULL
    ) WHERE explain LIKE '%Prewhere filter column%'
);

DROP TABLE test_const_null;

-- =============================================================================
-- String startsWith/endsWith predicates: statistics-based cardinality estimates
-- should participate in PREWHERE reordering. The WHERE clauses deliberately put
-- the less selective string predicate first; the estimated rows should move the
-- more selective predicate to the front of PREWHERE.
-- =============================================================================
SET move_all_conditions_to_prewhere = 1;

DROP TABLE IF EXISTS test_string_predicate_prewhere;

CREATE TABLE test_string_predicate_prewhere
(
    id UInt64,
    s_prefix_short String STATISTICS(basic),
    s_prefix_long String STATISTICS(basic),
    s_suffix_short String STATISTICS(basic),
    s_suffix_long String STATISTICS(basic),
    s_utf8_short String STATISTICS(basic),
    s_utf8_long String STATISTICS(basic),
    s_nullable Nullable(String) STATISTICS(basic),
    s_not String STATISTICS(basic),
    fs FixedString(8) STATISTICS(basic)
) ENGINE = MergeTree()
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = '';

INSERT INTO test_string_predicate_prewhere
SELECT
    number,
    'abcdef',
    'abcdef',
    'abcdefxyz',
    'abcdefxyz',
    '你好abcdef',
    '你好abcdef',
    if(number % 10 = 0, 'abcdef', NULL),
    'abcdef',
    'abcdefgh'
FROM numbers(10000);

ALTER TABLE test_string_predicate_prewhere MATERIALIZE STATISTICS
    s_prefix_short,
    s_prefix_long,
    s_suffix_short,
    s_suffix_long,
    s_utf8_short,
    s_utf8_long,
    s_nullable,
    s_not,
    fs;

SELECT 'String predicates: startsWith literal length drives prewhere ordering';
SELECT position(prewhere_line, 's_prefix_long') < position(prewhere_line, 's_prefix_short') AS longer_prefix_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_string_predicate_prewhere
        WHERE startsWith(s_prefix_short, 'a') AND startsWith(s_prefix_long, 'abc')
    ) WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'String predicates: endsWith literal length drives prewhere ordering';
SELECT position(prewhere_line, 's_suffix_long') < position(prewhere_line, 's_suffix_short') AS longer_suffix_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_string_predicate_prewhere
        WHERE endsWith(s_suffix_short, 'x') AND endsWith(s_suffix_long, 'xyz')
    ) WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'String predicates: UTF8 literal length uses code points';
SELECT position(prewhere_line, 's_utf8_long') < position(prewhere_line, 's_utf8_short') AS longer_utf8_prefix_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_string_predicate_prewhere
        WHERE startsWithCaseInsensitiveUTF8(s_utf8_short, '你') AND startsWithCaseInsensitiveUTF8(s_utf8_long, '你好')
    ) WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'String predicates: nullable empty prefix uses basic null count';
SELECT position(prewhere_line, 's_nullable') < position(prewhere_line, 's_prefix_short') AS nullable_empty_prefix_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_string_predicate_prewhere
        WHERE startsWith(s_prefix_short, '') AND startsWith(s_nullable, '')
    ) WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'String predicates: NOT inverts startsWith selectivity';
SELECT position(prewhere_line, 's_prefix_long') < position(prewhere_line, 's_not') AS positive_prefix_before_negated_prefix FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_string_predicate_prewhere
        WHERE not(startsWith(s_not, 'abc')) AND startsWith(s_prefix_long, 'abc')
    ) WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'String predicates: FixedString startsWith is estimated';
SELECT position(prewhere_line, 'fs') < position(prewhere_line, 's_prefix_short') AS fixed_string_prefix_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_string_predicate_prewhere
        WHERE startsWith(s_prefix_short, 'a') AND startsWith(fs, 'abc')
    ) WHERE explain LIKE '%Prewhere filter column%'
);

DROP TABLE test_string_predicate_prewhere;

-- =============================================================================
-- LIKE selectivity: constant LIKE patterns should participate in statistics-based
-- prewhere ordering, keep same-column NULL correlation, and avoid treating short
-- FixedString exact LIKE patterns as equality because equality ignores trailing
-- NUL padding while LIKE matches FixedString(N)'s full byte sequence.
-- =============================================================================
DROP TABLE IF EXISTS test_like_selectivity;

CREATE TABLE test_like_selectivity
(
    id UInt64,
    a Nullable(String) STATISTICS(basic),
    fixed FixedString(5) STATISTICS(uniq),
    probe UInt64 STATISTICS(tdigest)
) ENGINE = MergeTree()
ORDER BY id
SETTINGS auto_statistics_types = '';

INSERT INTO test_like_selectivity
SELECT
    number,
    if(number % 5 = 0, NULL, concat('value', toString(number))),
    toFixedString('abcde', 5),
    number
FROM numbers(1000);

SELECT 'LIKE contains pattern is more selective than range';
SELECT position(prewhere_line, 'like') < position(prewhere_line, 'probe') AS like_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count() FROM test_like_selectivity
        WHERE a LIKE '%9999%' AND probe < 50
    ) WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'LIKE match-all with IS NOT NULL keeps same-column nullable selectivity';
SELECT position(prewhere_line, 'probe') < position(prewhere_line, 'like') AS probe_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count() FROM test_like_selectivity
        WHERE a LIKE '%' AND a IS NOT NULL AND probe < 700
    ) WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'Nested same-column LIKE OR with IS NOT NULL keeps correlation';
SELECT position(prewhere_line, 'probe') < position(prewhere_line, 'like') AS probe_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count() FROM test_like_selectivity
        WHERE (a LIKE '%' OR a LIKE 'x%') AND a IS NOT NULL AND probe < 700
    ) WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'FixedString exact LIKE length mismatch is not estimated as equality';
SELECT position(prewhere_line, 'like') < position(prewhere_line, 'probe') AS like_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count() FROM test_like_selectivity
        WHERE fixed LIKE 'abc' AND probe < 500
    ) WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'FixedString exact LIKE matching length may use equality estimate';
SELECT position(prewhere_line, 'probe') < position(prewhere_line, 'like') AS probe_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count() FROM test_like_selectivity
        WHERE fixed LIKE 'abcde' AND probe < 500
    ) WHERE explain LIKE '%Prewhere filter column%'
);

DROP TABLE test_like_selectivity;
