-- Tags: no-random-merge-tree-settings

-- Test for three reverse key risk fixes:
-- 1. Old ReadInOrderOptimizer with reverse keys (allow_experimental_analyzer = 0)
-- 2. ALTER TABLE MODIFY ORDER BY direction change validation
-- 3. MinMaxCount projection with explicit PRIMARY KEY + reverse sorting key

-- ==========================================================================
-- Risk #1: Old ReadInOrderOptimizer must handle reverse keys correctly
-- When allow_experimental_analyzer = 0, the old ReadInOrderOptimizer is used.
-- It must account for reverse_flags when matching sort descriptions.
-- ==========================================================================

DROP TABLE IF EXISTS t_reverse_old_analyzer;

CREATE TABLE t_reverse_old_analyzer (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY (a DESC, b)
SETTINGS allow_experimental_reverse_key = 1, index_granularity = 8;

INSERT INTO t_reverse_old_analyzer SELECT number, number FROM numbers(100);
OPTIMIZE TABLE t_reverse_old_analyzer FINAL;

-- With old analyzer: ORDER BY a DESC should give correct results
SELECT a FROM t_reverse_old_analyzer ORDER BY a DESC LIMIT 5
SETTINGS allow_experimental_analyzer = 0;

-- ORDER BY a ASC should also work correctly
SELECT a FROM t_reverse_old_analyzer ORDER BY a ASC LIMIT 5
SETTINGS allow_experimental_analyzer = 0;

-- With new analyzer for comparison (should give same results)
SELECT a FROM t_reverse_old_analyzer ORDER BY a DESC LIMIT 5
SETTINGS allow_experimental_analyzer = 1;

SELECT a FROM t_reverse_old_analyzer ORDER BY a ASC LIMIT 5
SETTINGS allow_experimental_analyzer = 1;

DROP TABLE t_reverse_old_analyzer;

-- ==========================================================================
-- Risk #2: ALTER TABLE MODIFY ORDER BY must reject direction changes
-- Changing DESC to ASC for an existing column is invalid because
-- existing data parts are physically sorted in the old direction.
-- Note: ALTER TABLE MODIFY ORDER BY does not support DESC syntax,
-- so it always produces ASC columns. We test that changing a DESC column
-- to ASC (by omitting DESC in ALTER) is correctly rejected.
-- ==========================================================================

DROP TABLE IF EXISTS t_reverse_alter;

CREATE TABLE t_reverse_alter (a UInt64, b UInt64, c UInt64)
ENGINE = MergeTree ORDER BY (a DESC, b)
SETTINGS allow_experimental_reverse_key = 1;

INSERT INTO t_reverse_alter VALUES (1, 2, 3);

-- This should fail: column `a` was DESC, ALTER produces ASC (direction change)
ALTER TABLE t_reverse_alter MODIFY ORDER BY (a, b); -- { serverError BAD_ARGUMENTS }

-- This should also fail: even when adding a new column, if existing column direction changes
ALTER TABLE t_reverse_alter MODIFY ORDER BY (a, b, c); -- { serverError BAD_ARGUMENTS }

-- This should succeed: keeping same columns in same order (all ASC via ALTER matches
-- the original ASC column `b`), but we need to keep `a` as well. Since ALTER can't
-- express DESC, we can only successfully ALTER if no existing column changes direction.
-- With the current table (a DESC, b ASC), any ALTER that includes `a` will fail because
-- ALTER always makes it ASC. The only valid ALTER would be to extend with new columns
-- while keeping all existing columns unchanged, but since ALTER can't express DESC for `a`,
-- this particular table can't be successfully ALTERed for ORDER BY at all.

-- Verify data is still accessible after failed ALTERs
SELECT * FROM t_reverse_alter;

DROP TABLE t_reverse_alter;

-- ==========================================================================
-- Risk #3: MinMaxCount projection with explicit PRIMARY KEY + reverse sorting key
-- When PRIMARY KEY is explicitly specified, primary_key.reverse_flags may be empty.
-- The code should use sorting_key.reverse_flags instead.
-- ==========================================================================

DROP TABLE IF EXISTS t_reverse_minmax;

CREATE TABLE t_reverse_minmax (a UInt64, b UInt64)
ENGINE = MergeTree
PRIMARY KEY a
ORDER BY (a DESC, b)
SETTINGS allow_experimental_reverse_key = 1, index_granularity = 8;

INSERT INTO t_reverse_minmax SELECT number, number * 10 FROM numbers(1, 100);
OPTIMIZE TABLE t_reverse_minmax FINAL;

-- The MinMaxCount projection should correctly handle the reversed sorting key
-- even when PRIMARY KEY is explicitly specified.
SELECT count() FROM t_reverse_minmax;

-- min/max should be correct
SELECT min(a), max(a) FROM t_reverse_minmax;

DROP TABLE t_reverse_minmax;

-- ==========================================================================
-- Positive test for force_primary_key_reverse_order setting
-- Verifies that the setting rewrites ORDER BY columns to DESC and that
-- tables are created successfully without explicit allow_experimental_reverse_key.
-- ==========================================================================

SET force_primary_key_reverse_order = 1;

-- Single-column ORDER BY: should be rewritten to DESC
DROP TABLE IF EXISTS t_force_reverse_single;
CREATE TABLE t_force_reverse_single (x UInt64) ENGINE = MergeTree ORDER BY x;
-- Verify DESC appears in the stored ORDER BY (allow_experimental_reverse_key was auto-enabled)
SELECT create_table_query LIKE '%ORDER BY x DESC%' FROM system.tables
    WHERE database = currentDatabase() AND name = 't_force_reverse_single';

-- Multi-column ORDER BY: all columns should be rewritten to DESC
DROP TABLE IF EXISTS t_force_reverse_multi;
CREATE TABLE t_force_reverse_multi (a UInt64, b String, c DateTime) ENGINE = MergeTree ORDER BY (a, b, c);
SELECT create_table_query LIKE '%a DESC%b DESC%c DESC%' FROM system.tables
    WHERE database = currentDatabase() AND name = 't_force_reverse_multi';

-- Verify data roundtrip works correctly with forced reverse keys
INSERT INTO t_force_reverse_single SELECT number FROM numbers(10);
SELECT x FROM t_force_reverse_single ORDER BY x ASC;

INSERT INTO t_force_reverse_multi VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-02'), (3, 'c', '2024-01-03');
SELECT a, b FROM t_force_reverse_multi ORDER BY a ASC;

DROP TABLE t_force_reverse_single;
DROP TABLE t_force_reverse_multi;

SET force_primary_key_reverse_order = 0;
