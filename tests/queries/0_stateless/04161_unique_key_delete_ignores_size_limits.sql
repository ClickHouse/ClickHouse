-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
-- UNIQUE KEY DELETE must ignore user-set result/read size limits: its internal
-- row-finder SELECT is DELETE machinery, not a user-bounded result. Under
-- overflow_mode='break' a small max_result_rows / max_rows_to_read truncates the
-- scan to a prefix and silently under-deletes; under the default 'throw' a large
-- valid DELETE would wrongly raise TOO_MANY_ROWS.
--
-- break-mode limits are enforced per read block, so the part must hold well more
-- than one block for the truncation to surface: a single ~65k-row block is never
-- cut. Seed 200000 rows in one part so the finder's scan spans several blocks;
-- without the fix the DELETE then lands only a one-block prefix, not the full set.
-- All keys distinct (dedup is a later PR); merges stopped so the synchronous
-- DELETE bitmap stays observable.

SET allow_experimental_unique_key = 1;
SET async_insert = 0;
SET optimize_trivial_count_query = 0;
SET optimize_use_implicit_projections = 0;

-- ============================================================================
-- Case 1: max_result_rows=1, result_overflow_mode='break'
-- ============================================================================
DROP TABLE IF EXISTS uk_del_limits;

CREATE TABLE uk_del_limits (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

SYSTEM STOP MERGES uk_del_limits;

INSERT INTO uk_del_limits SELECT number, toString(number) FROM numbers(200000);

SELECT 'c1_after_insert' AS step, count() FROM uk_del_limits;  -- 200000

-- A tiny result limit with break mode: pre-fix the finder's scan stops after the
-- first read block, so only that prefix is deleted. DELETE matches all rows.
SET max_result_rows = 1;
SET result_overflow_mode = 'break';

DELETE FROM uk_del_limits WHERE id >= 0;

SET max_result_rows = 0;
SET result_overflow_mode = 'throw';

-- All rows must be gone, not just a one-block prefix.
SELECT 'c1_after_delete' AS step, count() FROM uk_del_limits;  -- 0

DROP TABLE uk_del_limits;

-- ============================================================================
-- Case 2: max_rows_to_read=1, read_overflow_mode='break'
-- ============================================================================
DROP TABLE IF EXISTS uk_del_readlimits;

CREATE TABLE uk_del_readlimits (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

SYSTEM STOP MERGES uk_del_readlimits;

INSERT INTO uk_del_readlimits SELECT number, toString(number) FROM numbers(200000);

SELECT 'c2_after_insert' AS step, count() FROM uk_del_readlimits;  -- 200000

-- A tiny read limit with break mode stops the read pipeline after the first
-- block; pre-fix only that prefix's matching rows are deleted. DELETE the even
-- ids across the whole part.
SET max_rows_to_read = 1;
SET read_overflow_mode = 'break';

DELETE FROM uk_del_readlimits WHERE id % 2 = 0;

SET max_rows_to_read = 0;
SET read_overflow_mode = 'throw';

-- All 100000 even ids deleted; the 100000 odd ids survive (not a prefix).
SELECT 'c2_after_delete' AS step, count() FROM uk_del_readlimits;  -- 100000
SELECT 'c2_survivors_min' AS step, min(id) FROM uk_del_readlimits;  -- 1
SELECT 'c2_survivors_max' AS step, max(id) FROM uk_del_readlimits;  -- 199999

DROP TABLE uk_del_readlimits;
