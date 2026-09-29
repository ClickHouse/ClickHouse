-- The DISTINCT early-stop limit hint (limit_length + limit_offset) may only bound the number of
-- distinct rows when there is an actual non-negative integer LIMIT. It must be disabled for:
--   * negative LIMIT (takes rows from the tail, not the head),
--   * fractional LIMIT/OFFSET (a fraction of the total row count is only known after all rows are
--     read, so it cannot bound the distinct rows collected from the head), and
--   * a bare OFFSET with no LIMIT (limit_offset is populated, so the hint would become the offset
--     alone and drop the tail that OFFSET must return).
-- A multi-part MergeTree enables the in-order DISTINCT whose hint previously truncated the stream.
-- https://github.com/ClickHouse/ClickHouse/issues/111254

-- Pin the settings that force the in-order DISTINCT limit-hint path. Without these, a randomized run
-- with optimize_distinct_in_order=0 and optimize_read_in_order=0 reads the full distinct stream and
-- returns the correct result even on the buggy build, turning this regression into a false negative.
SET optimize_distinct_in_order = 1;
SET optimize_read_in_order = 1;
SET max_threads = 4;

DROP TABLE IF EXISTS nl;
CREATE TABLE nl (x Int64) ENGINE = MergeTree ORDER BY x;
-- With only 400 rows a single part fits in one block, so the truncation is not observable and four
-- separate parts are needed here; stop merges to keep them from collapsing. (A single part does
-- expose the bug once the data spans several blocks - see the `big` table at the end of this file.)
SYSTEM STOP MERGES nl;
INSERT INTO nl SELECT number       FROM numbers(100);
INSERT INTO nl SELECT number + 100 FROM numbers(100);
INSERT INTO nl SELECT number + 200 FROM numbers(100);
INSERT INTO nl SELECT number + 300 FROM numbers(100);

SET enable_analyzer = 1;
SELECT 'analyzer';
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d LIMIT -1;          -- 3
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d LIMIT -2;          -- 2 3
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d LIMIT -3;          -- 1 2 3
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d LIMIT -1 OFFSET 1; -- 3
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d LIMIT 1;           -- 0 (positive unchanged)
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d LIMIT 2;           -- 0 1 (positive unchanged)
-- Fractional OFFSET with an integral LIMIT: offset = ceil(4 * 0.5) = 2, then LIMIT 1 -> group 2.
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d LIMIT 1 OFFSET 0.5; -- 2
-- Fractional LIMIT with an integral OFFSET: skip 5 groups, then ceil(40 * 0.3) = 12 groups -> 5..16.
SELECT DISTINCT intDiv(x, 10) AS d FROM nl ORDER BY d LIMIT 0.3 OFFSET 5;  -- 5 6 7 8 9 10 11 12 13 14 15 16
-- Bare OFFSET with no LIMIT: the hint must stay disabled, otherwise DISTINCT stops after the offset
-- and the later OFFSET strips it, dropping the tail.
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d OFFSET 1;          -- 1 2 3
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d OFFSET 2;          -- 2 3

DROP TABLE nl;

-- A single part large enough to span several blocks also truncates on the buggy build: the hint
-- stops the in-order DISTINCT at a block boundary, so the result is the tail of a truncated prefix
-- of the sorted distinct stream and the selected rows track block boundaries instead of the sorted
-- order. Several parts are not needed here, and this is wrong on the buggy build for every
-- max_block_size the test runner draws. The settings pinned above still apply here: both
-- optimize_distinct_in_order and optimize_read_in_order are pinned to their own default of 1, so the
-- default configuration is covered, but a run drawing 0 for both plans no hint at all and would pass
-- on the buggy build. The result does not depend on max_threads.
DROP TABLE IF EXISTS big;
CREATE TABLE big (x Int64) ENGINE = MergeTree ORDER BY x;
INSERT INTO big SELECT number FROM numbers(200000);

SET enable_analyzer = 1;
SELECT 'analyzer single part';
SELECT DISTINCT intDiv(x, 100) AS d FROM big ORDER BY d LIMIT -3;                             -- 1997 1998 1999
-- The same rows must come back for any block size, not just the default one.
SELECT DISTINCT intDiv(x, 100) AS d FROM big ORDER BY d LIMIT -3 SETTINGS max_block_size = 8192; -- 1997 1998 1999

DROP TABLE big;
