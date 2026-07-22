-- The DISTINCT early-stop limit hint (limit_length + limit_offset) may only bound the number of
-- distinct rows when the LIMIT/OFFSET is a plain non-negative integer. It must be disabled for:
--   * negative LIMIT (takes rows from the tail, not the head), and
--   * fractional LIMIT/OFFSET (a fraction of the total row count is only known after all rows are
--     read, so it cannot bound the distinct rows collected from the head).
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
-- Four separate parts are required: with a single part the result is correct even on the buggy
-- build, so stop merges to keep the parts from collapsing.
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

SET enable_analyzer = 0;
SELECT 'old analyzer';
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d LIMIT -1;          -- 3
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d LIMIT -2;          -- 2 3
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d LIMIT 1;           -- 0 (positive unchanged)
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d LIMIT 1 OFFSET 0.5; -- 2
SELECT DISTINCT intDiv(x, 10) AS d FROM nl ORDER BY d LIMIT 0.3 OFFSET 5;  -- 5 6 7 8 9 10 11 12 13 14 15 16

DROP TABLE nl;
