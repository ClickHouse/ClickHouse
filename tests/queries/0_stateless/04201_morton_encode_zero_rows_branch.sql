-- Test: exercises `mortonEncode` with input_rows_count == 0 — the early-return branch
-- added in PR #62288 to fix an MSan use-of-uninitialized-value report.
-- Covers: src/Functions/mortonEncode.cpp:272-273 — `if (input_rows_count == 0) return ColumnUInt64::create();`
--
-- Note: PR's own test 03035_morton_encode_no_rows.sql actually uses 1-row inputs
-- (each SELECT produces 1 row of output), so it does NOT exercise the new branch.
-- This test runs `mortonEncode` over a truly 0-row source (numbers(0) and an empty MergeTree).

DROP TABLE IF EXISTS test_morton_zero_rows;
CREATE TABLE test_morton_zero_rows (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;

-- 0-row MergeTree source, simple mode
SELECT count() FROM (SELECT mortonEncode(a, b) AS m FROM test_morton_zero_rows);

-- 0-row source, simple mode, single arg (nd == 1 path)
SELECT count() FROM (SELECT mortonEncode(toUInt32(number)) AS m FROM numbers(0));

-- 0-row source, expanded mode (mask is non-const via materialize) — same shape as MSan-reported query
SELECT count() FROM (SELECT mortonEncode(materialize((1, 1)), toUInt32(number), toUInt32(number + 1)) AS m FROM numbers(0));

-- Strong mutation guard: mask ratio 9 (>8) would throw ARGUMENT_OUT_OF_BOUND if the
-- per-row dispatch ran. The early-return short-circuits that path, so this returns 0.
-- If the fix is removed, this query throws ARGUMENT_OUT_OF_BOUND instead of returning 0.
SELECT count() FROM (SELECT mortonEncode(materialize((9, 9)), toUInt32(number), toUInt32(number + 1)) AS m FROM numbers(0));

-- Direct SELECT into client — empty result
SELECT mortonEncode(toUInt32(number), toUInt32(number + 1)) FROM numbers(0);

DROP TABLE test_morton_zero_rows;
