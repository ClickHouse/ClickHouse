-- STID 1941-26fa: a no-argument window function inside a CODEC, e.g.
-- CODEC(cume_dist() OVER (...), NONE), was formatted back as cume_dist OVER (...)
-- (the () dropped), so the format-parse-format round-trip diverged and the
-- server aborted with the Inconsistent AST formatting LOGICAL_ERROR in
-- debug / sanitizer builds. See the commit message for the full root cause.

-- Original reproducer. We do not care which error fires, only that the server
-- does not abort with LOGICAL_ERROR. cume_dist is not a valid codec family, so
-- the parser returns UNKNOWN_CODEC once the round-trip check passes cleanly.
CREATE TABLE base32__fuzz_6 (`i` Int16 CODEC(cume_dist() OVER (ORDER BY toDateTime64OrZero('2017-06-15') ASC, b DESC), NONE), `f` Nullable(Float32) CODEC(NONE)) ENGINE = MergeTree ORDER BY i; -- { serverError UNKNOWN_CODEC }

-- The formatted output must keep the () of every no-argument window function.
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int CODEC(cume_dist() OVER (ORDER BY x ASC, b DESC), NONE)) ENGINE = MergeTree ORDER BY c0');
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int CODEC(rank() OVER (ORDER BY x), NONE)) ENGINE = MergeTree ORDER BY c0');
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int CODEC(row_number() OVER (PARTITION BY y), NONE)) ENGINE = MergeTree ORDER BY c0');
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int CODEC(dense_rank() OVER w, NONE)) ENGINE = MergeTree ORDER BY c0');

-- A window function that has arguments was always fine; assert no regression.
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int CODEC(lagInFrame(c0) OVER (ORDER BY x), NONE)) ENGINE = MergeTree ORDER BY c0');

-- The fix is gated on isWindowFunction(): a plain no-argument codec family
-- (Delta) must NOT gain parentheses.
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int CODEC(Delta, NONE)) ENGINE = MergeTree ORDER BY c0');

-- Window functions in normal expression context already round-tripped; assert
-- the fix did not change them.
SELECT formatQuerySingleLine('SELECT cume_dist() OVER (ORDER BY number) FROM numbers(3)');
SELECT formatQuerySingleLine('SELECT count() OVER (ORDER BY number) FROM numbers(3)');
