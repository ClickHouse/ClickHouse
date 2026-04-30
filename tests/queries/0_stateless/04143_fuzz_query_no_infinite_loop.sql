-- Tags: no-fasttest

-- Regression test for `fuzzQuery` infinite loop in `FuzzQuerySource::createColumn`.
-- The previous code reset `fuzz_base` on every iteration when `max_query_length > 500`,
-- never producing a row, so any `SELECT FROM fuzzQuery(..., > 500)` hung forever.
-- The same shape can occur after tightening the cap if every fuzzed AST exceeds the
-- limit. The source must always make progress: cap retries per row and fall back to the
-- original query when fuzzing cannot fit the cap.

-- Cap above 500: with the old check `if (config.max_query_length > 500)` this hangs.
SELECT count() AS rows_returned FROM (
    SELECT * FROM fuzzQuery('SELECT 1', 1000, 42) LIMIT 5
);

-- Cap right at the threshold.
SELECT count() AS rows_returned FROM (
    SELECT * FROM fuzzQuery('SELECT 1', 501, 42) LIMIT 5
);

-- Tiny cap below the unmutated base query's formatted length: no fuzzed variant can
-- possibly satisfy the cap. Falling back to the unfuzzed query would emit oversized
-- rows and silently violate the documented `max_query_length` contract; reject the
-- impossible configuration up front instead.
SELECT * FROM fuzzQuery('SELECT 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10', 1, 42) LIMIT 5; -- { serverError BAD_ARGUMENTS }

-- `max_query_length = 0` is pathological: every non-empty fuzzed AST exceeds the cap.
-- Reject it at configuration time rather than degrading to a no-op fuzzer.
SELECT * FROM fuzzQuery('SELECT 1', 0, 42) LIMIT 1; -- { serverError BAD_ARGUMENTS }
