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

-- Tiny cap below the unmutated query's formatted length: every fuzzed variant exceeds
-- the cap, so the source must fall back to the unfuzzed base query rather than spinning.
SELECT count() AS rows_returned, max(length(query)) > 0 AS non_empty FROM (
    SELECT query FROM fuzzQuery('SELECT 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10', 1, 42) LIMIT 5
);
