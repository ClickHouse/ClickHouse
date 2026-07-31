-- Tags: no-fasttest

-- The `fuzzQuery` queries below disable the server-side AST fuzzer (`ast_fuzzer_runs = 0`):
-- the Stress test wraps every query with it, and it mutates the `fuzzQuery(...)` arguments
-- (e.g. the `max_query_length` literal), making `FuzzQuerySource::createColumn` unable to
-- produce a row within the cap and looping.

-- Regression test for the `fuzzQuery` `max_query_length` cap (#101354).
-- The previous check `if (config.max_query_length > 500)` did not compare against the
-- fuzzed AST length: for `max_query_length <= 500` the cap was never enforced, and for
-- `max_query_length > 500` the loop restarted on every iteration without producing rows.

SELECT max(length(query)) <= 200 AS within_cap, count() AS rows_returned
FROM (
    SELECT query
    FROM fuzzQuery('SELECT 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10', 200, 42)
    LIMIT 100
)
SETTINGS ast_fuzzer_runs = 0;

SELECT max(length(query)) <= 600 AS within_cap, count() AS rows_returned
FROM (
    SELECT query
    FROM fuzzQuery('SELECT 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10', 600, 42)
    LIMIT 100
)
SETTINGS ast_fuzzer_runs = 0;

-- `max_query_length = 0` would otherwise enter an infinite loop in `FuzzQuerySource::createColumn`.
SELECT * FROM fuzzQuery('SELECT 1', 0, 42); -- { serverError BAD_ARGUMENTS }

-- Small positive caps that are smaller than the formatted base query are rejected up front:
-- the fallback path would otherwise emit the unfuzzed query and exceed the cap, violating
-- the documented contract `length(generated) <= max_query_length`.
SELECT * FROM fuzzQuery('SELECT 1', 1, 42); -- { serverError BAD_ARGUMENTS }
