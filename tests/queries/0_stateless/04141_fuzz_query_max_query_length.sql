-- Tags: no-fasttest

-- Regression test for the `fuzzQuery` `max_query_length` cap (#101354).
-- The previous check `if (config.max_query_length > 500)` did not compare against the
-- fuzzed AST length: for `max_query_length <= 500` the cap was never enforced, and for
-- `max_query_length > 500` the loop restarted on every iteration without producing rows.

SELECT max(length(query)) <= 200 AS within_cap, count() AS rows_returned
FROM (
    SELECT query
    FROM fuzzQuery('SELECT 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10', 200, 42)
    LIMIT 100
);

SELECT max(length(query)) <= 600 AS within_cap, count() AS rows_returned
FROM (
    SELECT query
    FROM fuzzQuery('SELECT 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10', 600, 42)
    LIMIT 100
);
