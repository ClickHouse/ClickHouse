-- Tags: no-parallel, long
-- Reason: inspects `system.query_log` flushed from the same server instance, so cannot run
-- alongside other queries of the same user without racing on the expected row set.

-- Verify that the `function_calls` Map column in `system.query_log` records per-function
-- execution stats for the query that just ran, and that counts from a previous query do not
-- leak into a later query on the same worker thread (the per-thread map must be reset at query
-- start/finalize — see #98999).

SET log_queries = 1;
SET log_queries_min_type = 'QUERY_FINISH';

-- 1. A query that exercises a specific named function.
SELECT upper('abc') AS u SETTINGS log_comment = '04096_first_query_'||toString(queryID());
SYSTEM FLUSH LOGS query_log;

-- Expect the `upper` call count on the first query to be >= 1 (constant folding may collapse it,
-- but the executor still runs the function at least once).
SELECT (function_calls['upper'].1) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment LIKE '04096_first_query_%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- 2. A second query on the same connection that does NOT call `upper`. Its row in
-- system.query_log must not carry a bogus `upper` count inherited from the previous run.
SELECT lower('XYZ') AS l SETTINGS log_comment = '04096_second_query_'||toString(queryID());
SYSTEM FLUSH LOGS query_log;

-- `upper` must be absent (or zero) on the second query's row.
SELECT has(mapKeys(function_calls), 'upper')
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment LIKE '04096_second_query_%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- `lower` is present on the second query's row.
SELECT (function_calls['lower'].1) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment LIKE '04096_second_query_%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- 3. The tuple element order of the `function_calls` value is `(blocks, rows, bytes)`.
-- Sanity check: all three must be non-negative and consistent (blocks <= rows for any
-- function called at least once).
SELECT
    all_non_negative,
    blocks_not_greater_than_rows
FROM
(
    SELECT
        min(arrayMin(mapValues(function_calls).1)) >= 0
          AND min(arrayMin(mapValues(function_calls).2)) >= 0
          AND min(arrayMin(mapValues(function_calls).3)) >= 0 AS all_non_negative,
        min(arrayMin(arrayMap(
            (b, r) -> toInt64(r) - toInt64(b),
            mapValues(function_calls).1,
            mapValues(function_calls).2))) >= 0 AS blocks_not_greater_than_rows
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND type = 'QueryFinish'
      AND log_comment LIKE '04096_%'
      AND length(function_calls) > 0
);
