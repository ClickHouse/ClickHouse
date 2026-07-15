-- Tags: no-parallel
-- no-parallel: uses a server-global failpoint that stalls queries with a max_execution_time set.

-- A max_execution_time timeout can fire while the query is still pending (before execution starts,
-- e.g. during planning). Such a query must report TIMEOUT_EXCEEDED, not QUERY_WAS_CANCELLED.
-- The failpoint stalls the main thread so the timeout deterministically fires in the pending state.

SYSTEM ENABLE FAILPOINT execute_query_sleep_before_pending_kill_check;
SELECT sleep(0) SETTINGS max_execution_time = 1; -- { serverError TIMEOUT_EXCEEDED }
SYSTEM DISABLE FAILPOINT execute_query_sleep_before_pending_kill_check;
