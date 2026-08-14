-- A condition runs in a copied query context, so its own time limit starts with every
-- attempt. `waitUntil` must nevertheless apply the parent query's total time budget
-- between attempts. Without the parent checks this takes about 27 seconds.
CREATE TEMPORARY TABLE start_time AS SELECT now64() AS start;

SELECT result FROM waitUntil(sleep(0.9), 30, 0) SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break';

SELECT if(dateDiff('second', (SELECT start FROM start_time), now64()) < 10, 'fast', 'slow');

SELECT result FROM waitUntil(sleep(0.9), 30, 0) SETTINGS max_execution_time = 1, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }

-- The condition is evaluated with the caller's settings. In particular, an aggregate over an
-- empty input produces no row with this setting, which means the condition is not satisfied.
SELECT result FROM waitUntil((SELECT count() = 0 FROM numbers(0)), 1) SETTINGS empty_result_for_aggregation_by_empty_set = 1;
