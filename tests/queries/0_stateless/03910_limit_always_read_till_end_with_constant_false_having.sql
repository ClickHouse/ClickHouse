-- Regression test for a bug where a constant-false HAVING filter caused
-- the pipeline to shut down prematurely via DelayedPortsProcessor, aborting
-- set creation, while LimitTransform with always_read_till_end continued
-- pulling data through a FilterTransform that needed the unbuilt set.
-- The fix ensures LimitTransform does not continue reading when the limit
-- has not been reached and the output is already finished.

SELECT 1
FROM (SELECT 1 AS c0 WHERE EXISTS (SELECT 1) LIMIT 1) v0
GROUP BY v0.c0
HAVING v0.c0 = 1 AND v0.c0 = 2
SETTINGS exact_rows_before_limit = 1, execute_exists_as_scalar_subquery = 0;
