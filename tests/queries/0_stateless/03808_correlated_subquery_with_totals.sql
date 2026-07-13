-- Test for fix of double onFinish() call in SaveSubqueryResultToBufferTransform
-- This query previously caused a logical error due to ChunkBuffer underflow
-- Note: The totals row returns NULL because the common subplan buffering
-- optimization only buffers main data streams, not the totals stream.
-- This is a known limitation when combining correlated subqueries with WITH TOTALS.
SET enable_analyzer = 1;
SELECT (SELECT first_value(*) FROM (SELECT t0.c0)) AS a0 FROM (SELECT 1 AS c0 GROUP BY isNull(8) WITH TOTALS) AS t0;
