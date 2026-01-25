-- Test for fix of double onFinish() call in SaveSubqueryResultToBufferTransform
-- This query previously caused a logical error due to ChunkBuffer underflow
SELECT (SELECT first_value(*) FROM (SELECT t0.c0)) AS a0 FROM (SELECT 1 AS c0 GROUP BY isNull(8) WITH TOTALS) AS t0;
