-- Regression test: TABLE_FUNCTION node as argument of remote() must not cause
-- LOGICAL_ERROR when the same table expression is re-resolved during
-- analyzer_compatibility_join_using_top_level_identifier handling.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=b053840ef38b3b36bc7fb44fa6d5fb129571b2cd&name_0=MasterCI&name_1=BuzzHouse+%28amd_ubsan%29

SET enable_analyzer = 1;
SET analyzer_compatibility_join_using_top_level_identifier = 1;
SET enable_parallel_replicas = 0;

SELECT generate_series AS c0
FROM remote('localhost', generateSeries(1, 3)) AS t
LEFT JOIN (SELECT generate_series AS c0 FROM generateSeries(1, 3)) AS t2 USING (c0)
FORMAT Null;

-- Simplified reproduction from Kirill Fgrtue
SELECT 1 AS c0
FROM remote('localhost:9000', generateSeries(1, 10)) AS t1
LEFT JOIN (SELECT 1 AS c0) AS t2 USING (c0)
FORMAT Null; -- { serverError INVALID_JOIN_ON_EXPRESSION }
