-- Regression test: TABLE_FUNCTION node as argument of remote() must not cause
-- LOGICAL_ERROR when the same table expression is re-resolved during
-- analyzer_compatibility_join_using_top_level_identifier handling.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=b053840ef38b3b36bc7fb44fa6d5fb129571b2cd&name_0=MasterCI&name_1=BuzzHouse+%28amd_ubsan%29

SET enable_analyzer = 1;
SET analyzer_compatibility_join_using_top_level_identifier = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0; -- runtime filters can trigger outer→inner join conversion, causing constant USING join to succeed instead of error
SET query_plan_convert_outer_join_to_inner_join = 0; -- CI may inject True; directly converts LEFT JOIN to INNER, bypassing INVALID_JOIN_ON_EXPRESSION validation on the constant USING condition
SET query_plan_convert_any_join_to_semi_or_anti_join = 0; -- CI may inject True; converts ANY LEFT JOIN to SEMI, bypassing INVALID_JOIN_ON_EXPRESSION validation on the constant USING condition
SET query_plan_optimize_join_order_limit = 10; -- CI may inject 0; skips chooseJoinOrder entirely, bypassing the code path that validates and raises INVALID_JOIN_ON_EXPRESSION on the constant USING condition
SET prefer_localhost_replica = 1; -- with 0, remote('localhost:9000',...) uses TCP and may follow a different planning path that bypasses the join validation

SELECT generate_series AS c0
FROM remote('localhost', generateSeries(1, 3)) AS t
LEFT JOIN (SELECT generate_series AS c0 FROM generateSeries(1, 3)) AS t2 USING (c0)
FORMAT Null;

-- Simplified reproduction from Kirill Fgrtue
SELECT 1 AS c0
FROM remote('localhost:9000', generateSeries(1, 10)) AS t1
LEFT JOIN (SELECT 1 AS c0) AS t2 USING (c0)
FORMAT Null; -- { serverError INVALID_JOIN_ON_EXPRESSION }
