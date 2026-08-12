-- Regression test: tryExecuteFunctionsAfterSorting must not crash when sorting columns
-- are not present in the expression DAG's outputs (e.g. after convertJoinToIn adds qualified names).
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=47a1a42015008dee146876fbbcce728e4bcb993e&name_0=MasterCI&name_1=Stress%20test%20%28arm_asan%2C%20s3%29

SET join_algorithm = 'hash';
-- The conversion declines while a join or transfer limit is active, and the test profile sets all four.
SET max_rows_in_join = 0, max_bytes_in_join = 0, max_rows_to_transfer = 0, max_bytes_to_transfer = 0;

SELECT 1 FROM (SELECT 1 c0) tx SEMI LEFT JOIN (SELECT 1 c0) ty USING (c0) LIMIT 1 SETTINGS query_plan_merge_expressions = 0, query_plan_convert_join_to_in = 1;

-- The result above is also what the unconverted join returns, so pin that the conversion fired.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT 1 FROM (SELECT 1 c0) tx SEMI LEFT JOIN (SELECT 1 c0) ty USING (c0) LIMIT 1
    SETTINGS query_plan_merge_expressions = 0, query_plan_convert_join_to_in = 1
) WHERE explain ILIKE '%CreatingSets%';
