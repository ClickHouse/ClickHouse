-- Regression test: tryExecuteFunctionsAfterSorting must not crash when sorting columns
-- are not present in the expression DAG's outputs (e.g. after convertJoinToIn adds qualified names).
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=47a1a42015008dee146876fbbcce728e4bcb993e&name_0=MasterCI&name_1=Stress%20test%20%28arm_asan%2C%20s3%29

SELECT 1 FROM (SELECT 1 c0) tx JOIN (SELECT 1 c0) ty USING (c0) LIMIT 1 SETTINGS query_plan_merge_expressions = 0, query_plan_convert_join_to_in = 1;
