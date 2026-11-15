-- Regression test: convertJoinToIn optimization must materialize the __join_result_dummy column
-- to avoid a block structure mismatch (Const vs non-Const) that causes an exception in debug builds.
-- https://github.com/ClickHouse/ClickHouse/issues/96650

SELECT 1 FROM (SELECT 1 c0) tx JOIN (SELECT 1 c0) ty USING (c0) LIMIT 1 SETTINGS query_plan_merge_expressions = 0, query_plan_convert_join_to_in = 1;
