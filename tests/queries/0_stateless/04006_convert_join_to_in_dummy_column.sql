-- Regression test: convertJoinToIn optimization must materialize the __join_result_dummy column
-- to avoid a block structure mismatch (Const vs non-Const) that causes an exception in debug builds.
-- https://github.com/ClickHouse/ClickHouse/issues/96650

SET enable_analyzer = 1;
SET join_algorithm = 'hash';
-- The conversion declines while a join or transfer limit is active, and the test profile sets all four.
SET max_rows_in_join = 0, max_bytes_in_join = 0, max_rows_to_transfer = 0, max_bytes_to_transfer = 0;

SELECT 1 FROM (SELECT 1 c0) tx SEMI LEFT JOIN (SELECT 1 c0) ty USING (c0) LIMIT 1 SETTINGS query_plan_merge_expressions = 0, query_plan_convert_join_to_in = 1;

-- The result above is also what the unconverted join returns, so pin that the conversion fired.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT 1 FROM (SELECT 1 c0) tx SEMI LEFT JOIN (SELECT 1 c0) ty USING (c0) LIMIT 1
    SETTINGS query_plan_merge_expressions = 0, query_plan_convert_join_to_in = 1
) WHERE explain ILIKE '%CreatingSets%';
