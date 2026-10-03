-- Regression test: WITH TOTALS should produce consistent results regardless of
-- the query_plan_merge_expressions setting.
-- https://github.com/ClickHouse/ClickHouse/issues/101201
--
-- When query_plan_merge_expressions=0, a FilterStep above TotalsHavingStep could not
-- be pushed below it because an intermediate ExpressionStep blocked the push-down.
-- This caused the totals row to bypass the outer WHERE filter, producing wrong totals.

SELECT * FROM (
    SELECT number, count() FROM numbers(2) GROUP BY number WITH TOTALS
) WHERE number > 0
SETTINGS enable_optimize_predicate_expression=0, query_plan_merge_expressions=1;

SELECT '---';

SELECT * FROM (
    SELECT number, count() FROM numbers(2) GROUP BY number WITH TOTALS
) WHERE number > 0
SETTINGS enable_optimize_predicate_expression=0, query_plan_merge_expressions=0;
