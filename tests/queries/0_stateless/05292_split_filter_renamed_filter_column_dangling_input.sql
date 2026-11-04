-- The constant filter column `NULL` has the same name as the aggregation key it is computed over, so splitting
-- the filter renames it, and the expression above read it through an input with no column in the header. Removing
-- unused columns rejected that input with a logical error. Found by the AST fuzzer.

SET enable_analyzer = 1;
SET query_plan_merge_expressions = 1;
SET query_plan_split_filter = 1;
SET query_plan_remove_unused_columns = 1;

SELECT x FROM (SELECT arrayJoin([1]) AS x GROUP BY NULL WITH TOTALS) WHERE NULL;

SELECT x FROM (SELECT arrayJoin([1]) AS x GROUP BY NULL WITH TOTALS) WHERE NULL
INTERSECT DISTINCT SELECT DISTINCT 1;
