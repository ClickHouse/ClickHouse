SET enable_analyzer = 1;
SET query_plan_merge_expressions = 1;

explain header = 1 select 1 as x;
