SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;

explain header = 1 select 1 as x;
