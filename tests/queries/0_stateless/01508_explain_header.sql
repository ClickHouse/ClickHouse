SET query_plan_pretty_default = 0;
SET enable_analyzer = 1;

explain header = 1 select 1 as x;
