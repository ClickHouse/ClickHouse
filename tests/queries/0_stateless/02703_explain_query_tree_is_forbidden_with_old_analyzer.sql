SET query_plan_pretty_default = 0;
set enable_analyzer=0;
EXPLAIN QUERY TREE run_passes = true, dump_passes = true SELECT 1; -- { serverError NOT_IMPLEMENTED }
