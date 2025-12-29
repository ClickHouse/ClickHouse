-- Tags: no-random-merge-tree-settings, no-random-settings

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 10;
SET enable_analyzer=1;

create table tab (x UInt32, y UInt32, z UInt32) engine = MergeTree order by tuple();
insert into tab select number, number, number from numbers(1e6);

select trimLeft(explain) from (explain actions=1 select * from (select sin(x) + y as a, sin(x) - z as b from tab) order by b limit 10
settings query_plan_max_limit_for_lazy_materialization=0, query_plan_execute_functions_after_sorting=0) where explain like '%Lazily read columns%';
