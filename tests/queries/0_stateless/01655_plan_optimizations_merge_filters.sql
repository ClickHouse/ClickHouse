set optimize_syntax_fuse_functions = 0;
set query_plan_merge_filters=1;
set optimize_respect_aliases = 1;
set query_plan_optimize_prewhere = 1;
set enable_optimize_predicate_expression = 1; -- CI may inject False; prevents key=7 from being rewritten as bitAnd(number,15)=7 and pushed down through GROUP BY

set enable_analyzer=1;
select explain from (explain actions = 1 select * from (select sum(number) as v, bitAnd(number, 15) as key from numbers(1e8) group by key having v != 0) where key = 7) where explain like '%Filter%' or explain like '%Aggregating%';

set enable_analyzer=0;
select explain from (explain actions = 1 select * from (select sum(number) as v, bitAnd(number, 15) as key from numbers(1e8) group by key having v != 0) where key = 7) where explain like '%Filter%' or explain like '%Aggregating%';
