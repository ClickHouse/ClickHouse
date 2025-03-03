drop table if exists tab;
create table tab (a Int32, b Int32, c Int32, d Int32) engine = MergeTree order by (a, b, c);

insert into tab select 0, number % 3, 2 - intDiv(number, 3), (number % 3 + 1) * 10 from numbers(6);
insert into tab select 0, number % 3, 2 - intDiv(number, 3), (number % 3 + 1) * 100 from numbers(6);

select a, any(b), c, d from tab where b = 1 group by a, c, d order by c, d settings optimize_aggregation_in_order=1, query_plan_aggregation_in_order=1;
select * from (explain actions = 1, sorting=1 select a, any(b), c, d from tab where b = 1 group by a, c, d settings optimize_aggregation_in_order=1, query_plan_aggregation_in_order=1) where explain like '%ReadFromMergeTree%' or explain like '%Aggregating%' or explain like '%Order:%' settings enable_analyzer=0;
select * from (explain actions = 1, sorting=1 select a, any(b), c, d from tab where b = 1 group by a, c, d settings optimize_aggregation_in_order=1, query_plan_aggregation_in_order=1) where explain like '%ReadFromMergeTree%' or explain like '%Aggregating%' or explain like '%Order:%' settings enable_analyzer=1;
