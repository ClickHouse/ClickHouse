drop table if exists x;

set parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, optimize_aggregation_in_order = 0;

create table x (i int) engine MergeTree order by i settings index_granularity = 3;
insert into x select * from numbers(10);
select trimLeft(*) from (explain select count() from x where (i >= 3 and i <= 6) or i = 7) where explain like '%ReadFromPreparedSource%' or explain like '%ReadFromMergeTree%';
select count() from x where (i >= 3 and i <= 6) or i = 7;

drop table x;
