drop table if exists x;

create table x (i int) engine MergeTree order by i settings index_granularity = 3;
insert into x select * from numbers(10);
select * from (explain select count() from x where (i >= 3 and i <= 6) or i = 7) where explain like '%ReadFromPreparedSource%' or explain like '%ReadFromMergeTree%';
select count() from x where (i >= 3 and i <= 6) or i = 7;

drop table x;
