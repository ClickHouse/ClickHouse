drop table if exists test1;

create table test1 (i int, j int) engine MergeTree partition by i order by tuple() settings index_granularity = 1;

insert into test1 select number, number + 100 from numbers(10);
select count() from test1 where i not in (1,2,3);
set max_rows_to_read = 5;
select * from test1 where i not in (1,2,3,4,5) order by i;

drop table test1;
