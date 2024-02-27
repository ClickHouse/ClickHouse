drop table if exists x;

create table x (i int) engine MergeTree order by i settings index_granularity = 3;
insert into x select * from numbers(10);
explain select count() from x where (i >= 3 and i <= 6) or i = 7;
select count() from x where (i >= 3 and i <= 6) or i = 7;

drop table x;
