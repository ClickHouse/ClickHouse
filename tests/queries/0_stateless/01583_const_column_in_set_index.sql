drop table if exists insub;

create table insub (i int, j int) engine MergeTree order by i settings index_granularity = 1;
insert into insub select number a, a + 2 from numbers(10);

SET max_rows_to_read = 2;
select * from insub where i in (select toInt32(3) from numbers(10));

drop table if exists insub;
