drop table if exists dt_overflow;

create table dt_overflow(d Date, i int) engine MergeTree partition by d order by i;

insert into dt_overflow values('2106-11-11', 1);

insert into dt_overflow values('2106-11-12', 1);

select * from dt_overflow ORDER BY d;

drop table if exists dt_overflow;
