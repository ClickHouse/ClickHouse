drop table if exists x;

create table x (type Enum8('hourly' = 0, 'hourly_staging' = 1, 'daily' = 2, 'daily_staging' = 3), dt DateTime, i int, j int, k int, projection p (select sum(i), avg(j) group by toStartOfHour(dt)))
engine MergeTree partition by (type, if(toUInt8(type) < 2, dt, toStartOfDay(dt))) order by (dt, i);

insert into x values ('daily', '2021-03-28 01:00:00', 10, 1, 2), ('daily', '2021-03-28 02:00:00', 20, 5, 6);
insert into x values ('hourly', '2021-03-28 07:00:00', 80, 3, 4), ('hourly', '2021-03-28 08:00:00', 90, 7, 8);

-- update partition only
alter table x replace partition ('daily', '2021-03-28 00:00:00') from partition ('hourly', '2021-03-28 07:00:00') update type = 'daily';

select * from x order by type, dt;

-- update partition and a dependent column of projection p
alter table x replace partition ('daily', '2021-03-28 00:00:00') from partition ('hourly', '2021-03-28 07:00:00') update type = 'daily', j = 111;

select * from x order by type, dt;

-- update partition and an independent column of projection p
alter table x replace partition ('daily', '2021-03-28 00:00:00') from partition ('hourly', '2021-03-28 07:00:00') update type = 'daily', k = 222;

select * from x order by type, dt;

-- update all non-key columns
alter table x replace partition ('daily', '2021-03-28 00:00:00') from partition ('hourly', '2021-03-28 07:00:00') update type = 'daily', j = 8, k = 9;

select * from x order by type, dt;

drop table x;
