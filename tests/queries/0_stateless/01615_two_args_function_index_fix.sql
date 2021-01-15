drop table if exists bad_date_time;

create table bad_date_time (time Datetime('Europe/Moscow'), count UInt16) Engine = MergeTree() ORDER BY (time);

insert into bad_date_time values('2020-12-20 20:59:52', 1),  ('2020-12-20 21:59:52', 1),  ('2020-12-20 01:59:52', 1);

select toDate(time, 'UTC') dt, min(toDateTime(time, 'UTC')), max(toDateTime(time, 'UTC')), sum(count) from bad_date_time where toDate(time, 'UTC') = '2020-12-19' group by dt;

drop table if exists bad_date_time;
