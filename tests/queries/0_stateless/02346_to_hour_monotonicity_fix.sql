drop table if exists test_tz_hour;

create table test_tz_hour(t DateTime, x String) engine MergeTree partition by toYYYYMMDD(t) order by x;
insert into test_tz_hour select toDateTime('2021-06-01 00:00:00', 'UTC') + number * 600, 'x' from numbers(1e3);

select toHour(toTimeZone(t, 'UTC')) as toHour_UTC, toHour(toTimeZone(t, 'Asia/Jerusalem')) as toHour_Israel, count() from test_tz_hour where toHour_Israel = 8 group by toHour_UTC, toHour_Israel;

drop table test_tz_hour;
