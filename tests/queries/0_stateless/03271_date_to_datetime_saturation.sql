drop table if exists test;

create table test (stamp Date) engine MergeTree order by stamp;

insert into test select '2024-10-30' from numbers(100);
insert into test select '2024-11-19' from numbers(100);
insert into test select '2149-06-06' from numbers(100);

optimize table test final;

-- implicit toDateTime (always saturate)
select count() from test where stamp >= parseDateTimeBestEffort('2024-11-01');

select count() from test where toDateTime(stamp) >= parseDateTimeBestEffort('2024-11-01') settings date_time_overflow_behavior = 'saturate';
select count() from test where toDateTime(stamp) >= parseDateTimeBestEffort('2024-11-01') settings date_time_overflow_behavior = 'ignore';
select count() from test where toDateTime(stamp) >= parseDateTimeBestEffort('2024-11-01') settings date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

drop table test;
