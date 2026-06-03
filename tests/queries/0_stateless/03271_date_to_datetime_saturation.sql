drop table if exists test;

create table test (stamp Date) engine MergeTree order by stamp;

insert into test select '2024-10-30' from numbers(100);
insert into test select '2024-11-19' from numbers(100);
insert into test select '2149-06-06' from numbers(100);

optimize table test final;

-- { echoOn }
-- implicit toDateTime (always saturate)
select count() from test where stamp >= parseDateTimeBestEffort('2024-11-01');

select count() from test where toDateTime(stamp) >= parseDateTimeBestEffort('2024-11-01') settings date_time_overflow_behavior = 'saturate';
select count() from test where toDateTime(stamp) >= parseDateTimeBestEffort('2024-11-01') settings date_time_overflow_behavior = 'ignore';
select count() from test where toDateTime(stamp) >= parseDateTimeBestEffort('2024-11-01') settings date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

drop table test;

create table test (stamp Date) engine MergeTree order by stamp settings index_granularity = 20;

insert into test select number from numbers(65536);

set session_timezone = 'UTC'; -- The following tests are timezone sensitive
set optimize_use_implicit_projections = 0;

-- Boundary at UNIX epoch
SELECT count() FROM test WHERE stamp >= toDateTime(0) SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime(0);

-- Arbitrary DateTime
SELECT count() FROM test WHERE stamp >= toDateTime('2024-10-24 21:30:00') SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime('2024-10-24 21:30:00');

-- Extreme value beyond supported range
SELECT count() FROM test WHERE stamp >= toDateTime(4294967295) SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime(4294967295);

-- Negative timestamp
SELECT count() FROM test WHERE stamp >= toDateTime(-1) SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime(-1);

-- Pre-Gregorian date
SELECT count() FROM test WHERE stamp >= toDateTime('1000-01-01 00:00:00') SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime('1000-01-01 00:00:00');

-- UNIX epoch
SELECT count() FROM test WHERE stamp >= toDateTime('1970-01-01 00:00:00') SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime('1970-01-01 00:00:00');

-- Modern date within supported range
SELECT count() FROM test WHERE stamp >= toDateTime('2023-01-01 00:00:00') SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime('2023-01-01 00:00:00');

-- Far future but still valid
SELECT count() FROM test WHERE stamp >= toDateTime('2100-12-31 23:59:59') SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime('2100-12-31 23:59:59');

-- Maximum 32-bit timestamp
SELECT count() FROM test WHERE stamp >= toDateTime(2147483647) SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime(2147483647);

-- Maximum 32-bit unsigned overflow
SELECT count() FROM test WHERE stamp >= toDateTime(4294967295) SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime(4294967295);

-- Minimum Date boundary
SELECT count() FROM test WHERE stamp >= toDate('0000-01-01') SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDate('0000-01-01');

-- Maximum Date boundary
SELECT count() FROM test WHERE stamp >= toDate('9999-12-31') SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDate('9999-12-31');

-- Convert stamp to Date
SELECT count() FROM test WHERE toDate(stamp) >= toDateTime(0) SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE toDate(identity(stamp)) >= toDateTime(0);

-- Convert stamp to DateTime (This will overflow and should not use primary key)
SELECT count() FROM test WHERE toDateTime(stamp) >= toDateTime(0) SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM test WHERE toDateTime(identity(stamp)) >= toDateTime(0);

-- Exact Date match
SELECT count() FROM test WHERE stamp = toDate('2023-01-01') SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) = toDate('2023-01-01');

-- Exact DateTime match
SELECT count() FROM test WHERE stamp = toDateTime('2023-01-01 00:00:00') SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) = toDateTime('2023-01-01 00:00:00');

-- Invalid DateTime (negative)
SELECT count() FROM test WHERE stamp < toDateTime(-1) SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) < toDateTime(-1);

-- Extremely large DateTime
SELECT count() FROM test WHERE stamp > toDateTime(9999999999) SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) > toDateTime(9999999999);

-- NULL DateTime
SELECT count() FROM test WHERE stamp >= toDateTime(NULL) SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) >= toDateTime(NULL);

-- NULL Date
SELECT count() FROM test WHERE stamp <= toDate(NULL) SETTINGS force_primary_key = 1;
SELECT count() FROM test WHERE identity(stamp) <= toDate(NULL);
